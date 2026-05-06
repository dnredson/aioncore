use crate::{
    error::ApiError, record_auth_access_denied_event, record_auth_scope_denied_event,
    record_auth_token_accepted_event, record_token_rejected_event, record_token_used_event,
    AppState, StartupError,
};
use aion_storage::ApiTokenPrincipalType;
use axum::{extract::Request, http::header};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use subtle::ConstantTimeEq;
use uuid::Uuid;

const BOOTSTRAP_ADMIN_TOKEN_MIN_LENGTH: usize = 24;
const TOKEN_MODE_PROTECTED_ENDPOINT_GROUPS: [&str; 35] = [
    "auth_tokens",
    "connector_secrets",
    "adapters",
    "executors",
    "smartsentinel_executor_bridge",
    "ingestion_connectors",
    "connector_workers",
    "connector_aware_ingestion",
    "generic_http_ingestion",
    "ttn_device_mappings",
    "ttn_live_validation",
    "smartsentinel_snapshot_ingestion",
    "mcp_tools",
    "ai_context",
    "provenance_search",
    "events",
    "raw_messages",
    "entities",
    "observations",
    "timeseries",
    "commands",
    "actions",
    "rules",
    "policies",
    "capabilities",
    "executors_read",
    "entity_writes",
    "relationship_writes",
    "observation_writes",
    "command_writes",
    "action_writes",
    "rule_writes",
    "policy_writes",
    "capability_writes",
    "executor_config_writes",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    Dev,
    Disabled,
    Token,
}

impl AuthMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Disabled => "disabled",
            Self::Token => "token",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthEnforcementLevel {
    None,
    Partial,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub bootstrap_admin_token_hash: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            mode: AuthMode::Dev,
            bootstrap_admin_token_hash: None,
        }
    }
}

impl AuthConfig {
    pub fn from_env() -> Result<Self, StartupError> {
        Self::from_env_vars(
            env::var("AIONCORE_AUTH_MODE").ok(),
            env::var("AIONCORE_BOOTSTRAP_ADMIN_TOKEN").ok(),
        )
    }

    pub fn from_env_vars(
        mode: Option<String>,
        bootstrap_admin_token: Option<String>,
    ) -> Result<Self, StartupError> {
        let mode = match mode
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            None => AuthMode::Dev,
            Some("dev") => AuthMode::Dev,
            Some("disabled") => AuthMode::Disabled,
            Some("token") => AuthMode::Token,
            Some(other) => return Err(StartupError::unknown_auth_mode(other.to_string())),
        };

        let bootstrap_admin_token = bootstrap_admin_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);

        if let Some(token) = bootstrap_admin_token.as_deref() {
            if token.len() < BOOTSTRAP_ADMIN_TOKEN_MIN_LENGTH {
                return Err(StartupError::bootstrap_admin_token_too_short(
                    BOOTSTRAP_ADMIN_TOKEN_MIN_LENGTH,
                ));
            }
        }

        let bootstrap_admin_token_hash = bootstrap_admin_token.as_deref().map(hash_token_value);

        Ok(Self {
            mode,
            bootstrap_admin_token_hash,
        })
    }

    pub fn enforced(&self) -> bool {
        !matches!(self.enforcement_level(), AuthEnforcementLevel::None)
    }

    pub fn dev_bypass(&self) -> bool {
        matches!(self.mode, AuthMode::Dev)
    }

    pub fn enforcement_level(&self) -> AuthEnforcementLevel {
        match self.mode {
            AuthMode::Dev | AuthMode::Disabled => AuthEnforcementLevel::None,
            AuthMode::Token => AuthEnforcementLevel::Partial,
        }
    }

    pub fn protected_endpoint_groups(&self) -> &'static [&'static str] {
        match self.mode {
            AuthMode::Token => &TOKEN_MODE_PROTECTED_ENDPOINT_GROUPS,
            AuthMode::Dev | AuthMode::Disabled => &[],
        }
    }

    pub fn bootstrap_admin_configured(&self) -> bool {
        self.bootstrap_admin_token_hash.is_some()
    }

    pub fn ensure_supported(&self) -> Result<(), StartupError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalType {
    Anonymous,
    User,
    Device,
    Adapter,
    Executor,
    Connector,
    Service,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Principal {
    pub(crate) principal_type: PrincipalType,
    pub(crate) principal_id: Option<String>,
    pub(crate) tenant_id: Option<Uuid>,
    pub(crate) scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthContext {
    pub(crate) mode: AuthMode,
    pub(crate) enforced: bool,
    pub(crate) dev_bypass: bool,
    pub(crate) authenticated: bool,
    pub(crate) auth_valid: bool,
    pub(crate) token_id: Option<Uuid>,
    pub(crate) principal: Principal,
}

impl AuthContext {
    pub(crate) fn from_config(config: &AuthConfig) -> Self {
        let principal_id = match config.mode {
            AuthMode::Dev => Some("dev-bypass".to_string()),
            AuthMode::Disabled => Some("auth-disabled".to_string()),
            AuthMode::Token => Some("token-anonymous".to_string()),
        };

        Self {
            mode: config.mode,
            enforced: config.enforced(),
            dev_bypass: config.dev_bypass(),
            authenticated: false,
            auth_valid: false,
            token_id: None,
            principal: Principal {
                principal_type: PrincipalType::Anonymous,
                principal_id,
                tenant_id: None,
                scopes: Vec::new(),
            },
        }
    }

    pub(crate) fn authenticated_token(
        config: &AuthConfig,
        token_id: Uuid,
        principal_type: PrincipalType,
        principal_id: Option<String>,
        tenant_id: Uuid,
        scopes: Vec<String>,
    ) -> Self {
        Self::authenticated_principal(
            config,
            Some(token_id),
            principal_type,
            principal_id,
            tenant_id,
            scopes,
        )
    }

    pub(crate) fn authenticated_principal(
        config: &AuthConfig,
        token_id: Option<Uuid>,
        principal_type: PrincipalType,
        principal_id: Option<String>,
        tenant_id: Uuid,
        scopes: Vec<String>,
    ) -> Self {
        Self {
            mode: config.mode,
            enforced: config.enforced(),
            dev_bypass: config.dev_bypass(),
            authenticated: true,
            auth_valid: true,
            token_id,
            principal: Principal {
                principal_type,
                principal_id,
                tenant_id: Some(tenant_id),
                scopes,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IssuedApiToken {
    pub(crate) raw_token: String,
    pub(crate) token_prefix: String,
    pub(crate) token_hash: String,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TokenRejectionReason {
    Malformed,
    NotFound,
    HashMismatch,
    Revoked,
    Expired,
}

impl TokenRejectionReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::NotFound => "not_found",
            Self::HashMismatch => "hash_mismatch",
            Self::Revoked => "revoked",
            Self::Expired => "expired",
        }
    }
}

pub(crate) fn issue_api_token() -> IssuedApiToken {
    let prefix = Uuid::new_v4().simple().to_string()[..8].to_string();
    let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let raw_token = format!("aion_{prefix}_{secret}");
    IssuedApiToken {
        token_hash: hash_token_value(&raw_token),
        raw_token,
        token_prefix: prefix,
    }
}

pub(crate) fn hash_token_value(raw_token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_token.as_bytes());
    hex::encode(hasher.finalize())
}

fn token_hash_matches(stored_hash: &str, candidate_raw_token: &str) -> bool {
    let candidate_hash = hash_token_value(candidate_raw_token);
    stored_hash
        .as_bytes()
        .ct_eq(candidate_hash.as_bytes())
        .into()
}

fn parse_bearer_token(request: &Request) -> Option<String> {
    let header_value = request.headers().get(header::AUTHORIZATION)?;
    let value = header_value.to_str().ok()?.trim();
    let token = value.strip_prefix("Bearer ")?;
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn parse_token_prefix(raw_token: &str) -> Option<String> {
    let mut parts = raw_token.split('_');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("aion"), Some(prefix), Some(secret), None)
            if !prefix.trim().is_empty() && !secret.trim().is_empty() =>
        {
            Some(prefix.to_string())
        }
        _ => None,
    }
}

pub(crate) fn map_principal_type_to_storage(value: PrincipalType) -> Option<ApiTokenPrincipalType> {
    match value {
        PrincipalType::Anonymous => None,
        PrincipalType::User => Some(ApiTokenPrincipalType::User),
        PrincipalType::Device => Some(ApiTokenPrincipalType::Device),
        PrincipalType::Adapter => Some(ApiTokenPrincipalType::Adapter),
        PrincipalType::Executor => Some(ApiTokenPrincipalType::Executor),
        PrincipalType::Connector => Some(ApiTokenPrincipalType::Connector),
        PrincipalType::Service => Some(ApiTokenPrincipalType::Service),
        PrincipalType::Admin => Some(ApiTokenPrincipalType::Admin),
    }
}

pub(crate) fn map_principal_type_from_storage(value: ApiTokenPrincipalType) -> PrincipalType {
    match value {
        ApiTokenPrincipalType::User => PrincipalType::User,
        ApiTokenPrincipalType::Device => PrincipalType::Device,
        ApiTokenPrincipalType::Adapter => PrincipalType::Adapter,
        ApiTokenPrincipalType::Executor => PrincipalType::Executor,
        ApiTokenPrincipalType::Connector => PrincipalType::Connector,
        ApiTokenPrincipalType::Service => PrincipalType::Service,
        ApiTokenPrincipalType::Admin => PrincipalType::Admin,
    }
}

pub(crate) fn resolve_auth_context(state: &AppState, request: &Request) -> AuthContext {
    match state.auth.mode {
        AuthMode::Dev | AuthMode::Disabled => state.auth_context(),
        AuthMode::Token => resolve_token_auth_context(state, request),
    }
}

fn resolve_token_auth_context(state: &AppState, request: &Request) -> AuthContext {
    let Some(raw_token) = parse_bearer_token(request) else {
        return state.auth_context();
    };

    if auth_configured_bootstrap_token_matches(&state.auth, &raw_token) {
        record_auth_token_accepted_event(state, None, PrincipalType::Admin, Some("bootstrap_env"));
        return AuthContext::authenticated_principal(
            &state.auth,
            None,
            PrincipalType::Admin,
            Some("bootstrap-admin".to_string()),
            state.tenant_id,
            vec!["auth:tokens:admin".to_string(), "admin:all".to_string()],
        );
    }

    let Some(prefix) = parse_token_prefix(&raw_token) else {
        record_token_rejected_event(state, None, TokenRejectionReason::Malformed);
        return state.auth_context();
    };

    let token = match state.storage.find_api_token_by_prefix_any_tenant(&prefix) {
        Ok(Some(token)) => token,
        Ok(None) => {
            record_token_rejected_event(state, Some(prefix), TokenRejectionReason::NotFound);
            return state.auth_context();
        }
        Err(_) => return state.auth_context(),
    };

    if !token_hash_matches(&token.token_hash, &raw_token) {
        record_token_rejected_event(
            state,
            Some(token.token_prefix.clone()),
            TokenRejectionReason::HashMismatch,
        );
        return state.auth_context();
    }

    if token.revoked_at.is_some() {
        record_token_rejected_event(
            state,
            Some(token.token_prefix.clone()),
            TokenRejectionReason::Revoked,
        );
        return state.auth_context();
    }

    if token
        .expires_at
        .map(|value| value <= Utc::now())
        .unwrap_or(false)
    {
        record_token_rejected_event(
            state,
            Some(token.token_prefix.clone()),
            TokenRejectionReason::Expired,
        );
        return state.auth_context();
    }

    let now = Utc::now();
    let _ = state
        .storage
        .update_api_token_last_used_at(token.tenant_id, token.id, now);
    record_auth_token_accepted_event(
        state,
        Some(token.id),
        map_principal_type_from_storage(token.principal_type),
        Some("stored_api_token"),
    );
    record_token_used_event(state, &token);
    AuthContext::authenticated_token(
        &state.auth,
        token.id,
        map_principal_type_from_storage(token.principal_type),
        token.principal_id.clone(),
        token.tenant_id,
        token.scopes.clone(),
    )
}

fn auth_configured_bootstrap_token_matches(config: &AuthConfig, candidate_raw_token: &str) -> bool {
    config
        .bootstrap_admin_token_hash
        .as_deref()
        .map(|stored_hash| {
            let candidate_hash = hash_token_value(candidate_raw_token);
            stored_hash
                .as_bytes()
                .ct_eq(candidate_hash.as_bytes())
                .into()
        })
        .unwrap_or(false)
}

pub(crate) fn auth_has_scope(auth: &AuthContext, scope: &str) -> bool {
    auth.principal
        .scopes
        .iter()
        .any(|value| value == "admin:all" || value == scope)
}

pub(crate) fn is_admin_all(auth: &AuthContext) -> bool {
    matches!(auth.mode, AuthMode::Token) && auth_has_scope(auth, "admin:all")
}

pub(crate) fn principal_tenant_id(auth: &AuthContext) -> Result<Uuid, ApiError> {
    auth.principal
        .tenant_id
        .ok_or_else(|| ApiError::forbidden("authenticated token is missing tenant context"))
}

pub(crate) fn principal_tenant_or_default(
    state: &AppState,
    auth: &AuthContext,
) -> Result<Uuid, ApiError> {
    match auth.mode {
        AuthMode::Dev | AuthMode::Disabled => Ok(state.tenant_id),
        AuthMode::Token => auth
            .principal
            .tenant_id
            .or_else(|| is_admin_all(auth).then_some(state.tenant_id))
            .ok_or_else(|| ApiError::forbidden("authenticated token is missing tenant context")),
    }
}

pub(crate) fn tenant_for_created_resource(
    state: &AppState,
    auth: &AuthContext,
) -> Result<Uuid, ApiError> {
    principal_tenant_or_default(state, auth)
}

pub(crate) fn deny_cross_tenant_write(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    resource_name: &'static str,
) -> ApiError {
    record_auth_access_denied_event(state, endpoint, Some("tenant_mismatch"), auth);
    ApiError::forbidden(format!(
        "principal tenant does not own the target {resource_name} for {endpoint}"
    ))
}

#[allow(dead_code)]
pub(crate) fn require_same_tenant(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    resource_tenant_id: Uuid,
) -> Result<(), ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) || is_admin_all(auth) {
        return Ok(());
    }

    let principal_tenant_id = principal_tenant_id(auth)?;
    if principal_tenant_id == resource_tenant_id {
        return Ok(());
    }

    record_auth_access_denied_event(state, endpoint, Some("tenant_mismatch"), auth);
    Err(ApiError::forbidden(format!(
        "principal tenant does not own the resource for {endpoint}"
    )))
}

#[allow(dead_code)]
pub(crate) fn read_tenant_id(state: &AppState, auth: &AuthContext) -> Result<Uuid, ApiError> {
    match auth.mode {
        AuthMode::Dev | AuthMode::Disabled => Ok(state.tenant_id),
        AuthMode::Token => principal_tenant_id(auth),
    }
}

pub(crate) fn require_authenticated(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
) -> Result<(), ApiError> {
    match auth.mode {
        AuthMode::Dev | AuthMode::Disabled => Ok(()),
        AuthMode::Token if auth.authenticated && auth.auth_valid => Ok(()),
        AuthMode::Token => {
            record_auth_access_denied_event(state, endpoint, None, auth);
            Err(ApiError::unauthorized(format!(
                "bearer token is required for {endpoint}"
            )))
        }
    }
}

pub(crate) fn require_scope(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    scope: &'static str,
) -> Result<(), ApiError> {
    require_authenticated(state, auth, endpoint)?;
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return Ok(());
    }
    if auth_has_scope(auth, scope) {
        return Ok(());
    }

    record_auth_scope_denied_event(state, endpoint, &[scope], auth);
    Err(ApiError::forbidden(format!(
        "scope '{scope}' is required for {endpoint}"
    )))
}

pub(crate) fn require_scope_for_write(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    scope: &'static str,
) -> Result<(), ApiError> {
    require_scope(state, auth, endpoint, scope)
}

#[allow(dead_code)]
pub(crate) fn require_any_scope(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    scopes: &[&'static str],
) -> Result<(), ApiError> {
    require_authenticated(state, auth, endpoint)?;
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return Ok(());
    }
    if scopes.iter().any(|scope| auth_has_scope(auth, scope)) {
        return Ok(());
    }

    record_auth_scope_denied_event(state, endpoint, scopes, auth);
    Err(ApiError::forbidden(format!(
        "one of the required scopes is missing for {endpoint}"
    )))
}
