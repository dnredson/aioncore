use crate::{
    auth::{
        issue_api_token, map_principal_type_from_storage, map_principal_type_to_storage,
        AuthContext, PrincipalType,
    },
    error::ApiError,
    record_event, AppState, EventDraft,
};
use aion_event::EventSeverity;
use aion_storage::ApiToken;
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/auth/whoami", get(whoami))
        .route("/auth/tokens", post(create_api_token).get(list_api_tokens))
        .route("/auth/tokens/:token_id", get(get_api_token))
        .route("/auth/tokens/:token_id/revoke", post(revoke_api_token))
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateApiTokenRequest {
    pub(crate) token_name: String,
    pub(crate) principal_type: PrincipalType,
    pub(crate) principal_id: Option<String>,
    #[serde(default)]
    pub(crate) scopes: Vec<String>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ApiTokenRecordResponse {
    pub(crate) id: Uuid,
    pub(crate) tenant_id: Uuid,
    pub(crate) token_name: String,
    pub(crate) token_prefix: String,
    pub(crate) principal_type: PrincipalType,
    pub(crate) principal_id: Option<String>,
    pub(crate) scopes: Vec<String>,
    pub(crate) expires_at: Option<DateTime<Utc>>,
    pub(crate) revoked_at: Option<DateTime<Utc>>,
    pub(crate) last_used_at: Option<DateTime<Utc>>,
    pub(crate) metadata: Option<Value>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CreateApiTokenResponse {
    pub(crate) token: ApiTokenRecordResponse,
    pub(crate) raw_token: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct WhoAmIResponse {
    pub(crate) auth_mode: &'static str,
    pub(crate) authenticated: bool,
    pub(crate) auth_valid: bool,
    pub(crate) dev_bypass: bool,
    pub(crate) principal_type: PrincipalType,
    pub(crate) principal_id: Option<String>,
    pub(crate) tenant_id: Option<Uuid>,
    pub(crate) scopes: Vec<String>,
    pub(crate) token_id: Option<Uuid>,
}

async fn whoami(Extension(auth): Extension<AuthContext>) -> Json<WhoAmIResponse> {
    Json(WhoAmIResponse {
        auth_mode: auth.mode.as_str(),
        authenticated: auth.authenticated,
        auth_valid: auth.auth_valid,
        dev_bypass: auth.dev_bypass,
        principal_type: auth.principal.principal_type,
        principal_id: auth.principal.principal_id,
        tenant_id: auth.principal.tenant_id,
        scopes: auth.principal.scopes,
        token_id: auth.token_id,
    })
}

async fn create_api_token(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateApiTokenRequest>,
) -> Result<(StatusCode, Json<CreateApiTokenResponse>), ApiError> {
    crate::auth::require_scope(&state, &auth, "/auth/tokens", "auth:tokens:admin")?;

    let principal_type = map_principal_type_to_storage(request.principal_type)
        .ok_or_else(|| ApiError::bad_request("principal_type must not be anonymous"))?;
    let issued = issue_api_token();
    let now = Utc::now();
    let token = ApiToken::new(
        state.tenant_id,
        request.token_name,
        issued.token_prefix.clone(),
        issued.token_hash,
        principal_type,
        request.principal_id,
        request.scopes,
        request.expires_at,
        request.metadata,
        now,
    )?;
    let token = state.storage.create_api_token(token)?;
    record_api_token_created_event(&state, &token);

    Ok((
        StatusCode::CREATED,
        Json(CreateApiTokenResponse {
            token: api_token_record_response(token),
            raw_token: issued.raw_token,
        }),
    ))
}

async fn list_api_tokens(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<ApiTokenRecordResponse>>, ApiError> {
    crate::auth::require_scope(&state, &auth, "/auth/tokens", "auth:tokens:admin")?;
    Ok(Json(
        state
            .storage
            .list_api_tokens(state.tenant_id)?
            .into_iter()
            .map(api_token_record_response)
            .collect(),
    ))
}

async fn get_api_token(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(token_id): Path<Uuid>,
) -> Result<Json<ApiTokenRecordResponse>, ApiError> {
    crate::auth::require_scope(&state, &auth, "/auth/tokens/:token_id", "auth:tokens:admin")?;
    let token = state
        .storage
        .get_api_token(state.tenant_id, token_id)?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(api_token_record_response(token)))
}

async fn revoke_api_token(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(token_id): Path<Uuid>,
) -> Result<Json<ApiTokenRecordResponse>, ApiError> {
    crate::auth::require_scope(
        &state,
        &auth,
        "/auth/tokens/:token_id/revoke",
        "auth:tokens:admin",
    )?;
    let token = state
        .storage
        .revoke_api_token(state.tenant_id, token_id, Utc::now())?;
    record_api_token_revoked_event(&state, &token);
    Ok(Json(api_token_record_response(token)))
}

fn api_token_record_response(token: ApiToken) -> ApiTokenRecordResponse {
    ApiTokenRecordResponse {
        id: token.id,
        tenant_id: token.tenant_id,
        token_name: token.token_name,
        token_prefix: token.token_prefix,
        principal_type: map_principal_type_from_storage(token.principal_type),
        principal_id: token.principal_id,
        scopes: token.scopes,
        expires_at: token.expires_at,
        revoked_at: token.revoked_at,
        last_used_at: token.last_used_at,
        metadata: token.metadata,
        created_at: token.created_at,
        updated_at: token.updated_at,
    }
}

fn record_api_token_created_event(state: &AppState, token: &ApiToken) {
    record_auth_event(
        state,
        "aion:ApiTokenCreated",
        EventSeverity::Info,
        Some(format!("api token '{}' created", token.token_name)),
        Some(json!({
            "token_id": token.id,
            "token_prefix": token.token_prefix,
            "principal_type": map_principal_type_from_storage(token.principal_type),
            "principal_id": token.principal_id,
            "scopes": token.scopes,
        })),
    );
}

fn record_api_token_revoked_event(state: &AppState, token: &ApiToken) {
    record_auth_event(
        state,
        "aion:ApiTokenRevoked",
        EventSeverity::Info,
        Some(format!("api token '{}' revoked", token.token_name)),
        Some(json!({
            "token_id": token.id,
            "token_prefix": token.token_prefix,
            "principal_type": map_principal_type_from_storage(token.principal_type),
            "principal_id": token.principal_id,
        })),
    );
}

fn record_auth_event(
    state: &AppState,
    event_type: impl Into<String>,
    severity: EventSeverity,
    message: Option<String>,
    metadata: Option<Value>,
) {
    let now = Utc::now();
    let event = EventDraft {
        event_type: event_type.into(),
        severity,
        source_entity_id: None,
        target_entity_id: None,
        message,
        occurred_at: now,
        observed_at: Some(now),
        correlation_id: None,
        raw_message_id: None,
        observation_id: None,
        command_id: None,
        action_id: None,
        action_result_id: None,
        metadata,
    };
    let _ = record_event(state, event);
}
