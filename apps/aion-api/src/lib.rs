use aion_action::{Action, ApprovalStatus, Command, ExecutorAgent, Policy};
use aion_entity::Entity;
use aion_event::{Event, EventSeverity};
use aion_flow::Flow;
use aion_observation::{Observation, ObservationValue};
use aion_payload::{
    CanonicalJsonDecoder, DecodeInput, DecodedMeasurement, PayloadDecoder, PayloadFormat,
    SenMlJsonDecoder, TtnUplinkJsonDecoder, UltraLightDecoder,
};
use aion_raw_message::{RawMessage, RawMessageSource};
use aion_rule::{Rule, RuleAction, RuleCondition, RuleEvaluationResult, RuleTriggerType};
#[cfg(test)]
use aion_storage::ApiTokenPrincipalType;
use aion_storage::{
    ApiToken, ConnectorProfile, ConnectorSecret, ConnectorSecretType, EventFilter, InMemoryStorage,
    PostgresStorage, PostgresStorageConfig, StorageBackend,
};
use axum::{
    extract::{Extension, Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::Response,
    routing::{get, get_service, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use command_support::{mutate_command_raw, record_command_event};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    env, fs,
    path::{Path as FsPath, PathBuf},
    str::FromStr,
    sync::{Arc, RwLock},
};
use tower_http::services::ServeDir;
use uuid::Uuid;

mod ai_context;
mod auth;
mod command_support;
mod connector_support;
mod error;
mod flow_execution;
mod flow_support;
mod mqtt_ingest;
mod query_filters;
mod routes;
mod ttn_support;
mod worker_support;

#[cfg(test)]
use auth::hash_token_value;
#[cfg(test)]
use auth::issue_api_token;
use auth::{
    deny_cross_tenant_write, is_admin_all, map_principal_type_from_storage, principal_tenant_id,
    principal_tenant_or_default, require_scope, require_scope_for_write, resolve_auth_context,
    tenant_for_created_resource, AuthContext, TokenRejectionReason,
};
pub use auth::{AuthConfig, AuthEnforcementLevel, AuthMode, PrincipalType};
pub(crate) use connector_support::{
    connector_event_metadata, ensure_connector_secret_exists, get_connector, record_connector_event,
};
use error::ApiError;
pub(crate) use routes::raw_messages::{raw_message_response, RawMessageResponse};
#[cfg(test)]
pub(crate) use routes::workers::{connector_runtime_status_from_spec, connector_workers_status};
pub(crate) use routes::workers::{
    connector_workers_readiness, worker_plan_summary, ConnectorWorkerRuntimeStatus,
    ConnectorWorkersReadiness,
};
pub(crate) use ttn_support::{is_plausible_ttn_topic_filter, record_ttn_device_mapping_event};
pub(crate) use worker_support::{
    build_ingestion_worker_plan, build_ready_worker_plan_summary, connector_worker_start_decision,
    connector_workers_enabled, mark_connector_worker_connected, mark_connector_worker_failure,
    mark_connector_worker_ingest_failed, mark_connector_worker_ingest_success,
    mark_connector_worker_message, mark_connector_worker_reconnect_scheduled,
    mark_connector_worker_starting, mark_connector_worker_subscribed, reconcile_connector_workers,
    reconcile_connector_workers_after_mutation, start_connector_workers, ConnectorWorkerHandle,
    ConnectorWorkerStartDecision,
};
#[cfg(test)]
pub(crate) use worker_support::{
    connector_worker_signature, set_connector_worker_runtime_status, set_connector_workers_enabled,
};
pub use worker_support::{
    ConnectorWorkerConfig, ConnectorWorkerEnvValues, ConnectorWorkerRuntimeState,
    IngestionWorkerKind, IngestionWorkerPlan, IngestionWorkerSpec, IngestionWorkerSpecStatus,
    IngestionWorkerValidationIssue,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageBackendName {
    Memory,
    Postgres,
}

impl StorageBackendName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackendConfig {
    Memory,
    Postgres { database_url: String },
}

impl StorageBackendConfig {
    pub fn from_env() -> Result<Self, StartupError> {
        Self::from_env_vars(
            env::var("AIONCORE_STORAGE_BACKEND").ok(),
            env::var("AIONCORE_DATABASE_URL").ok(),
        )
    }

    pub fn from_env_vars(
        backend: Option<String>,
        database_url: Option<String>,
    ) -> Result<Self, StartupError> {
        match backend
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            None => Ok(Self::Memory),
            Some("memory") => Ok(Self::Memory),
            Some("postgres") => {
                let database_url = database_url
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
                    .ok_or_else(StartupError::missing_database_url)?;
                Ok(Self::Postgres { database_url })
            }
            Some(other) => Err(StartupError::unknown_backend(other.to_string())),
        }
    }

    pub fn backend_name(&self) -> StorageBackendName {
        match self {
            Self::Memory => StorageBackendName::Memory,
            Self::Postgres { .. } => StorageBackendName::Postgres,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StartupError {
    message: String,
}

pub const DASHBOARD_STATIC_ENV_VAR: &str = "AIONCORE_DASHBOARD_STATIC_DIR";
pub const DASHBOARD_STATIC_MOUNT_PATH: &str = "/ui";

impl StartupError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn unknown_backend(value: String) -> Self {
        Self::new(format!(
            "unknown AIONCORE_STORAGE_BACKEND value '{value}'; expected memory or postgres"
        ))
    }

    fn missing_database_url() -> Self {
        Self::new("AIONCORE_DATABASE_URL is required when AIONCORE_STORAGE_BACKEND=postgres")
    }

    fn unknown_auth_mode(value: String) -> Self {
        Self::new(format!(
            "unknown AIONCORE_AUTH_MODE value '{value}'; expected dev, disabled, or token"
        ))
    }

    fn bootstrap_admin_token_too_short(minimum_length: usize) -> Self {
        Self::new(format!(
            "AIONCORE_BOOTSTRAP_ADMIN_TOKEN must be at least {minimum_length} characters long"
        ))
    }

    fn backend_initialization(message: impl Into<String>) -> Self {
        Self::new(message)
    }

    fn invalid_dashboard_static_dir(path: &str, reason: &str) -> Self {
        Self::new(format!(
            "{DASHBOARD_STATIC_ENV_VAR} must point to an existing directory; got '{path}': {reason}"
        ))
    }
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StartupError {}

#[derive(Debug, Clone)]
pub struct AppState {
    storage: Arc<dyn StorageBackend>,
    storage_backend: StorageBackendName,
    auth: AuthConfig,
    tenant_id: Uuid,
    mqtt_state: Arc<RwLock<mqtt_ingest::MqttWorkerState>>,
    connector_workers_enabled: Arc<RwLock<bool>>,
    connector_worker_statuses: Arc<RwLock<HashMap<Uuid, ConnectorWorkerRuntimeStatus>>>,
    connector_worker_handles: Arc<RwLock<HashMap<Uuid, ConnectorWorkerHandle>>>,
}

#[derive(Debug, Clone)]
pub struct StartupDiagnostics {
    pub storage_backend: StorageBackendName,
    pub database_url_provided: bool,
    pub migrations_applied: Option<bool>,
    pub auth_mode: AuthMode,
    pub auth_enforcement_level: AuthEnforcementLevel,
    pub auth_dev_bypass: bool,
    pub auth_bootstrap_admin_configured: bool,
    pub dashboard_static: DashboardStaticDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardStaticConfig {
    directory: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DashboardStaticDiagnostics {
    pub enabled: bool,
    pub path_configured: bool,
    pub available: bool,
}

impl DashboardStaticConfig {
    pub fn disabled() -> Self {
        Self { directory: None }
    }

    pub fn from_env() -> Result<Self, StartupError> {
        Self::from_env_var(env::var(DASHBOARD_STATIC_ENV_VAR).ok())
    }

    pub fn from_env_var(value: Option<String>) -> Result<Self, StartupError> {
        let Some(path) = value
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(Self::disabled());
        };

        let directory = PathBuf::from(path);
        validate_dashboard_static_dir(&directory, path)?;

        Ok(Self {
            directory: Some(directory),
        })
    }

    pub fn diagnostics(&self) -> DashboardStaticDiagnostics {
        let enabled = self.directory.is_some();
        DashboardStaticDiagnostics {
            enabled,
            path_configured: enabled,
            available: enabled,
        }
    }

    fn into_router(self) -> Option<Router> {
        let directory = self.directory?;
        let service = get_service(ServeDir::new(directory).append_index_html_on_directories(true))
            .handle_error(|_| async { StatusCode::INTERNAL_SERVER_ERROR });

        Some(Router::new().nest_service(DASHBOARD_STATIC_MOUNT_PATH, service))
    }
}

fn validate_dashboard_static_dir(directory: &FsPath, raw_value: &str) -> Result<(), StartupError> {
    let metadata = fs::metadata(directory)
        .map_err(|err| StartupError::invalid_dashboard_static_dir(raw_value, &err.to_string()))?;

    if !metadata.is_dir() {
        return Err(StartupError::invalid_dashboard_static_dir(
            raw_value,
            "path is not a directory",
        ));
    }

    Ok(())
}

impl AppState {
    pub fn local() -> Self {
        Self::with_backend_storage_and_auth(
            Arc::new(InMemoryStorage::new()),
            StorageBackendName::Memory,
            AuthConfig::default(),
            Uuid::nil(),
        )
    }

    pub fn with_storage(storage: InMemoryStorage, tenant_id: Uuid) -> Self {
        Self::with_backend_storage_and_auth(
            Arc::new(storage),
            StorageBackendName::Memory,
            AuthConfig::default(),
            tenant_id,
        )
    }

    pub fn with_backend_storage(
        storage: Arc<dyn StorageBackend>,
        storage_backend: StorageBackendName,
        tenant_id: Uuid,
    ) -> Self {
        Self::with_backend_storage_and_auth(
            storage,
            storage_backend,
            AuthConfig::default(),
            tenant_id,
        )
    }

    pub fn with_backend_storage_and_auth(
        storage: Arc<dyn StorageBackend>,
        storage_backend: StorageBackendName,
        auth: AuthConfig,
        tenant_id: Uuid,
    ) -> Self {
        Self {
            storage,
            storage_backend,
            auth,
            tenant_id,
            mqtt_state: Arc::new(RwLock::new(mqtt_ingest::MqttWorkerState::default())),
            connector_workers_enabled: Arc::new(RwLock::new(false)),
            connector_worker_statuses: Arc::new(RwLock::new(HashMap::new())),
            connector_worker_handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn from_config(config: StorageBackendConfig) -> Result<Self, StartupError> {
        Self::from_config_and_auth(config, AuthConfig::from_env()?)
    }

    pub fn from_config_with_diagnostics(
        config: StorageBackendConfig,
    ) -> Result<(Self, StartupDiagnostics), StartupError> {
        Self::from_config_and_auth_with_diagnostics(config, AuthConfig::from_env()?)
    }

    pub fn from_config_and_auth(
        config: StorageBackendConfig,
        auth: AuthConfig,
    ) -> Result<Self, StartupError> {
        Self::from_config_and_auth_with_diagnostics(config, auth).map(|(state, _)| state)
    }

    pub fn from_config_and_auth_with_diagnostics(
        config: StorageBackendConfig,
        auth: AuthConfig,
    ) -> Result<(Self, StartupDiagnostics), StartupError> {
        auth.ensure_supported()?;
        match config {
            StorageBackendConfig::Memory => Ok((
                Self::with_backend_storage_and_auth(
                    Arc::new(InMemoryStorage::new()),
                    StorageBackendName::Memory,
                    auth.clone(),
                    Uuid::nil(),
                ),
                StartupDiagnostics {
                    storage_backend: StorageBackendName::Memory,
                    database_url_provided: false,
                    migrations_applied: None,
                    auth_mode: auth.mode,
                    auth_enforcement_level: auth.enforcement_level(),
                    auth_dev_bypass: auth.dev_bypass(),
                    auth_bootstrap_admin_configured: auth.bootstrap_admin_configured(),
                    dashboard_static: DashboardStaticConfig::disabled().diagnostics(),
                },
            )),
            StorageBackendConfig::Postgres { database_url } => {
                let storage = PostgresStorage::connect(PostgresStorageConfig::new(database_url))
                    .map_err(|err| {
                        StartupError::backend_initialization(format!(
                            "failed to initialize postgres storage: {err}"
                        ))
                    })?;
                storage.run_embedded_migrations().map_err(|err| {
                    StartupError::backend_initialization(format!(
                        "failed to initialize postgres storage: {err}"
                    ))
                })?;
                Ok((
                    Self::with_backend_storage_and_auth(
                        Arc::new(storage),
                        StorageBackendName::Postgres,
                        auth.clone(),
                        Uuid::nil(),
                    ),
                    StartupDiagnostics {
                        storage_backend: StorageBackendName::Postgres,
                        database_url_provided: true,
                        migrations_applied: Some(true),
                        auth_mode: auth.mode,
                        auth_enforcement_level: auth.enforcement_level(),
                        auth_dev_bypass: auth.dev_bypass(),
                        auth_bootstrap_admin_configured: auth.bootstrap_admin_configured(),
                        dashboard_static: DashboardStaticConfig::disabled().diagnostics(),
                    },
                ))
            }
        }
    }

    pub fn from_env() -> Result<Self, StartupError> {
        Self::from_config(StorageBackendConfig::from_env()?)
    }

    pub fn from_env_with_diagnostics() -> Result<(Self, StartupDiagnostics), StartupError> {
        Self::from_config_with_diagnostics(StorageBackendConfig::from_env()?)
    }

    fn auth_context(&self) -> AuthContext {
        AuthContext::from_config(&self.auth)
    }
}

pub(crate) fn state_for_tenant(state: &AppState, tenant_id: Uuid) -> AppState {
    let mut scoped = state.clone();
    scoped.tenant_id = tenant_id;
    scoped
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    storage: &'static str,
}

#[derive(Debug, Serialize)]
struct ReadyResponse {
    ready: bool,
    status: &'static str,
    service: &'static str,
    storage: &'static str,
    auth: ReadyAuthResponse,
    mqtt: mqtt_ingest::MqttReadiness,
    worker_plan: ReadyWorkerPlanSummary,
    connector_workers: ConnectorWorkersReadiness,
    migrations_ready: Option<bool>,
    details: Option<String>,
}

#[derive(Debug, Serialize)]
struct ReadyAuthResponse {
    mode: &'static str,
    dev_bypass: bool,
    enforcement_level: AuthEnforcementLevel,
    protected_endpoint_groups: Vec<&'static str>,
    bootstrap_admin_configured: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReadyWorkerPlanSummary {
    planned_workers: usize,
    invalid_workers: usize,
    unsupported_workers: usize,
}

pub(crate) const SMARTSENTINEL_PAYLOAD_FORMAT: &str = "smartsentinel-snapshot-json";

#[derive(Debug, Deserialize)]
pub struct CreateConnectorSecretRequest {
    pub secret_key: String,
    pub secret_type: ConnectorSecretType,
    pub username: Option<String>,
    pub secret_value: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct ConnectorSecretResponse {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub secret_key: String,
    pub secret_type: ConnectorSecretType,
    pub username: Option<String>,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct PutPolicyRequest {
    pub target_entity_id: Option<Uuid>,
    pub command_type: Option<String>,
    pub requires_approval: bool,
    pub auto_execute_allowed: bool,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct PolicyQuery {
    pub target_entity_id: Option<Uuid>,
    pub command_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateRuleRequest {
    pub name: String,
    pub description: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub trigger_type: RuleTriggerType,
    pub target_entity_id: Option<Uuid>,
    pub observed_property: Option<String>,
    pub event_type: Option<String>,
    pub condition: RuleCondition,
    pub action: RuleAction,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ManualRuleEvaluationRequest {
    pub observation_id: Option<Uuid>,
    pub event_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct RuleEvaluationResponse {
    pub results: Vec<RuleEvaluationResult>,
    pub generated_commands: Vec<Command>,
    pub generated_events: Vec<Event>,
}

const DEFAULT_COMMAND_LEASE_SECONDS: i64 = 60;

pub fn app() -> Router {
    app_with_state(AppState::local())
}

pub fn app_from_env() -> Result<Router, StartupError> {
    Ok(app_with_state_and_dashboard_static(
        AppState::from_env()?,
        DashboardStaticConfig::from_env()?,
    ))
}

pub fn app_from_env_with_diagnostics() -> Result<(Router, StartupDiagnostics), StartupError> {
    let (state, mut diagnostics) = AppState::from_env_with_diagnostics()?;
    let dashboard_static = DashboardStaticConfig::from_env()?;
    diagnostics.dashboard_static = dashboard_static.diagnostics();
    Ok((
        app_with_state_and_dashboard_static(state, dashboard_static),
        diagnostics,
    ))
}

pub async fn start_mqtt_ingest_if_enabled(state: AppState) -> Result<(), StartupError> {
    mqtt_ingest::start_if_enabled(state).await
}

pub async fn start_connector_workers_if_enabled(state: AppState) -> Result<(), StartupError> {
    let config = ConnectorWorkerConfig::from_env()?;
    start_connector_workers(state, config).await
}

pub fn app_with_state(state: AppState) -> Router {
    app_with_state_and_dashboard_static(state, DashboardStaticConfig::disabled())
}

pub fn app_with_state_and_dashboard_static(
    state: AppState,
    dashboard_static: DashboardStaticConfig,
) -> Router {
    let router = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .merge(routes::auth::router())
        .merge(routes::entities::router())
        .route("/policies", put(put_policies).get(query_policies))
        .route("/rules", post(create_rule).get(list_rules))
        .route("/rules/evaluate", post(evaluate_rules_manually))
        .route("/rules/:rule_id", get(get_rule))
        .route("/rules/:rule_id/enable", put(enable_rule))
        .route("/rules/:rule_id/disable", put(disable_rule))
        .merge(routes::executors::router())
        .merge(routes::adapters::router())
        .merge(routes::commands::router())
        .merge(routes::connectors::router())
        .merge(routes::dashboard::router())
        .merge(routes::dlq::router())
        .merge(routes::flows::router())
        .merge(routes::smartsentinel::router())
        .merge(routes::sync_sessions::router())
        .merge(routes::mcp::router())
        .merge(routes::ai::router())
        .merge(routes::provenance::router())
        .merge(routes::events::router())
        .merge(routes::observations::router())
        .merge(routes::timeseries::router())
        .merge(routes::ingestion::router())
        .merge(routes::ttn::router())
        .merge(routes::workers::router())
        .route(
            "/secrets/connectors",
            post(create_connector_secret).get(list_connector_secrets),
        )
        .route(
            "/secrets/connectors/:secret_id",
            get(get_connector_secret).delete(delete_connector_secret),
        )
        .merge(routes::raw_messages::router())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_context_middleware,
        ))
        .with_state(state);

    if let Some(static_router) = dashboard_static.into_router() {
        router.merge(static_router)
    } else {
        router
    }
}

async fn auth_context_middleware(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let auth_context = resolve_auth_context(&state, &request);
    request.extensions_mut().insert(auth_context);
    next.run(request).await
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "aion-api",
        storage: state.storage_backend.as_str(),
    })
}

async fn ready(State(state): State<AppState>) -> (StatusCode, Json<ReadyResponse>) {
    let storage_readiness = state.storage.check_readiness();
    let storage_ready = storage_readiness.is_ok();
    let mqtt = mqtt_ingest::readiness(&state);
    let worker_plan = worker_plan_summary(&state);
    let connector_workers = connector_workers_readiness(&state);
    let ready = storage_ready && mqtt.ready;

    let details = match (
        storage_readiness.err(),
        mqtt.ready,
        mqtt.last_error.as_deref(),
    ) {
        (Some(err), false, Some(mqtt_error)) => {
            Some(format!("{err}; mqtt not ready: {mqtt_error}"))
        }
        (Some(err), _, _) => Some(err.to_string()),
        (None, false, Some(mqtt_error)) => Some(format!("mqtt not ready: {mqtt_error}")),
        (None, false, None) => Some("mqtt not ready".to_string()),
        (None, true, _) => None,
    };

    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(ReadyResponse {
            ready,
            status: if ready { "ready" } else { "not_ready" },
            service: "aion-api",
            storage: state.storage_backend.as_str(),
            auth: ReadyAuthResponse {
                mode: state.auth.mode.as_str(),
                dev_bypass: state.auth.dev_bypass(),
                enforcement_level: state.auth.enforcement_level(),
                protected_endpoint_groups: state.auth.protected_endpoint_groups().to_vec(),
                bootstrap_admin_configured: state.auth.bootstrap_admin_configured(),
            },
            mqtt,
            worker_plan,
            connector_workers,
            migrations_ready: match (state.storage_backend, storage_ready) {
                (StorageBackendName::Memory, _) => None,
                (StorageBackendName::Postgres, true) => Some(true),
                (StorageBackendName::Postgres, false) => Some(false),
            },
            details,
        }),
    )
}

async fn put_policies(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(requests): Json<Vec<PutPolicyRequest>>,
) -> Result<(StatusCode, Json<Vec<Policy>>), ApiError> {
    require_scope_for_write(&state, &auth, "/policies", "policies:write")?;
    for request in &requests {
        if let Some(target_entity_id) = request.target_entity_id {
            require_same_tenant_for_target_entity(&state, &auth, "/policies", target_entity_id)?;
        }
    }
    let tenant_id = tenant_for_created_resource(&state, &auth)?;
    let scoped_state = state_for_tenant(&state, tenant_id);

    let policies = requests
        .into_iter()
        .map(|request| {
            Policy::new(
                scoped_state.tenant_id,
                request.target_entity_id,
                request.command_type,
                request.requires_approval,
                request.auto_execute_allowed,
                request.metadata,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let policies = scoped_state
        .storage
        .put_policies(scoped_state.tenant_id, policies)?;
    Ok((StatusCode::OK, Json(policies)))
}

async fn query_policies(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<PolicyQuery>,
) -> Result<Json<Vec<Policy>>, ApiError> {
    require_scope(&state, &auth, "/policies", "policies:read")?;
    let policies = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.query_policies(
            state.tenant_id,
            query.target_entity_id,
            query.command_type.as_deref(),
        )?
    } else if is_admin_all(&auth) {
        state
            .storage
            .list_all_policies()?
            .into_iter()
            .filter(|policy| {
                query
                    .target_entity_id
                    .map(|id| policy.target_entity_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|policy| {
                query
                    .command_type
                    .as_deref()
                    .map(|command_type| policy.command_type.as_deref() == Some(command_type))
                    .unwrap_or(true)
            })
            .collect()
    } else {
        state.storage.query_policies(
            principal_tenant_id(&auth)?,
            query.target_entity_id,
            query.command_type.as_deref(),
        )?
    };
    Ok(Json(policies))
}

async fn create_rule(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateRuleRequest>,
) -> Result<(StatusCode, Json<Rule>), ApiError> {
    require_scope_for_write(&state, &auth, "/rules", "rules:write")?;
    if let Some(target_entity_id) = request.target_entity_id {
        require_same_tenant_for_target_entity(&state, &auth, "/rules", target_entity_id)?;
    }
    ensure_rule_action_targets_exist_with_auth(&state, &auth, "/rules", &request.action)?;
    let tenant_id = tenant_for_created_resource(&state, &auth)?;
    let scoped_state = state_for_tenant(&state, tenant_id);

    let rule = Rule::new(
        scoped_state.tenant_id,
        request.name,
        request.description,
        request.enabled,
        request.trigger_type,
        request.target_entity_id,
        request.observed_property,
        request.event_type,
        request.condition,
        request.action,
        request.metadata,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok((
        StatusCode::CREATED,
        Json(scoped_state.storage.store_rule(rule)?),
    ))
}

async fn list_rules(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<Rule>>, ApiError> {
    require_scope(&state, &auth, "/rules", "rules:read")?;
    let rules = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_rules(state.tenant_id)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_rules()?
    } else {
        state.storage.list_rules(principal_tenant_id(&auth)?)?
    };
    Ok(Json(rules))
}

async fn get_rule(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(rule_id): Path<Uuid>,
) -> Result<Json<Rule>, ApiError> {
    require_scope(&state, &auth, "/rules/:rule_id", "rules:read")?;
    let rule = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_rule(state.tenant_id, rule_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_rule_any_tenant(rule_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_rule(tenant_id, rule_id)? {
            Some(rule) => rule,
            None => {
                if state.storage.get_rule_any_tenant(rule_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /rules/:rule_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };
    Ok(Json(rule))
}

async fn enable_rule(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(rule_id): Path<Uuid>,
) -> Result<Json<Rule>, ApiError> {
    require_scope_for_write(&state, &auth, "/rules/:rule_id/enable", "rules:write")?;
    let rule =
        require_same_tenant_for_target_rule(&state, &auth, "/rules/:rule_id/enable", rule_id)?;
    set_rule_enabled(state_for_tenant(&state, rule.tenant_id), rule_id, true)
}

async fn disable_rule(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(rule_id): Path<Uuid>,
) -> Result<Json<Rule>, ApiError> {
    require_scope_for_write(&state, &auth, "/rules/:rule_id/disable", "rules:write")?;
    let rule =
        require_same_tenant_for_target_rule(&state, &auth, "/rules/:rule_id/disable", rule_id)?;
    set_rule_enabled(state_for_tenant(&state, rule.tenant_id), rule_id, false)
}

fn set_rule_enabled(state: AppState, rule_id: Uuid, enabled: bool) -> Result<Json<Rule>, ApiError> {
    let mut rule = state
        .storage
        .get_rule(state.tenant_id, rule_id)?
        .ok_or_else(ApiError::not_found)?;
    rule.set_enabled(enabled, Utc::now());
    Ok(Json(state.storage.update_rule(rule)?))
}

async fn evaluate_rules_manually(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<ManualRuleEvaluationRequest>,
) -> Result<Json<RuleEvaluationResponse>, ApiError> {
    require_scope_for_write(&state, &auth, "/rules/evaluate", "rules:write")?;
    let has_observation = request.observation_id.is_some();
    let has_event = request.event_id.is_some();
    if has_observation == has_event {
        return Err(ApiError::bad_request(
            "exactly one of observation_id or event_id is required",
        ));
    }

    if let Some(observation_id) = request.observation_id {
        let observation = require_same_tenant_for_target_observation(
            &state,
            &auth,
            "/rules/evaluate",
            observation_id,
        )?;
        let scoped_state = state_for_tenant(&state, observation.tenant_id);
        return evaluate_rules_for_observation(&scoped_state, &observation, false).map(Json);
    }

    let event_id = request.event_id.expect("event_id presence checked above");
    let event = require_same_tenant_for_target_event(&state, &auth, "/rules/evaluate", event_id)?;
    let scoped_state = state_for_tenant(&state, event.tenant_id);
    evaluate_rules_for_event(&scoped_state, &event, false).map(Json)
}

async fn create_connector_secret(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateConnectorSecretRequest>,
) -> Result<(StatusCode, Json<ConnectorSecretResponse>), ApiError> {
    require_scope(&state, &auth, "/secrets/connectors", "secrets:admin")?;
    let secret = ConnectorSecret::new(
        state.tenant_id,
        request.secret_key,
        request.secret_type,
        request.username,
        request.secret_value,
        request.metadata,
        Utc::now(),
    )?;
    let secret = state.storage.create_connector_secret(secret)?;
    record_connector_secret_event(
        &state,
        "aion:ConnectorSecretCreated",
        &secret,
        Some("Connector secret created".to_string()),
    )?;
    Ok((StatusCode::CREATED, Json(connector_secret_response(secret))))
}

async fn list_connector_secrets(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<ConnectorSecretResponse>>, ApiError> {
    require_scope(&state, &auth, "/secrets/connectors", "secrets:admin")?;
    let secrets = state
        .storage
        .list_connector_secrets(state.tenant_id)?
        .into_iter()
        .map(connector_secret_response)
        .collect();
    Ok(Json(secrets))
}

async fn get_connector_secret(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(secret_id): Path<Uuid>,
) -> Result<Json<ConnectorSecretResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/secrets/connectors/:secret_id",
        "secrets:admin",
    )?;
    let secret = state
        .storage
        .get_connector_secret(state.tenant_id, secret_id)?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(connector_secret_response(secret)))
}

async fn delete_connector_secret(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(secret_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_scope(
        &state,
        &auth,
        "/secrets/connectors/:secret_id",
        "secrets:admin",
    )?;
    let secret = state
        .storage
        .get_connector_secret(state.tenant_id, secret_id)?
        .ok_or_else(ApiError::not_found)?;
    state
        .storage
        .delete_connector_secret(state.tenant_id, secret_id)?;
    record_connector_secret_event(
        &state,
        "aion:ConnectorSecretDeleted",
        &secret,
        Some("Connector secret deleted".to_string()),
    )?;
    reconcile_connector_workers_after_mutation(&state).await;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) fn require_same_tenant_for_target_entity(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    entity_id: Uuid,
) -> Result<Entity, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_entity(state.tenant_id, entity_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_entity_any_tenant(entity_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_or_default(state, auth)?;
    match state.storage.get_entity(tenant_id, entity_id)? {
        Some(entity) => Ok(entity),
        None => {
            if state.storage.get_entity_any_tenant(entity_id)?.is_some() {
                Err(deny_cross_tenant_write(state, auth, endpoint, "entity"))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

pub(crate) fn require_same_tenant_for_target_command(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    command_id: Uuid,
) -> Result<Command, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_command(state.tenant_id, command_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_command_any_tenant(command_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_or_default(state, auth)?;
    match state.storage.get_command(tenant_id, command_id)? {
        Some(command) => Ok(command),
        None => {
            if state.storage.get_command_any_tenant(command_id)?.is_some() {
                Err(deny_cross_tenant_write(state, auth, endpoint, "command"))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

fn require_same_tenant_for_target_rule(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    rule_id: Uuid,
) -> Result<Rule, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_rule(state.tenant_id, rule_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_rule_any_tenant(rule_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_or_default(state, auth)?;
    match state.storage.get_rule(tenant_id, rule_id)? {
        Some(rule) => Ok(rule),
        None => {
            if state.storage.get_rule_any_tenant(rule_id)?.is_some() {
                Err(deny_cross_tenant_write(state, auth, endpoint, "rule"))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

pub(crate) fn require_same_tenant_for_target_flow(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    flow_id: Uuid,
) -> Result<Flow, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_flow(state.tenant_id, flow_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_flow_any_tenant(flow_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_or_default(state, auth)?;
    match state.storage.get_flow(tenant_id, flow_id)? {
        Some(flow) => Ok(flow),
        None => {
            if state.storage.get_flow_any_tenant(flow_id)?.is_some() {
                Err(deny_cross_tenant_write(state, auth, endpoint, "flow"))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

pub(crate) fn require_same_tenant_for_target_executor(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    executor_id: Uuid,
) -> Result<ExecutorAgent, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_executor(state.tenant_id, executor_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_executor_any_tenant(executor_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_or_default(state, auth)?;
    match state.storage.get_executor(tenant_id, executor_id)? {
        Some(executor) => Ok(executor),
        None => {
            if state
                .storage
                .get_executor_any_tenant(executor_id)?
                .is_some()
            {
                Err(deny_cross_tenant_write(state, auth, endpoint, "executor"))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

pub(crate) fn require_same_tenant_for_target_action(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    action_id: Uuid,
) -> Result<Action, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_action(state.tenant_id, action_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_action_any_tenant(action_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_or_default(state, auth)?;
    match state.storage.get_action(tenant_id, action_id)? {
        Some(action) => Ok(action),
        None => {
            if state.storage.get_action_any_tenant(action_id)?.is_some() {
                Err(deny_cross_tenant_write(state, auth, endpoint, "action"))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

fn require_same_tenant_for_target_observation(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    observation_id: Uuid,
) -> Result<Observation, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_observation(state.tenant_id, observation_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_observation_any_tenant(observation_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_or_default(state, auth)?;
    match state.storage.get_observation(tenant_id, observation_id)? {
        Some(observation) => Ok(observation),
        None => {
            if state
                .storage
                .get_observation_any_tenant(observation_id)?
                .is_some()
            {
                Err(deny_cross_tenant_write(
                    state,
                    auth,
                    endpoint,
                    "observation",
                ))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

fn require_same_tenant_for_target_event(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    event_id: Uuid,
) -> Result<Event, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_event(state.tenant_id, event_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_event_any_tenant(event_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_or_default(state, auth)?;
    match state.storage.get_event(tenant_id, event_id)? {
        Some(event) => Ok(event),
        None => {
            if state.storage.get_event_any_tenant(event_id)?.is_some() {
                Err(deny_cross_tenant_write(state, auth, endpoint, "event"))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

pub(crate) fn decoder_for_format(
    payload_format: &str,
) -> Result<Box<dyn PayloadDecoder>, ApiError> {
    let normalized = payload_format.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "senml" | "senml_json" => Ok(Box::new(SenMlJsonDecoder)),
        "ultralight" | "ultra_light" => Ok(Box::new(UltraLightDecoder)),
        "canonical_json" | "canonical" => Ok(Box::new(CanonicalJsonDecoder)),
        "ttn_uplink_json" => Ok(Box::new(TtnUplinkJsonDecoder)),
        _ => Err(ApiError::bad_request(format!(
            "unsupported payload_format: {payload_format}"
        ))),
    }
}

pub(crate) fn payload_format_requires_mapping(payload_format: &str) -> bool {
    matches!(
        payload_format
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .as_str(),
        "ultralight" | "ultra_light"
    )
}

pub(crate) fn is_ttn_uplink_payload_format(payload_format: &str) -> bool {
    matches!(
        payload_format
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .as_str(),
        "ttn_uplink_json"
    )
}

pub(crate) fn payload_to_bytes(payload: &Value) -> Vec<u8> {
    payload
        .as_str()
        .map(|value| value.as_bytes().to_vec())
        .unwrap_or_else(|| payload.to_string().into_bytes())
}

pub(crate) fn ensure_entity_exists(state: &AppState, entity_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn connector_secret_response(secret: ConnectorSecret) -> ConnectorSecretResponse {
    ConnectorSecretResponse {
        id: secret.id,
        tenant_id: secret.tenant_id,
        secret_key: secret.secret_key,
        secret_type: secret.secret_type,
        username: secret.username,
        metadata: secret.metadata,
        created_at: secret.created_at,
        updated_at: secret.updated_at,
    }
}

fn connector_secret_event_metadata(secret: &ConnectorSecret) -> Value {
    json!({
        "secret_id": secret.id,
        "secret_key": secret.secret_key,
        "secret_type": secret.secret_type,
        "username_configured": secret.username.is_some()
    })
}

pub(crate) fn metadata_with_connector(
    mut metadata: Value,
    connector_metadata: Option<Value>,
) -> Value {
    let Some(connector_metadata) = connector_metadata else {
        return metadata;
    };

    if let Some(object) = metadata.as_object_mut() {
        object.insert("connector".to_string(), connector_metadata.clone());
        for key in ["connector_id", "connector_key", "connector_profile"] {
            if let Some(value) = connector_metadata.get(key) {
                object.insert(key.to_string(), value.clone());
            }
        }
    }

    metadata
}

pub(crate) fn decoded_ingest_metadata(decoded: &[DecodedMeasurement]) -> Value {
    let Some(first) = decoded.first() else {
        return json!({});
    };
    let mut metadata = json!({});
    if let Some(value) = first.metadata.get("decoded_payload_keys") {
        metadata["decoded_payload_keys"] = value.clone();
    }
    if let Some(value) = first.metadata.get("ttn_device_id") {
        metadata["ttn_device_id"] = value.clone();
    }
    if let Some(value) = first.metadata.get("ttn_application_id") {
        metadata["ttn_application_id"] = value.clone();
    }
    metadata
}

pub(crate) fn merge_json_object(target: &mut Value, source: Value) {
    let (Some(target), Some(source)) = (target.as_object_mut(), source.as_object()) else {
        return;
    };
    for (key, value) in source {
        target.insert(key.clone(), value.clone());
    }
}

fn ensure_command_exists(state: &AppState, command_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_command(state.tenant_id, command_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn ensure_action_exists(state: &AppState, action_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_action(state.tenant_id, action_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn ensure_action_result_exists(state: &AppState, action_result_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .query_action_results(state.tenant_id, None, None)?
        .into_iter()
        .find(|result| result.id == action_result_id)
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn ensure_raw_message_exists(state: &AppState, raw_message_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_raw_message(state.tenant_id, raw_message_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

pub(crate) fn ensure_executor_exists(state: &AppState, executor_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_executor(state.tenant_id, executor_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

pub(crate) fn get_executor_agent(
    state: &AppState,
    executor_id: Uuid,
) -> Result<ExecutorAgent, ApiError> {
    state
        .storage
        .get_executor(state.tenant_id, executor_id)?
        .ok_or_else(ApiError::not_found)
}

pub(crate) fn insert_optional_string(
    object: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        object.insert(key.to_string(), json!(value));
    }
}

pub(crate) fn record_executor_event(
    state: &AppState,
    event_type: impl Into<String>,
    executor: &ExecutorAgent,
    command: Option<&Command>,
    metadata: Option<Value>,
) -> Result<Event, ApiError> {
    let mut event_metadata = json!({
        "executor_id": executor.id,
        "agent_key": executor.agent_key,
        "agent_type": executor.agent_type
    });
    if let Some(object) = event_metadata.as_object_mut() {
        if let Some(command) = command {
            object.insert("command_id".to_string(), json!(command.id));
            object.insert("command_type".to_string(), json!(command.command_type));
        }
        if let Some(metadata) = metadata {
            object.insert("metadata".to_string(), metadata);
        }
    }

    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity: EventSeverity::Info,
            source_entity_id: None,
            target_entity_id: command.map(|command| command.target_entity_id),
            message: Some(format!("Executor {} event", executor.agent_key)),
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: command.map(|command| command.id),
            action_id: None,
            action_result_id: None,
            metadata: Some(event_metadata),
        },
    )
}

fn record_connector_secret_event(
    state: &AppState,
    event_type: impl Into<String>,
    secret: &ConnectorSecret,
    message: Option<String>,
) -> Result<Event, ApiError> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity: EventSeverity::Info,
            source_entity_id: None,
            target_entity_id: None,
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(connector_secret_event_metadata(secret)),
        },
    )
}

pub(crate) fn record_connector_worker_event(
    state: &AppState,
    event_type: impl Into<String>,
    severity: EventSeverity,
    message: Option<String>,
    metadata: Value,
) -> Result<Event, ApiError> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity,
            source_entity_id: None,
            target_entity_id: None,
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(metadata),
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_ingest_event(
    state: &AppState,
    event_type: impl Into<String>,
    severity: EventSeverity,
    source_entity_id: Uuid,
    target_entity_id: Uuid,
    raw_message_id: Uuid,
    message: Option<String>,
    metadata: Value,
) -> Result<Event, ApiError> {
    record_ingest_event_optional(
        state,
        event_type,
        severity,
        Some(source_entity_id),
        Some(target_entity_id),
        Some(raw_message_id),
        message,
        metadata,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_ingest_event_optional(
    state: &AppState,
    event_type: impl Into<String>,
    severity: EventSeverity,
    source_entity_id: Option<Uuid>,
    target_entity_id: Option<Uuid>,
    raw_message_id: Option<Uuid>,
    message: Option<String>,
    metadata: Value,
) -> Result<Event, ApiError> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity,
            source_entity_id,
            target_entity_id,
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(metadata),
        },
    )
}

pub(crate) fn record_event(state: &AppState, draft: EventDraft) -> Result<Event, ApiError> {
    let now = Utc::now();
    let event = Event::new(
        state.tenant_id,
        draft.event_type,
        draft.severity,
        draft.source_entity_id,
        draft.target_entity_id,
        draft.message,
        draft.occurred_at,
        draft.observed_at,
        draft.correlation_id,
        draft.raw_message_id,
        draft.observation_id,
        draft.command_id,
        draft.action_id,
        draft.action_result_id,
        draft.metadata,
        now,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok(state.storage.store_event(event)?)
}

fn record_auth_event(
    state: &AppState,
    event_type: &str,
    severity: EventSeverity,
    message: Option<String>,
    metadata: Option<Value>,
) {
    let now = Utc::now();
    let event = Event::new(
        state.tenant_id,
        event_type,
        severity,
        None,
        None,
        message,
        now,
        Some(now),
        None,
        None,
        None,
        None,
        None,
        None,
        metadata,
        now,
    );
    if let Ok(event) = event {
        let _ = state.storage.store_event(event);
    }
}

fn record_auth_token_accepted_event(
    state: &AppState,
    token_id: Option<Uuid>,
    principal_type: PrincipalType,
    source: Option<&str>,
) {
    record_auth_event(
        state,
        "aion:AuthTokenAccepted",
        EventSeverity::Info,
        Some("authenticated token accepted".to_string()),
        Some(json!({
            "token_id": token_id,
            "principal_type": principal_type,
            "source": source,
        })),
    );
}

fn record_token_used_event(state: &AppState, token: &ApiToken) {
    record_auth_event(
        state,
        "aion:ApiTokenUsed",
        EventSeverity::Info,
        Some(format!("api token '{}' used", token.token_name)),
        Some(json!({
            "token_id": token.id,
            "token_prefix": token.token_prefix,
            "principal_type": map_principal_type_from_storage(token.principal_type),
            "principal_id": token.principal_id,
        })),
    );
}

fn record_auth_access_denied_event(
    state: &AppState,
    endpoint: &str,
    required_scope: Option<&str>,
    auth: &AuthContext,
) {
    record_auth_event(
        state,
        "aion:AuthAccessDenied",
        EventSeverity::Warning,
        Some(format!("authentication required for {endpoint}")),
        Some(json!({
            "endpoint": endpoint,
            "required_scope": required_scope,
            "principal_type": auth.principal.principal_type,
            "principal_id": auth.principal.principal_id,
            "auth_mode": auth.mode.as_str(),
        })),
    );
}

fn record_auth_scope_denied_event(
    state: &AppState,
    endpoint: &str,
    required_scopes: &[&str],
    auth: &AuthContext,
) {
    record_auth_event(
        state,
        "aion:AuthScopeDenied",
        EventSeverity::Warning,
        Some(format!("scope denied for {endpoint}")),
        Some(json!({
            "endpoint": endpoint,
            "required_scopes": required_scopes,
            "principal_type": auth.principal.principal_type,
            "principal_id": auth.principal.principal_id,
            "granted_scopes": auth.principal.scopes,
        })),
    );
}

fn record_token_rejected_event(
    state: &AppState,
    token_prefix: Option<String>,
    reason: TokenRejectionReason,
) {
    record_auth_event(
        state,
        "aion:ApiTokenRejected",
        EventSeverity::Warning,
        Some(format!("api token rejected: {}", reason.as_str())),
        Some(json!({
            "token_prefix": token_prefix,
            "reason": reason.as_str(),
        })),
    );
}

pub(crate) struct EventDraft {
    pub(crate) event_type: String,
    pub(crate) severity: EventSeverity,
    pub(crate) source_entity_id: Option<Uuid>,
    pub(crate) target_entity_id: Option<Uuid>,
    pub(crate) message: Option<String>,
    pub(crate) occurred_at: DateTime<Utc>,
    pub(crate) observed_at: Option<DateTime<Utc>>,
    pub(crate) correlation_id: Option<String>,
    pub(crate) raw_message_id: Option<Uuid>,
    pub(crate) observation_id: Option<Uuid>,
    pub(crate) command_id: Option<Uuid>,
    pub(crate) action_id: Option<Uuid>,
    pub(crate) action_result_id: Option<Uuid>,
    pub(crate) metadata: Option<Value>,
}

pub(crate) fn mutate_command(
    state: &AppState,
    command_id: Uuid,
    event_type: &'static str,
    severity: EventSeverity,
    mutate: impl FnOnce(&mut Command, DateTime<Utc>) -> Result<(), aion_action::ActionModelError>,
) -> Result<Json<Command>, ApiError> {
    let command = mutate_command_raw(state, command_id, mutate)?;
    record_command_event(state, event_type, severity, &command, None)?;
    Ok(Json(command))
}

#[allow(dead_code)]
fn ensure_rule_action_targets_exist(state: &AppState, action: &RuleAction) -> Result<(), ApiError> {
    match action {
        RuleAction::CreateEvent {
            source_entity_id,
            target_entity_id,
            ..
        } => {
            if let Some(source_entity_id) = source_entity_id {
                ensure_entity_exists(state, *source_entity_id)?;
            }
            if let Some(target_entity_id) = target_entity_id {
                ensure_entity_exists(state, *target_entity_id)?;
            }
        }
        RuleAction::CreateCommand {
            target_entity_id, ..
        } => ensure_entity_exists(state, *target_entity_id)?,
    }

    Ok(())
}

fn ensure_rule_action_targets_exist_with_auth(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    action: &RuleAction,
) -> Result<(), ApiError> {
    match action {
        RuleAction::CreateEvent {
            source_entity_id,
            target_entity_id,
            ..
        } => {
            if let Some(source_entity_id) = source_entity_id {
                require_same_tenant_for_target_entity(state, auth, endpoint, *source_entity_id)?;
            }
            if let Some(target_entity_id) = target_entity_id {
                require_same_tenant_for_target_entity(state, auth, endpoint, *target_entity_id)?;
            }
        }
        RuleAction::CreateCommand {
            target_entity_id, ..
        } => {
            require_same_tenant_for_target_entity(state, auth, endpoint, *target_entity_id)?;
        }
    }

    Ok(())
}

pub(crate) fn evaluate_rules_for_observation(
    state: &AppState,
    observation: &Observation,
    automatic_only: bool,
) -> Result<RuleEvaluationResponse, ApiError> {
    let rules = state.storage.list_rules(state.tenant_id)?;
    let mut response = RuleEvaluationResponse {
        results: Vec::new(),
        generated_commands: Vec::new(),
        generated_events: Vec::new(),
    };

    for rule in rules.into_iter().filter(|rule| {
        rule.enabled
            && rule.trigger_type == RuleTriggerType::ObservationCreated
            && rule
                .target_entity_id
                .map(|id| id == observation.feature_of_interest_id)
                .unwrap_or(true)
            && rule
                .observed_property
                .as_deref()
                .map(|property| property == observation.observed_property)
                .unwrap_or(true)
    }) {
        let actual = observation_value_to_json(&observation.value);
        let matched = rule
            .condition
            .matches(&actual)
            .map_err(|err| ApiError::bad_request(err.to_string()))?;

        if !matched {
            if !automatic_only {
                response.results.push(RuleEvaluationResult::skipped(
                    rule.id,
                    "condition did not match",
                ));
            }
            continue;
        }

        let result = apply_rule_action(
            state,
            &rule,
            Some(observation),
            None,
            &mut response.generated_commands,
            &mut response.generated_events,
        )?;
        response.results.push(result);
    }

    Ok(response)
}

pub(crate) fn evaluate_rules_for_event(
    state: &AppState,
    event: &Event,
    automatic_only: bool,
) -> Result<RuleEvaluationResponse, ApiError> {
    if automatic_only && is_rule_generated_event(event) {
        return Ok(RuleEvaluationResponse {
            results: Vec::new(),
            generated_commands: Vec::new(),
            generated_events: Vec::new(),
        });
    }

    let rules = state.storage.list_rules(state.tenant_id)?;
    let mut response = RuleEvaluationResponse {
        results: Vec::new(),
        generated_commands: Vec::new(),
        generated_events: Vec::new(),
    };

    for rule in rules.into_iter().filter(|rule| {
        rule.enabled
            && rule.trigger_type == RuleTriggerType::EventCreated
            && rule
                .target_entity_id
                .map(|id| event.target_entity_id == Some(id))
                .unwrap_or(true)
            && rule
                .event_type
                .as_deref()
                .map(|event_type| event.event_type == event_type)
                .unwrap_or(true)
    }) {
        let actual = event_condition_value(event);
        let matched = rule
            .condition
            .matches(&actual)
            .map_err(|err| ApiError::bad_request(err.to_string()))?;

        if !matched {
            if !automatic_only {
                response.results.push(RuleEvaluationResult::skipped(
                    rule.id,
                    "condition did not match",
                ));
            }
            continue;
        }

        let result = apply_rule_action(
            state,
            &rule,
            None,
            Some(event),
            &mut response.generated_commands,
            &mut response.generated_events,
        )?;
        response.results.push(result);
    }

    Ok(response)
}

fn apply_rule_action(
    state: &AppState,
    rule: &Rule,
    observation: Option<&Observation>,
    event: Option<&Event>,
    generated_commands: &mut Vec<Command>,
    generated_events: &mut Vec<Event>,
) -> Result<RuleEvaluationResult, ApiError> {
    let mut result = RuleEvaluationResult {
        rule_id: rule.id,
        matched: true,
        generated_command_ids: Vec::new(),
        generated_event_ids: Vec::new(),
        reason: None,
    };

    match &rule.action {
        RuleAction::CreateCommand {
            target_entity_id,
            command_type,
            payload,
            requested_by,
            reason,
            metadata,
        } => {
            let command = create_rule_command(
                state,
                rule,
                *target_entity_id,
                command_type,
                enrich_rule_payload(payload.clone(), rule, observation, event, metadata),
                requested_by.clone(),
                reason.clone(),
            )?;
            result.generated_command_ids.push(command.id);
            generated_commands.push(command);
        }
        RuleAction::CreateEvent {
            event_type,
            severity,
            source_entity_id,
            target_entity_id,
            message,
            metadata,
        } => {
            let event = create_rule_event(
                state,
                rule,
                event_type,
                severity.clone(),
                *source_entity_id,
                *target_entity_id,
                message.clone(),
                observation,
                event,
                metadata.clone(),
            )?;
            result.generated_event_ids.push(event.id);
            generated_events.push(event);
        }
    }

    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn create_rule_command(
    state: &AppState,
    rule: &Rule,
    target_entity_id: Uuid,
    command_type: &str,
    payload: Value,
    requested_by: Option<String>,
    reason: Option<String>,
) -> Result<Command, ApiError> {
    ensure_entity_exists(state, target_entity_id)?;
    let (approval_status, mut policy_decision) =
        command_policy_decision(state, target_entity_id, command_type)?;
    if let Some(object) = policy_decision.as_object_mut() {
        object.insert("source".to_string(), json!("rule_engine"));
        object.insert("rule_id".to_string(), json!(rule.id));
    }

    let command = Command::new(
        state.tenant_id,
        target_entity_id,
        command_type,
        payload,
        requested_by.or_else(|| Some("aion-rule-engine".to_string())),
        reason.or_else(|| Some(format!("generated by rule '{}'", rule.name))),
        Some(approval_status),
        Some(policy_decision),
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let command = state.storage.store_command(command)?;
    record_command_event(
        state,
        "aion:CommandCreated",
        EventSeverity::Info,
        &command,
        Some(format!("generated by rule '{}'", rule.name)),
    )?;
    Ok(command)
}

#[allow(clippy::too_many_arguments)]
fn create_rule_event(
    state: &AppState,
    rule: &Rule,
    event_type: &str,
    severity: EventSeverity,
    source_entity_id: Option<Uuid>,
    target_entity_id: Option<Uuid>,
    message: Option<String>,
    observation: Option<&Observation>,
    source_event: Option<&Event>,
    metadata: Option<Value>,
) -> Result<Event, ApiError> {
    if let Some(source_entity_id) = source_entity_id {
        ensure_entity_exists(state, source_entity_id)?;
    }
    if let Some(target_entity_id) = target_entity_id {
        ensure_entity_exists(state, target_entity_id)?;
    }

    let event = Event::new(
        state.tenant_id,
        event_type,
        severity,
        source_entity_id.or_else(|| observation.map(|observation| observation.producer_entity_id)),
        target_entity_id.or_else(|| {
            observation
                .map(|observation| observation.feature_of_interest_id)
                .or_else(|| source_event.and_then(|event| event.target_entity_id))
        }),
        message,
        Utc::now(),
        observation.map(|observation| observation.observed_at),
        None,
        observation.and_then(|observation| observation.raw_message_id),
        observation.map(|observation| observation.id),
        None,
        None,
        None,
        Some(rule_event_metadata(
            rule,
            metadata,
            observation,
            source_event,
        )),
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok(state.storage.store_event(event)?)
}

fn enrich_rule_payload(
    mut payload: Value,
    rule: &Rule,
    observation: Option<&Observation>,
    event: Option<&Event>,
    action_metadata: &Option<Value>,
) -> Value {
    if !payload.is_object() {
        payload = json!({ "value": payload });
    }

    if let Some(object) = payload.as_object_mut() {
        object.insert("rule_id".to_string(), json!(rule.id));
        object.insert("rule_name".to_string(), json!(rule.name));
        if let Some(observation) = observation {
            object.insert("observation_id".to_string(), json!(observation.id));
            object.insert(
                "observed_property".to_string(),
                json!(observation.observed_property),
            );
            object.insert(
                "observed_value".to_string(),
                observation_value_to_json(&observation.value),
            );
        }
        if let Some(event) = event {
            object.insert("event_id".to_string(), json!(event.id));
            object.insert("event_type".to_string(), json!(event.event_type));
        }
        if let Some(action_metadata) = action_metadata {
            object.insert("rule_action_metadata".to_string(), action_metadata.clone());
        }
    }

    payload
}

fn rule_event_metadata(
    rule: &Rule,
    metadata: Option<Value>,
    observation: Option<&Observation>,
    source_event: Option<&Event>,
) -> Value {
    let mut enriched = json!({
        "source": "rule_engine",
        "rule_id": rule.id,
        "rule_name": rule.name,
        "rule_generated": true
    });

    if let Some(object) = enriched.as_object_mut() {
        if let Some(metadata) = metadata {
            object.insert("rule_action_metadata".to_string(), metadata);
        }
        if let Some(observation) = observation {
            object.insert("observation_id".to_string(), json!(observation.id));
            object.insert(
                "observed_property".to_string(),
                json!(observation.observed_property),
            );
            object.insert(
                "observed_value".to_string(),
                observation_value_to_json(&observation.value),
            );
        }
        if let Some(source_event) = source_event {
            object.insert("source_event_id".to_string(), json!(source_event.id));
            object.insert(
                "source_event_type".to_string(),
                json!(source_event.event_type),
            );
        }
    }

    enriched
}

fn observation_value_to_json(value: &ObservationValue) -> Value {
    match value {
        ObservationValue::Number { value } => json!(value),
        ObservationValue::Text { value } => json!(value),
        ObservationValue::Bool { value } => json!(value),
        ObservationValue::Json { value } => value.clone(),
    }
}

fn event_condition_value(event: &Event) -> Value {
    event
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("value"))
        .cloned()
        .unwrap_or_else(|| json!(event.event_type))
}

fn is_rule_generated_event(event: &Event) -> bool {
    event.metadata.as_ref().is_some_and(|metadata| {
        metadata
            .get("rule_generated")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    })
}

pub(crate) fn command_policy_decision(
    state: &AppState,
    target_entity_id: Uuid,
    command_type: &str,
) -> Result<(ApprovalStatus, Value), ApiError> {
    let policies = state.storage.query_policies(state.tenant_id, None, None)?;
    let mut matching_policies = policies
        .into_iter()
        .filter(|policy| policy.matches(target_entity_id, command_type))
        .collect::<Vec<_>>();

    matching_policies.sort_by_key(|policy| {
        (
            policy.target_entity_id.is_none(),
            policy.command_type.is_none(),
            policy.id,
        )
    });

    let requires_approval = matching_policies
        .iter()
        .any(|policy| policy.requires_approval);
    let auto_execute_allowed = matching_policies
        .iter()
        .any(|policy| policy.auto_execute_allowed);
    let approval_status = if requires_approval {
        ApprovalStatus::Required
    } else {
        ApprovalStatus::NotRequired
    };
    let matched_policy_ids = matching_policies
        .iter()
        .map(|policy| policy.id)
        .collect::<Vec<_>>();
    let matched_policy_count = matched_policy_ids.len();

    Ok((
        approval_status,
        json!({
            "matched_policy_ids": matched_policy_ids,
            "matched_policy_count": matched_policy_count,
            "requires_approval": requires_approval,
            "auto_execute_allowed": auto_execute_allowed,
            "safe_default": matched_policy_count == 0
        }),
    ))
}

pub(crate) fn empty_object() -> Value {
    json!({})
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use aion_relationship::Relationship;
    use aion_storage::{
        ApiTokenStore, CommandStore, DlqStore, EventStore, IngestionConnector,
        IngestionConnectorType, ObservationStore, RawMessageStore, RelationshipStore,
    };
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use chrono::Duration;
    use serde_json::json;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_reports_memory_storage() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_json(response).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["storage"], "memory");
    }

    #[tokio::test]
    async fn ready_reports_memory_storage_as_ready() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_json(response).await;
        assert_eq!(body["ready"], true);
        assert_eq!(body["storage"], "memory");
        assert_eq!(body["auth"]["mode"], "dev");
        assert_eq!(body["auth"]["dev_bypass"], true);
        assert_eq!(body["auth"]["enforcement_level"], "none");
        assert_eq!(body["auth"]["protected_endpoint_groups"], json!([]));
        assert_eq!(body["auth"]["bootstrap_admin_configured"], false);
        assert_eq!(body["mqtt"]["enabled"], false);
        assert_eq!(body["migrations_ready"], Value::Null);
    }

    #[tokio::test]
    async fn dashboard_static_serving_is_disabled_by_default() {
        let app = app();

        let static_response = app
            .clone()
            .oneshot(Request::builder().uri("/ui/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(static_response.status(), StatusCode::NOT_FOUND);

        let dashboard_response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(dashboard_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dashboard_static_serving_returns_index_for_ui_root() {
        let temp_dir = create_temp_dashboard_dir();
        fs::write(
            temp_dir.join("index.html"),
            "<!doctype html><html><body>Aion Dashboard</body></html>",
        )
        .unwrap();
        fs::write(temp_dir.join("dashboard.js"), "console.log('entry');").unwrap();

        let app = app_with_state_and_dashboard_static(
            AppState::local(),
            DashboardStaticConfig::from_env_var(Some(temp_dir.to_string_lossy().into_owned()))
                .unwrap(),
        );

        let ui_root = app
            .clone()
            .oneshot(Request::builder().uri("/ui").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(
            matches!(
                ui_root.status(),
                StatusCode::OK | StatusCode::PERMANENT_REDIRECT | StatusCode::TEMPORARY_REDIRECT
            ),
            "unexpected /ui status {}",
            ui_root.status()
        );
        if ui_root.status().is_redirection() {
            assert_eq!(ui_root.headers()["location"], "/ui/");
        } else {
            assert!(response_text(ui_root).await.contains("Aion Dashboard"));
        }

        let index = app
            .clone()
            .oneshot(Request::builder().uri("/ui/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(index.status(), StatusCode::OK);
        assert!(response_text(index).await.contains("Aion Dashboard"));

        let entrypoint = app
            .oneshot(
                Request::builder()
                    .uri("/ui/dashboard.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(entrypoint.status(), StatusCode::OK);
        assert!(response_text(entrypoint)
            .await
            .contains("console.log('entry');"));
    }

    #[tokio::test]
    async fn dashboard_static_serving_does_not_shadow_dashboard_api_routes() {
        let temp_dir = create_temp_dashboard_dir();
        fs::write(temp_dir.join("index.html"), "static root").unwrap();
        fs::create_dir_all(temp_dir.join("dashboard")).unwrap();
        fs::write(temp_dir.join("dashboard").join("overview"), "not api").unwrap();

        let app = app_with_state_and_dashboard_static(
            AppState::local(),
            DashboardStaticConfig::from_env_var(Some(temp_dir.to_string_lossy().into_owned()))
                .unwrap(),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert!(body["generated_at"].is_string());
        assert!(body["entities_count"].is_number());
    }

    #[test]
    fn dashboard_static_config_rejects_invalid_directory() {
        let missing_dir = create_temp_dashboard_dir().join("missing");
        let error =
            DashboardStaticConfig::from_env_var(Some(missing_dir.to_string_lossy().into_owned()))
                .unwrap_err();

        assert!(error.to_string().contains(DASHBOARD_STATIC_ENV_VAR));
    }

    #[tokio::test]
    async fn ready_reports_disabled_auth_mode_without_enforcement() {
        let state = AppState::with_backend_storage_and_auth(
            Arc::new(InMemoryStorage::new()),
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Disabled,
                bootstrap_admin_token_hash: None,
            },
            Uuid::nil(),
        );
        let response = app_with_state(state)
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_json(response).await;
        assert_eq!(body["auth"]["mode"], "disabled");
        assert_eq!(body["auth"]["dev_bypass"], false);
        assert_eq!(body["auth"]["enforcement_level"], "none");
        assert_eq!(body["auth"]["protected_endpoint_groups"], json!([]));
        assert_eq!(body["auth"]["bootstrap_admin_configured"], false);
    }

    #[tokio::test]
    async fn ready_reports_token_auth_mode_as_partial_enforcement() {
        let state = AppState::with_backend_storage_and_auth(
            Arc::new(InMemoryStorage::new()),
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Token,
                bootstrap_admin_token_hash: None,
            },
            Uuid::nil(),
        );
        let response = app_with_state(state)
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_json(response).await;
        assert_eq!(body["auth"]["mode"], "token");
        assert_eq!(body["auth"]["dev_bypass"], false);
        assert_eq!(body["auth"]["enforcement_level"], "partial");
        assert_eq!(
            body["auth"]["protected_endpoint_groups"],
            json!([
                "auth_tokens",
                "connector_secrets",
                "adapters",
                "executors",
                "smartsentinel_executor_bridge",
                "ingestion_connectors",
                "connector_workers",
                "connector_aware_ingestion",
                "generic_http_ingestion",
                "reliable_ingestion",
                "batch_ingestion",
                "sync_sessions",
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
                "dashboard",
                "dlq",
                "flows",
                "flow_execution",
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
                "executor_config_writes"
            ])
        );
        assert_eq!(body["auth"]["bootstrap_admin_configured"], false);
    }

    #[tokio::test]
    async fn ready_reports_dashboard_as_protected_group_in_token_mode() {
        let state = AppState::with_backend_storage_and_auth(
            Arc::new(InMemoryStorage::new()),
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Token,
                bootstrap_admin_token_hash: None,
            },
            Uuid::nil(),
        );
        let response = app_with_state(state)
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_json(response).await;
        assert!(body["auth"]["protected_endpoint_groups"]
            .as_array()
            .unwrap()
            .iter()
            .any(|group| group == "dashboard"));
    }

    #[tokio::test]
    async fn ready_reports_bootstrap_admin_configured_without_exposing_token_value() {
        let bootstrap_token = "bootstrap-admin-token-123456";
        let state = AppState::with_backend_storage_and_auth(
            Arc::new(InMemoryStorage::new()),
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Token,
                bootstrap_admin_token_hash: Some(hash_token_value(bootstrap_token)),
            },
            Uuid::nil(),
        );
        let response = app_with_state(state)
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_json(response).await;
        assert_eq!(body["auth"]["bootstrap_admin_configured"], true);
        assert!(!body.to_string().contains(bootstrap_token));
        assert!(body["auth"]["bootstrap_admin_token"].is_null());
    }

    #[tokio::test]
    async fn whoami_reports_dev_mode_context() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/auth/whoami")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body["auth_mode"], "dev");
        assert_eq!(body["authenticated"], false);
        assert_eq!(body["dev_bypass"], true);
        assert_eq!(body["principal_type"], "anonymous");
    }

    #[tokio::test]
    async fn create_token_returns_raw_token_once_and_stored_record_hides_hash() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/auth/tokens",
                json!({
                    "token_name": "ops",
                    "principal_type": "service",
                    "principal_id": "service-01",
                    "scopes": ["entities:read"],
                    "metadata": {"purpose": "test"}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let created = to_json(response).await;
        assert!(created["raw_token"].as_str().unwrap().starts_with("aion_"));
        assert!(created["token"].get("token_hash").is_none());
        assert!(created["token"].get("raw_token").is_none());

        let token_id = created["token"]["id"].as_str().unwrap();
        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/auth/tokens")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);
        let listed = to_json(list_response).await;
        assert_eq!(listed.as_array().unwrap().len(), 1);
        assert!(listed[0].get("token_hash").is_none());
        assert!(listed[0].get("raw_token").is_none());
        assert_eq!(listed[0]["id"], token_id);

        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/auth/tokens/{token_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        let fetched = to_json(get_response).await;
        assert!(fetched.get("token_hash").is_none());
        assert!(fetched.get("raw_token").is_none());
        assert_eq!(fetched["token_prefix"], created["token"]["token_prefix"]);
    }

    #[tokio::test]
    async fn token_mode_whoami_resolves_valid_bearer_token_and_revoke_blocks_later_use() {
        let bootstrap_app = app();
        let create_response = bootstrap_app
            .clone()
            .oneshot(json_request(
                "POST",
                "/auth/tokens",
                json!({
                    "token_name": "bootstrap",
                    "principal_type": "service",
                    "principal_id": "bootstrap-service",
                    "scopes": ["entities:read", "observations:read", "auth:tokens:admin"]
                }),
            ))
            .await
            .unwrap();
        let created = to_json(create_response).await;
        let raw_token = created["raw_token"].as_str().unwrap().to_string();
        let token_id = created["token"]["id"].as_str().unwrap().to_string();

        let storage = Arc::new(InMemoryStorage::new());
        let copied = bootstrap_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/auth/tokens/{token_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let copied = to_json(copied).await;
        let token_record = ApiToken::new(
            Uuid::nil(),
            copied["token_name"].as_str().unwrap(),
            copied["token_prefix"].as_str().unwrap(),
            hash_token_value(&raw_token),
            ApiTokenPrincipalType::Service,
            copied["principal_id"].as_str().map(ToOwned::to_owned),
            copied["scopes"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                .collect(),
            None,
            copied.get("metadata").cloned(),
            Utc::now(),
        )
        .unwrap();
        let token_record = ApiToken {
            id: Uuid::parse_str(&token_id).unwrap(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            ..token_record
        };
        storage.create_api_token(token_record).unwrap();

        let token_app = app_with_state(AppState::with_backend_storage_and_auth(
            storage.clone(),
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Token,
                bootstrap_admin_token_hash: None,
            },
            Uuid::nil(),
        ));

        let whoami = token_app
            .clone()
            .oneshot(auth_request("GET", "/auth/whoami", &raw_token))
            .await
            .unwrap();
        assert_eq!(whoami.status(), StatusCode::OK);
        let body = to_json(whoami).await;
        assert_eq!(body["auth_mode"], "token");
        assert_eq!(body["authenticated"], true);
        assert_eq!(body["principal_type"], "service");
        assert_eq!(body["principal_id"], "bootstrap-service");

        let revoke = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                &format!("/auth/tokens/{token_id}/revoke"),
                json!({}),
                &raw_token,
            ))
            .await
            .unwrap();
        assert_eq!(revoke.status(), StatusCode::OK);

        let rejected = token_app
            .clone()
            .oneshot(auth_request("GET", "/auth/whoami", &raw_token))
            .await
            .unwrap();
        let rejected = to_json(rejected).await;
        assert_eq!(rejected["authenticated"], false);
        assert_eq!(rejected["principal_type"], "anonymous");
    }

    #[tokio::test]
    async fn invalid_and_expired_tokens_do_not_authenticate_and_do_not_expose_secrets() {
        let now = Utc::now();
        let storage = Arc::new(InMemoryStorage::new());
        let valid_token = "aion_expired01_deadbeef";
        let expired = ApiToken::new(
            Uuid::nil(),
            "expired",
            "expired01",
            hash_token_value(valid_token),
            ApiTokenPrincipalType::Service,
            Some("service-expired".to_string()),
            vec!["entities:read".to_string()],
            Some(now - Duration::minutes(1)),
            None,
            now,
        )
        .unwrap();
        storage.create_api_token(expired).unwrap();

        let app = app_with_state(AppState::with_backend_storage_and_auth(
            storage,
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Token,
                bootstrap_admin_token_hash: None,
            },
            Uuid::nil(),
        ));

        let invalid = app
            .clone()
            .oneshot(auth_request("GET", "/auth/whoami", "aion_invalid01_secret"))
            .await
            .unwrap();
        let invalid = to_json(invalid).await;
        assert_eq!(invalid["authenticated"], false);
        assert!(!invalid.to_string().contains("invalid01"));

        let expired = app
            .clone()
            .oneshot(auth_request("GET", "/auth/whoami", valid_token))
            .await
            .unwrap();
        let expired = to_json(expired).await;
        assert_eq!(expired["authenticated"], false);
        assert_eq!(expired["principal_type"], "anonymous");
    }

    #[tokio::test]
    async fn token_mode_requires_bearer_for_token_management_and_selected_writes() {
        let state = AppState::with_backend_storage_and_auth(
            Arc::new(InMemoryStorage::new()),
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Token,
                bootstrap_admin_token_hash: None,
            },
            Uuid::nil(),
        );
        let app = app_with_state(state);

        let whoami = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/auth/whoami")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(whoami.status(), StatusCode::OK);

        let create_token = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/auth/tokens",
                json!({
                    "token_name": "blocked",
                    "principal_type": "service",
                    "scopes": []
                }),
            ))
            .await
            .unwrap();
        assert_eq!(create_token.status(), StatusCode::UNAUTHORIZED);

        let create_entity = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/entities",
                json!({
                    "entity_key": "token-open-write-01",
                    "entity_type": "aion:Sensor",
                    "jsonld": {
                        "@context": {"aion": "https://aioncore.org/ns#"},
                        "@id": "urn:aion:test:token-open-write-01",
                        "@type": "aion:Sensor"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(create_entity.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn dev_mode_allows_protected_adapter_registration_without_token() {
        let response = app()
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": "dev-adapter-01",
                    "display_name": "Dev Adapter 01",
                    "adapter_type": "edge",
                    "status": "online"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn dev_mode_allows_connector_create_without_token() {
        let response = app()
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": "dev-connector-01",
                    "connector_type": "http",
                    "connector_profile": "custom",
                    "enabled": true,
                    "protocol": "http",
                    "endpoint": "/ingestion/connectors/{connector_id}/ingest",
                    "http_path": "/ingestion/connectors/{connector_id}/ingest",
                    "payload_format": "senml-json",
                    "content_type": "application/senml+json"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn disabled_mode_allows_protected_adapter_registration_without_token() {
        let app = disabled_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": "disabled-adapter-01",
                    "display_name": "Disabled Adapter 01",
                    "adapter_type": "edge",
                    "status": "online"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn token_mode_rejects_protected_adapter_registration_without_token() {
        let app = token_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": "token-adapter-01",
                    "display_name": "Token Adapter 01",
                    "adapter_type": "edge",
                    "status": "online"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_json(response).await;
        assert!(body["error"].as_str().unwrap().contains("bearer token"));
    }

    #[tokio::test]
    async fn token_mode_rejects_invalid_token_for_protected_adapter_registration() {
        let app = token_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(auth_json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": "token-adapter-02",
                    "display_name": "Token Adapter 02",
                    "adapter_type": "edge",
                    "status": "online"
                }),
                "aion_invalid01_secret",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_json(response).await;
        assert!(!body.to_string().contains("aion_invalid01_secret"));
    }

    #[tokio::test]
    async fn token_mode_rejects_connector_create_without_token() {
        let app = token_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": "token-connector-01",
                    "connector_type": "http",
                    "connector_profile": "custom",
                    "enabled": true,
                    "protocol": "http",
                    "endpoint": "/ingestion/connectors/{connector_id}/ingest",
                    "http_path": "/ingestion/connectors/{connector_id}/ingest",
                    "payload_format": "senml-json",
                    "content_type": "application/senml+json"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = to_json(response).await;
        assert!(body["error"].as_str().unwrap().contains("bearer token"));
    }

    #[tokio::test]
    async fn token_mode_rejects_valid_token_without_required_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("service-no-adapter-scope"),
            &["entities:read"],
        );
        let app = token_mode_app_with_storage(storage);
        let response = app
            .oneshot(auth_json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": "token-adapter-03",
                    "display_name": "Token Adapter 03",
                    "adapter_type": "edge",
                    "status": "online"
                }),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_json(response).await;
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("adapters:register"));
    }

    #[tokio::test]
    async fn token_mode_rejects_connector_create_with_valid_token_missing_connectors_admin() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("connector-reader"),
            &["connectors:read"],
        );
        let app = token_mode_app_with_storage(storage);
        let response = app
            .oneshot(auth_json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": "token-connector-02",
                    "connector_type": "http",
                    "connector_profile": "custom",
                    "enabled": true,
                    "protocol": "http",
                    "endpoint": "/ingestion/connectors/{connector_id}/ingest",
                    "http_path": "/ingestion/connectors/{connector_id}/ingest",
                    "payload_format": "senml-json",
                    "content_type": "application/senml+json"
                }),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_json(response).await;
        assert!(body["error"].as_str().unwrap().contains("connectors:admin"));
    }

    #[tokio::test]
    async fn token_mode_allows_connector_create_with_connectors_admin() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("connector-admin"),
            &["connectors:admin"],
        );
        let app = token_mode_app_with_storage(storage);
        let response = app
            .oneshot(auth_json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": "token-connector-03",
                    "connector_type": "http",
                    "connector_profile": "custom",
                    "enabled": true,
                    "protocol": "http",
                    "endpoint": "/ingestion/connectors/{connector_id}/ingest",
                    "http_path": "/ingestion/connectors/{connector_id}/ingest",
                    "payload_format": "senml-json",
                    "content_type": "application/senml+json"
                }),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn token_mode_allows_adapter_heartbeat_with_required_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let adapter = dev_app
            .clone()
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": "heartbeat-adapter-01",
                    "display_name": "Heartbeat Adapter 01",
                    "adapter_type": "edge",
                    "status": "online"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(adapter.status(), StatusCode::CREATED);
        let adapter = to_json(adapter).await;
        let adapter_id = adapter["adapter"]["id"].as_str().unwrap();
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Adapter,
            Some("heartbeat-adapter-01"),
            &["adapters:heartbeat"],
        );

        let response = token_app
            .oneshot(auth_json_request(
                "PUT",
                &format!("/adapters/{adapter_id}/heartbeat"),
                json!({
                    "status": "online",
                    "metadata": {"source": "test"}
                }),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_all_satisfies_protected_adapter_registration() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Admin,
            Some("platform-admin"),
            &["admin:all"],
        );
        let app = token_mode_app_with_storage(storage);
        let response = app
            .oneshot(auth_json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": "admin-adapter-01",
                    "display_name": "Admin Adapter 01",
                    "adapter_type": "edge",
                    "status": "online"
                }),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn admin_all_allows_connector_administration() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Admin,
            Some("platform-admin"),
            &["admin:all"],
        );
        let app = token_mode_app_with_storage(storage);
        let response = app
            .oneshot(auth_json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": "admin-connector-01",
                    "connector_type": "http",
                    "connector_profile": "custom",
                    "enabled": true,
                    "protocol": "http",
                    "endpoint": "/ingestion/connectors/{connector_id}/ingest",
                    "http_path": "/ingestion/connectors/{connector_id}/ingest",
                    "payload_format": "senml-json",
                    "content_type": "application/senml+json"
                }),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn token_mode_allows_connector_read_with_connectors_read() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let connector = create_http_connector(&dev_app, "connector-read-01", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("connector-reader"),
            &["connectors:read"],
        );

        let listed = token_app
            .clone()
            .oneshot(auth_request("GET", "/ingestion/connectors", &raw_token))
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);

        let fetched = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/ingestion/connectors/{connector_id}"),
                &raw_token,
            ))
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);

        let status = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/ingestion/connectors/{connector_id}/status"),
                &raw_token,
            ))
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);

        let plan = token_app
            .oneshot(auth_request("GET", "/ingestion/workers/plan", &raw_token))
            .await
            .unwrap();
        assert_eq!(plan.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn connector_aware_ingestion_requires_ingestion_write() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let sensor_id =
            create_test_entity(&dev_app, "connector-ingest-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "connector-ingest-plot-01", "aion:Plot").await;
        let connector = create_http_connector(
            &dev_app,
            "connector-ingest-01",
            Some(&sensor_id),
            Some(&plot_id),
        )
        .await;
        let connector_id = connector["id"].as_str().unwrap();

        let denied = token_app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": [
                        {
                            "bn": "urn:aion:test:connector-ingest:",
                            "n": "soil_moisture",
                            "u": "%",
                            "v": 18.5
                        }
                    ]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Connector,
            Some("connector-ingest-01"),
            &["ingestion:write"],
        );
        let allowed = token_app
            .oneshot(auth_json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": [
                        {
                            "bn": "urn:aion:test:connector-ingest:",
                            "n": "soil_moisture",
                            "u": "%",
                            "v": 18.5
                        }
                    ]
                }),
                &raw_token,
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn generic_ingest_auth_respects_dev_bypass_and_token_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let sensor_id =
            create_test_entity(&dev_app, "generic-ingest-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "generic-ingest-plot-01", "aion:Plot").await;
        let body = json!({
            "producer_entity_id": sensor_id,
            "feature_of_interest_id": plot_id,
            "payload_format": "canonical-json",
            "protocol": "http",
            "content_type": "application/json",
            "payload": {
                "observations": [
                    {
                        "observed_property": "aion:SoilMoisture",
                        "value": {"type": "number", "value": 18.5},
                        "unit": "%",
                        "observed_at": "2026-05-05T12:00:00Z"
                    }
                ]
            }
        });

        let dev_allowed = dev_app
            .clone()
            .oneshot(json_request("POST", "/ingest/http", body.clone()))
            .await
            .unwrap();
        assert_eq!(dev_allowed.status(), StatusCode::CREATED);

        let missing_token = token_app
            .clone()
            .oneshot(json_request("POST", "/ingest/http", body.clone()))
            .await
            .unwrap();
        assert_eq!(missing_token.status(), StatusCode::UNAUTHORIZED);

        let wrong_scope = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/ingest/http",
                body.clone(),
                &store_api_token(
                    &storage,
                    ApiTokenPrincipalType::Service,
                    Some("generic-ingest-reader"),
                    &["connectors:read"],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(wrong_scope.status(), StatusCode::FORBIDDEN);

        let allowed = token_app
            .oneshot(auth_json_request(
                "POST",
                "/ingest/http",
                body,
                &store_api_token(
                    &storage,
                    ApiTokenPrincipalType::Service,
                    Some("generic-ingest-writer"),
                    &["ingestion:write"],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn reliable_ingest_creates_raw_message_and_observations_with_provenance_metadata() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&app, "reliable-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "reliable-plot-01", "aion:Plot").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/reliable",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "protocol": "http",
                    "payload_format": "senml-json",
                    "source_system": "minifi",
                    "source_id": "edge-01",
                    "idempotency_key": "tenant-a:reliable-01",
                    "external_flow_id": "flow-edge-sync",
                    "external_flow_name": "Edge Sync",
                    "external_flowfile_uuid": "flowfile-01",
                    "external_process_group_id": "pg-01",
                    "external_processor_id": "proc-01",
                    "external_provenance_uri": "nifi://provenance/events/1",
                    "sync_session_id": "sync-01",
                    "edge_sequence": 41,
                    "observed_at": "2026-05-06T12:00:00Z",
                    "stored_at_edge": "2026-05-06T12:00:05Z",
                    "sent_at": "2026-05-06T12:10:11Z",
                    "replay_count": 1,
                    "retry_count": 2,
                    "connectivity_state": "replayed_after_outage",
                    "payload_hash": "sha256:test",
                    "metadata": {"route": "edge-http->cloud-sync->aioncore"},
                    "payload": [
                        {"n": "soil_moisture", "u": "%", "v": 18.5}
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_json(response).await;
        assert_eq!(body["duplicate"], false);
        assert_eq!(body["observations_created"], 1);
        assert_eq!(body["payload_format"], "senml-json");
        assert_eq!(body["source_system"], "minifi");
        assert_eq!(body["sync_session_id"], "sync-01");
        assert!(body["event_id"].as_str().is_some());

        let raw_message_id = body["raw_message_id"].as_str().unwrap().parse().unwrap();
        let raw_message =
            aion_storage::RawMessageStore::get_raw_message(&*storage, Uuid::nil(), raw_message_id)
                .unwrap()
                .unwrap();
        assert_eq!(
            raw_message.idempotency_key.as_deref(),
            Some("tenant-a:reliable-01")
        );
        assert_eq!(raw_message.headers["external.source_system"], "minifi");
        assert_eq!(raw_message.headers["external.flow_id"], "flow-edge-sync");
        assert_eq!(raw_message.headers["external.flow_name"], "Edge Sync");
        assert_eq!(raw_message.headers["external.flowfile_uuid"], "flowfile-01");
        assert_eq!(raw_message.headers["external.process_group_id"], "pg-01");
        assert_eq!(raw_message.headers["external.processor_id"], "proc-01");
        assert_eq!(
            raw_message.headers["external.provenance_uri"],
            "nifi://provenance/events/1"
        );
        assert_eq!(
            raw_message.headers["external.idempotency_key"],
            "tenant-a:reliable-01"
        );
        assert_eq!(raw_message.headers["external.sync_session_id"], "sync-01");
        assert_eq!(raw_message.headers["external.edge_sequence"], 41);
        assert_eq!(
            raw_message.headers["external.connectivity_state"],
            "replayed_after_outage"
        );

        let events =
            query_events_by_raw_message(&app, body["raw_message_id"].as_str().unwrap()).await;
        let event = events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_type"] == "aion:PayloadIngested")
            .unwrap();
        assert_eq!(
            event["metadata"]["external.idempotency_key"],
            "tenant-a:reliable-01"
        );
        assert_eq!(event["metadata"]["external.source_system"], "minifi");
        assert_eq!(event["metadata"]["external.sync_session_id"], "sync-01");
        assert_eq!(event["metadata"]["duplicate"], false);
    }

    #[tokio::test]
    async fn reliable_ingest_deduplicates_by_tenant_scoped_idempotency_key() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor_id = create_test_entity(&app, "reliable-dedupe-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "reliable-dedupe-plot-01", "aion:Plot").await;
        let body = json!({
            "producer_entity_id": sensor_id,
            "feature_of_interest_id": plot_id,
            "protocol": "http",
            "payload_format": "canonical-json",
            "idempotency_key": "tenant-a:dedupe-01",
            "source_system": "smartsentinel",
            "sync_session_id": "sync-dedupe-01",
            "payload": {
                "observations": [{
                    "observed_property": "aion:SoilMoisture",
                    "value": {"type": "number", "value": 19.4},
                    "unit": "%"
                }]
            }
        });

        let first = to_json(
            app.clone()
                .oneshot(json_request("POST", "/ingest/reliable", body.clone()))
                .await
                .unwrap(),
        )
        .await;
        let second_response = app
            .clone()
            .oneshot(json_request("POST", "/ingest/reliable", body))
            .await
            .unwrap();
        assert_eq!(second_response.status(), StatusCode::OK);
        let second = to_json(second_response).await;

        assert_eq!(first["duplicate"], false);
        assert_eq!(second["duplicate"], true);
        assert_eq!(second["observations_created"], 0);
        assert_eq!(first["raw_message_id"], second["raw_message_id"]);

        let observations = get_json(
            &app,
            &format!("/observations?feature_of_interest_id={plot_id}"),
        )
        .await;
        assert_eq!(observations.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn reliable_ingest_same_idempotency_key_different_tenants_does_not_collide() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let sensor_a =
            create_test_entity(&tenant_a_app, "reliable-tenant-a-sensor", "aion:Sensor").await;
        let plot_a = create_test_entity(&tenant_a_app, "reliable-tenant-a-plot", "aion:Plot").await;
        let sensor_b =
            create_test_entity(&tenant_b_app, "reliable-tenant-b-sensor", "aion:Sensor").await;
        let plot_b = create_test_entity(&tenant_b_app, "reliable-tenant-b-plot", "aion:Plot").await;
        let token_a = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-reliable"),
            &["ingestion:write"],
        );
        let token_b = store_api_token_for_tenant(
            &storage,
            tenant_b,
            ApiTokenPrincipalType::Service,
            Some("tenant-b-reliable"),
            &["ingestion:write"],
        );
        let idempotency_key = "shared-key-01";

        let created_a = to_json(
            app.clone()
                .oneshot(auth_json_request(
                    "POST",
                    "/ingest/reliable",
                    json!({
                        "producer_entity_id": sensor_a,
                        "feature_of_interest_id": plot_a,
                        "payload_format": "canonical-json",
                        "idempotency_key": idempotency_key,
                        "payload": {
                            "observations": [{
                                "observed_property": "aion:SoilMoisture",
                                "value": {"type": "number", "value": 20.0}
                            }]
                        }
                    }),
                    &token_a,
                ))
                .await
                .unwrap(),
        )
        .await;
        let created_b = to_json(
            app.clone()
                .oneshot(auth_json_request(
                    "POST",
                    "/ingest/reliable",
                    json!({
                        "producer_entity_id": sensor_b,
                        "feature_of_interest_id": plot_b,
                        "payload_format": "canonical-json",
                        "idempotency_key": idempotency_key,
                        "payload": {
                            "observations": [{
                                "observed_property": "aion:SoilMoisture",
                                "value": {"type": "number", "value": 30.0}
                            }]
                        }
                    }),
                    &token_b,
                ))
                .await
                .unwrap(),
        )
        .await;

        assert_eq!(created_a["duplicate"], false);
        assert_eq!(created_b["duplicate"], false);
        assert_ne!(created_a["raw_message_id"], created_b["raw_message_id"]);
        assert_eq!(
            aion_storage::ObservationStore::query_observations(
                &*storage,
                tenant_a,
                Some(plot_a.parse().unwrap()),
                None,
                None,
                None,
                10,
            )
            .unwrap()
            .len(),
            1
        );
        assert_eq!(
            aion_storage::ObservationStore::query_observations(
                &*storage,
                tenant_b,
                Some(plot_b.parse().unwrap()),
                None,
                None,
                None,
                10,
            )
            .unwrap()
            .len(),
            1
        );
    }

    #[tokio::test]
    async fn reliable_ingest_without_idempotency_key_does_not_deduplicate() {
        let app = app();
        let sensor_id = create_test_entity(&app, "reliable-no-key-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "reliable-no-key-plot-01", "aion:Plot").await;
        let body = json!({
            "producer_entity_id": sensor_id,
            "feature_of_interest_id": plot_id,
            "payload_format": "canonical-json",
            "payload": {
                "observations": [{
                    "observed_property": "aion:SoilTemperature",
                    "value": {"type": "number", "value": 24.3},
                    "unit": "Cel"
                }]
            }
        });

        let first = to_json(
            app.clone()
                .oneshot(json_request("POST", "/ingest/reliable", body.clone()))
                .await
                .unwrap(),
        )
        .await;
        let second = to_json(
            app.clone()
                .oneshot(json_request("POST", "/ingest/reliable", body))
                .await
                .unwrap(),
        )
        .await;

        assert_ne!(first["raw_message_id"], second["raw_message_id"]);
        let observations = get_json(
            &app,
            &format!("/observations?feature_of_interest_id={plot_id}"),
        )
        .await;
        assert_eq!(observations.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn reliable_ingest_failure_event_preserves_idempotency_and_provenance_metadata() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&app, "reliable-fail-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "reliable-fail-plot-01", "aion:Plot").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/reliable",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "senml-json",
                    "source_system": "minifi",
                    "idempotency_key": "tenant-a:fail-01",
                    "sync_session_id": "sync-fail-01",
                    "external_provenance_uri": "nifi://provenance/events/99",
                    "payload": "not json"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let raw_message = aion_storage::RawMessageStore::list_raw_messages(&*storage, Uuid::nil())
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let events = query_events_by_raw_message(&app, &raw_message.id.to_string()).await;
        let failed = events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_type"] == "aion:PayloadIngestionFailed")
            .unwrap();
        assert_eq!(
            failed["metadata"]["external.idempotency_key"],
            "tenant-a:fail-01"
        );
        assert_eq!(failed["metadata"]["external.source_system"], "minifi");
        assert_eq!(
            failed["metadata"]["external.sync_session_id"],
            "sync-fail-01"
        );
        assert_eq!(
            failed["metadata"]["external.provenance_uri"],
            "nifi://provenance/events/99"
        );
    }

    #[tokio::test]
    async fn reliable_ingest_auth_respects_token_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let sensor_id =
            create_test_entity(&dev_app, "reliable-auth-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "reliable-auth-plot-01", "aion:Plot").await;
        let body = json!({
            "producer_entity_id": sensor_id,
            "feature_of_interest_id": plot_id,
            "payload_format": "canonical-json",
            "idempotency_key": "tenant-a:auth-01",
            "payload": {
                "observations": [{
                    "observed_property": "aion:SoilMoisture",
                    "value": {"type": "number", "value": 11.2}
                }]
            }
        });

        let missing_token = token_app
            .clone()
            .oneshot(json_request("POST", "/ingest/reliable", body.clone()))
            .await
            .unwrap();
        assert_eq!(missing_token.status(), StatusCode::UNAUTHORIZED);

        let wrong_scope = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/ingest/reliable",
                body.clone(),
                &store_api_token(
                    &storage,
                    ApiTokenPrincipalType::Service,
                    Some("reliable-ingest-reader"),
                    &["connectors:read"],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(wrong_scope.status(), StatusCode::FORBIDDEN);

        let allowed = token_app
            .oneshot(auth_json_request(
                "POST",
                "/ingest/reliable",
                body,
                &store_api_token(
                    &storage,
                    ApiTokenPrincipalType::Service,
                    Some("reliable-ingest-writer"),
                    &["ingestion:write"],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn batch_ingest_creates_multiple_items_and_reports_batch_event() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&app, "batch-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "batch-plot-01", "aion:Plot").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/batch",
                json!({
                    "batch_id": "batch-01",
                    "sync_session_id": "sync-01",
                    "source_system": "smartsentinel",
                    "source_id": "edge-01",
                    "connectivity_state": "reconnected_backfill",
                    "metadata": {"transport": "store-and-forward"},
                    "items": [
                        {
                            "producer_entity_id": sensor_id,
                            "feature_of_interest_id": plot_id,
                            "payload_format": "canonical-json",
                            "idempotency_key": "tenant-a:batch-01",
                            "payload": {
                                "observations": [{
                                    "observed_property": "aion:SoilMoisture",
                                    "value": {"type": "number", "value": 19.2}
                                }]
                            }
                        },
                        {
                            "producer_entity_id": sensor_id,
                            "feature_of_interest_id": plot_id,
                            "payload_format": "canonical-json",
                            "idempotency_key": "tenant-a:batch-02",
                            "payload": {
                                "observations": [{
                                    "observed_property": "aion:SoilTemperature",
                                    "value": {"type": "number", "value": 25.4},
                                    "unit": "Cel"
                                }]
                            }
                        }
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body["total_items"], 2);
        assert_eq!(body["accepted_count"], 2);
        assert_eq!(body["duplicate_count"], 0);
        assert_eq!(body["failed_count"], 0);
        assert_eq!(body["observations_created"], 2);
        assert_eq!(body["stopped_early"], false);
        assert!(body["event_id"].as_str().is_some());

        let results = body["results"].as_array().unwrap();
        let first_raw_id = results[0]["raw_message_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let second_raw_id = results[1]["raw_message_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let first_raw =
            aion_storage::RawMessageStore::get_raw_message(&*storage, Uuid::nil(), first_raw_id)
                .unwrap()
                .unwrap();
        let second_raw =
            aion_storage::RawMessageStore::get_raw_message(&*storage, Uuid::nil(), second_raw_id)
                .unwrap()
                .unwrap();
        assert_eq!(first_raw.headers["external.batch_id"], "batch-01");
        assert_eq!(first_raw.headers["external.source_system"], "smartsentinel");
        assert_eq!(first_raw.headers["external.sync_session_id"], "sync-01");
        assert_eq!(
            first_raw.headers["external.metadata"]["transport"],
            "store-and-forward"
        );
        assert_eq!(second_raw.headers["external.batch_id"], "batch-01");

        let observations = get_json(
            &app,
            &format!("/observations?feature_of_interest_id={plot_id}"),
        )
        .await;
        assert_eq!(observations.as_array().unwrap().len(), 2);

        let batch_events = get_json(&app, "/events?event_type=aion:ReliableBatchIngested").await;
        let batch_event = batch_events.as_array().unwrap().last().unwrap();
        assert_eq!(batch_event["metadata"]["batch_id"], "batch-01");
        assert_eq!(batch_event["metadata"]["accepted_count"], 2);
        assert_eq!(batch_event["metadata"]["observations_created"], 2);
    }

    #[tokio::test]
    async fn batch_ingest_deduplicates_across_requests_and_within_batch() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor_id = create_test_entity(&app, "batch-dedupe-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "batch-dedupe-plot-01", "aion:Plot").await;

        let first_batch = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/ingest/batch",
                    json!({
                        "batch_id": "batch-dedupe-01",
                        "items": [
                            {
                                "producer_entity_id": sensor_id,
                                "feature_of_interest_id": plot_id,
                                "payload_format": "canonical-json",
                                "idempotency_key": "tenant-a:batch-dedupe-01",
                                "payload": {
                                    "observations": [{
                                        "observed_property": "aion:SoilMoisture",
                                        "value": {"type": "number", "value": 20.1}
                                    }]
                                }
                            },
                            {
                                "producer_entity_id": sensor_id,
                                "feature_of_interest_id": plot_id,
                                "payload_format": "canonical-json",
                                "idempotency_key": "tenant-a:batch-dedupe-01",
                                "payload": {
                                    "observations": [{
                                        "observed_property": "aion:SoilMoisture",
                                        "value": {"type": "number", "value": 20.1}
                                    }]
                                }
                            }
                        ]
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(first_batch["accepted_count"], 1);
        assert_eq!(first_batch["duplicate_count"], 1);
        assert_eq!(first_batch["results"][1]["duplicate"], true);

        let second_batch = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/ingest/batch",
                    json!({
                        "batch_id": "batch-dedupe-02",
                        "items": [{
                            "producer_entity_id": sensor_id,
                            "feature_of_interest_id": plot_id,
                            "payload_format": "canonical-json",
                            "idempotency_key": "tenant-a:batch-dedupe-01",
                            "payload": {
                                "observations": [{
                                    "observed_property": "aion:SoilMoisture",
                                    "value": {"type": "number", "value": 20.1}
                                }]
                            }
                        }]
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(second_batch["accepted_count"], 0);
        assert_eq!(second_batch["duplicate_count"], 1);
        assert_eq!(second_batch["results"][0]["duplicate"], true);

        let observations = get_json(
            &app,
            &format!("/observations?feature_of_interest_id={plot_id}"),
        )
        .await;
        assert_eq!(observations.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn batch_ingest_tenant_scoped_idempotency_and_missing_key_behavior_are_preserved() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let token_app = token_mode_app_with_storage(storage.clone());
        let sensor_a =
            create_test_entity(&tenant_a_app, "batch-tenant-a-sensor", "aion:Sensor").await;
        let plot_a = create_test_entity(&tenant_a_app, "batch-tenant-a-plot", "aion:Plot").await;
        let sensor_b =
            create_test_entity(&tenant_b_app, "batch-tenant-b-sensor", "aion:Sensor").await;
        let plot_b = create_test_entity(&tenant_b_app, "batch-tenant-b-plot", "aion:Plot").await;
        let token_a = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("batch-tenant-a"),
            &["batches:write"],
        );
        let token_b = store_api_token_for_tenant(
            &storage,
            tenant_b,
            ApiTokenPrincipalType::Service,
            Some("batch-tenant-b"),
            &["batches:write"],
        );

        let create_a = to_json(
            token_app
                .clone()
                .oneshot(auth_json_request(
                    "POST",
                    "/ingest/batch",
                    json!({
                        "items": [{
                            "producer_entity_id": sensor_a,
                            "feature_of_interest_id": plot_a,
                            "payload_format": "canonical-json",
                            "idempotency_key": "shared-batch-key-01",
                            "payload": {
                                "observations": [{
                                    "observed_property": "aion:SoilMoisture",
                                    "value": {"type": "number", "value": 10.0}
                                }]
                            }
                        }]
                    }),
                    &token_a,
                ))
                .await
                .unwrap(),
        )
        .await;
        let create_b = to_json(
            token_app
                .clone()
                .oneshot(auth_json_request(
                    "POST",
                    "/ingest/batch",
                    json!({
                        "items": [{
                            "producer_entity_id": sensor_b,
                            "feature_of_interest_id": plot_b,
                            "payload_format": "canonical-json",
                            "idempotency_key": "shared-batch-key-01",
                            "payload": {
                                "observations": [{
                                    "observed_property": "aion:SoilMoisture",
                                    "value": {"type": "number", "value": 30.0}
                                }]
                            }
                        }]
                    }),
                    &token_b,
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(create_a["duplicate_count"], 0);
        assert_eq!(create_b["duplicate_count"], 0);
        assert_ne!(
            create_a["results"][0]["raw_message_id"],
            create_b["results"][0]["raw_message_id"]
        );

        let no_key_batch = to_json(
            tenant_a_app
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/ingest/batch",
                    json!({
                        "items": [
                            {
                                "producer_entity_id": sensor_a,
                                "feature_of_interest_id": plot_a,
                                "payload_format": "canonical-json",
                                "payload": {
                                    "observations": [{
                                        "observed_property": "aion:SoilTemperature",
                                        "value": {"type": "number", "value": 21.0}
                                    }]
                                }
                            },
                            {
                                "producer_entity_id": sensor_a,
                                "feature_of_interest_id": plot_a,
                                "payload_format": "canonical-json",
                                "payload": {
                                    "observations": [{
                                        "observed_property": "aion:SoilTemperature",
                                        "value": {"type": "number", "value": 22.0}
                                    }]
                                }
                            }
                        ]
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(no_key_batch["accepted_count"], 2);
        assert_eq!(no_key_batch["duplicate_count"], 0);
    }

    #[tokio::test]
    async fn batch_ingest_validates_limits_and_continue_on_error_modes() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor_id = create_test_entity(&app, "batch-limit-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "batch-limit-plot-01", "aion:Plot").await;

        let empty = app
            .clone()
            .oneshot(json_request("POST", "/ingest/batch", json!({"items": []})))
            .await
            .unwrap();
        assert_eq!(empty.status(), StatusCode::BAD_REQUEST);

        let items = (0..1_001)
            .map(|index| {
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "canonical-json",
                    "idempotency_key": format!("tenant-a:overflow-{index}"),
                    "payload": {
                        "observations": [{
                            "observed_property": "aion:SoilMoisture",
                            "value": {"type": "number", "value": index}
                        }]
                    }
                })
            })
            .collect::<Vec<_>>();
        let overflow = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/batch",
                json!({"items": items}),
            ))
            .await
            .unwrap();
        assert_eq!(overflow.status(), StatusCode::BAD_REQUEST);

        let continue_true = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/ingest/batch",
                    json!({
                        "continue_on_error": true,
                        "items": [
                            {
                                "producer_entity_id": sensor_id,
                                "feature_of_interest_id": plot_id,
                                "payload_format": "senml-json",
                                "idempotency_key": "tenant-a:continue-fail",
                                "payload": "not json"
                            },
                            {
                                "producer_entity_id": sensor_id,
                                "feature_of_interest_id": plot_id,
                                "payload_format": "canonical-json",
                                "idempotency_key": "tenant-a:continue-ok",
                                "payload": {
                                    "observations": [{
                                        "observed_property": "aion:SoilMoisture",
                                        "value": {"type": "number", "value": 55.0}
                                    }]
                                }
                            }
                        ]
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(continue_true["accepted_count"], 1);
        assert_eq!(continue_true["failed_count"], 1);
        assert_eq!(continue_true["stopped_early"], false);
        assert_eq!(continue_true["results"][0]["status"], "failed");
        assert_eq!(continue_true["results"][1]["status"], "accepted");

        let continue_false = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/ingest/batch",
                    json!({
                        "continue_on_error": false,
                        "items": [
                            {
                                "producer_entity_id": sensor_id,
                                "feature_of_interest_id": plot_id,
                                "payload_format": "senml-json",
                                "idempotency_key": "tenant-a:stop-fail",
                                "payload": "not json"
                            },
                            {
                                "producer_entity_id": sensor_id,
                                "feature_of_interest_id": plot_id,
                                "payload_format": "canonical-json",
                                "idempotency_key": "tenant-a:stop-ok",
                                "payload": {
                                    "observations": [{
                                        "observed_property": "aion:SoilMoisture",
                                        "value": {"type": "number", "value": 99.0}
                                    }]
                                }
                            }
                        ]
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(continue_false["accepted_count"], 0);
        assert_eq!(continue_false["failed_count"], 1);
        assert_eq!(continue_false["stopped_early"], true);
        assert_eq!(continue_false["results"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn batch_ingest_inherits_and_overrides_batch_level_provenance() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&app, "batch-prov-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "batch-prov-plot-01", "aion:Plot").await;

        let body = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/ingest/batch",
                    json!({
                        "batch_id": "batch-prov-01",
                        "sync_session_id": "sync-prov-01",
                        "source_system": "minifi",
                        "source_id": "edge-default",
                        "connectivity_state": "reconnected_backfill",
                        "external_flow_id": "flow-default",
                        "external_flow_name": "Default Flow",
                        "metadata": {
                            "shared": "batch",
                            "batch_only": true
                        },
                        "items": [
                            {
                                "producer_entity_id": sensor_id,
                                "feature_of_interest_id": plot_id,
                                "payload_format": "canonical-json",
                                "idempotency_key": "tenant-a:prov-01",
                                "payload": {
                                    "observations": [{
                                        "observed_property": "aion:SoilMoisture",
                                        "value": {"type": "number", "value": 1.0}
                                    }]
                                }
                            },
                            {
                                "producer_entity_id": sensor_id,
                                "feature_of_interest_id": plot_id,
                                "payload_format": "canonical-json",
                                "idempotency_key": "tenant-a:prov-02",
                                "source_system": "smartsentinel",
                                "source_id": "edge-override",
                                "connectivity_state": "replayed_after_outage",
                                "external_flow_id": "flow-override",
                                "external_flow_name": "Override Flow",
                                "metadata": {
                                    "shared": "item",
                                    "item_only": true
                                },
                                "payload": {
                                    "observations": [{
                                        "observed_property": "aion:SoilTemperature",
                                        "value": {"type": "number", "value": 2.0}
                                    }]
                                }
                            }
                        ]
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;

        let first_raw_id = body["results"][0]["raw_message_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let second_raw_id = body["results"][1]["raw_message_id"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let first_raw =
            aion_storage::RawMessageStore::get_raw_message(&*storage, Uuid::nil(), first_raw_id)
                .unwrap()
                .unwrap();
        let second_raw =
            aion_storage::RawMessageStore::get_raw_message(&*storage, Uuid::nil(), second_raw_id)
                .unwrap()
                .unwrap();

        assert_eq!(first_raw.headers["external.source_system"], "minifi");
        assert_eq!(first_raw.headers["external.source_id"], "edge-default");
        assert_eq!(
            first_raw.headers["external.connectivity_state"],
            "reconnected_backfill"
        );
        assert_eq!(first_raw.headers["external.flow_id"], "flow-default");
        assert_eq!(first_raw.headers["external.flow_name"], "Default Flow");
        assert_eq!(first_raw.headers["external.metadata"]["shared"], "batch");
        assert_eq!(first_raw.headers["external.metadata"]["batch_only"], true);

        assert_eq!(
            second_raw.headers["external.source_system"],
            "smartsentinel"
        );
        assert_eq!(second_raw.headers["external.source_id"], "edge-override");
        assert_eq!(
            second_raw.headers["external.connectivity_state"],
            "replayed_after_outage"
        );
        assert_eq!(second_raw.headers["external.flow_id"], "flow-override");
        assert_eq!(second_raw.headers["external.flow_name"], "Override Flow");
        assert_eq!(second_raw.headers["external.metadata"]["shared"], "item");
        assert_eq!(second_raw.headers["external.metadata"]["batch_only"], true);
        assert_eq!(second_raw.headers["external.metadata"]["item_only"], true);
    }

    #[tokio::test]
    async fn batch_ingest_auth_respects_batch_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&dev_app, "batch-auth-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "batch-auth-plot-01", "aion:Plot").await;
        let body = json!({
            "items": [{
                "producer_entity_id": sensor_id,
                "feature_of_interest_id": plot_id,
                "payload_format": "canonical-json",
                "idempotency_key": "tenant-a:batch-auth-01",
                "payload": {
                    "observations": [{
                        "observed_property": "aion:SoilMoisture",
                        "value": {"type": "number", "value": 11.2}
                    }]
                }
            }]
        });

        let missing_token = token_app
            .clone()
            .oneshot(json_request("POST", "/ingest/batch", body.clone()))
            .await
            .unwrap();
        assert_eq!(missing_token.status(), StatusCode::UNAUTHORIZED);

        let wrong_scope = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/ingest/batch",
                body.clone(),
                &store_api_token(
                    &storage,
                    ApiTokenPrincipalType::Service,
                    Some("batch-ingest-reader"),
                    &["ingestion:write"],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(wrong_scope.status(), StatusCode::FORBIDDEN);

        let allowed = token_app
            .oneshot(auth_json_request(
                "POST",
                "/ingest/batch",
                body,
                &store_api_token(
                    &storage,
                    ApiTokenPrincipalType::Service,
                    Some("batch-ingest-writer"),
                    &["batches:write"],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ttn_live_validation_requires_connectors_admin() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&dev_app, "ttn-live-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "ttn-live-plot-01", "aion:Plot").await;
        let connector =
            create_ttn_connector(&dev_app, "ttn-live-auth-01", &sensor_id, &plot_id).await;
        let connector_id = connector["id"].as_str().unwrap();

        let denied = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ttn-live-validate"),
                json!({}),
                &store_api_token(
                    &storage,
                    ApiTokenPrincipalType::Service,
                    Some("ttn-live-reader"),
                    &["connectors:read"],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("ttn-live-admin"),
            &["connectors:admin"],
        );
        let allowed = token_app
            .oneshot(auth_json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ttn-live-validate"),
                json!({}),
                &raw_token,
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ttn_mapping_routes_require_connector_scopes() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&dev_app, "ttn-auth-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "ttn-auth-plot-01", "aion:Plot").await;
        let connector =
            create_ttn_connector(&dev_app, "ttn-auth-connector-01", &sensor_id, &plot_id).await;
        let connector_id = connector["id"].as_str().unwrap();
        let create_body = json!({
            "ttn_application_id": "farm-app",
            "ttn_device_id": "soil-node-01",
            "producer_entity_id": sensor_id,
            "feature_of_interest_id": plot_id
        });

        let missing_token = token_app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings"),
                create_body.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(missing_token.status(), StatusCode::UNAUTHORIZED);

        let read_only_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("ttn-mapping-reader"),
            &["connectors:read"],
        );
        let create_denied = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings"),
                create_body.clone(),
                &read_only_token,
            ))
            .await
            .unwrap();
        assert_eq!(create_denied.status(), StatusCode::FORBIDDEN);

        let admin_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("ttn-mapping-admin"),
            &["connectors:admin"],
        );
        let created = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings"),
                create_body,
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created = to_json(created).await;
        let mapping_id = created["id"].as_str().unwrap();

        let listed = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings"),
                &read_only_token,
            ))
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);

        let fetched = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}"),
                &read_only_token,
            ))
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);

        let patch_denied = token_app
            .clone()
            .oneshot(auth_json_request(
                "PATCH",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}"),
                json!({"enabled": false}),
                &read_only_token,
            ))
            .await
            .unwrap();
        assert_eq!(patch_denied.status(), StatusCode::FORBIDDEN);

        let delete_denied = token_app
            .clone()
            .oneshot(auth_request(
                "DELETE",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}"),
                &read_only_token,
            ))
            .await
            .unwrap();
        assert_eq!(delete_denied.status(), StatusCode::FORBIDDEN);

        let admin_all_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Admin,
            Some("ttn-mapping-break-glass"),
            &["admin:all"],
        );
        let disabled = token_app
            .clone()
            .oneshot(auth_request(
                "PUT",
                &format!(
                    "/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}/disable"
                ),
                &admin_all_token,
            ))
            .await
            .unwrap();
        assert_eq!(disabled.status(), StatusCode::OK);

        let deleted = token_app
            .oneshot(auth_request(
                "DELETE",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}"),
                &admin_all_token,
            ))
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn smartsentinel_snapshot_ingestion_requires_smartsentinel_ingest() {
        let storage = Arc::new(InMemoryStorage::new());
        let token_app = token_mode_app_with_storage(storage.clone());

        let denied = token_app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_sample_snapshot(),
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("smartsentinel-ingest"),
            &["smartsentinel:ingest"],
        );
        let allowed = token_app
            .oneshot(auth_json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_sample_snapshot(),
                &raw_token,
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn adapter_status_reads_require_adapters_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let adapter = dev_app
            .clone()
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": "adapter-read-01",
                    "display_name": "Adapter Read 01",
                    "adapter_type": "edge",
                    "status": "online"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(adapter.status(), StatusCode::CREATED);
        let adapter = to_json(adapter).await;
        let adapter_id = adapter["adapter"]["id"].as_str().unwrap();

        let missing_token = token_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/adapters/{adapter_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_token.status(), StatusCode::UNAUTHORIZED);

        let denied = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/adapters/{adapter_id}/status"),
                &store_api_token(
                    &storage,
                    ApiTokenPrincipalType::Service,
                    Some("adapter-heartbeat-only"),
                    &["adapters:heartbeat"],
                ),
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("adapter-reader"),
            &["adapters:read"],
        );
        let listed = token_app
            .clone()
            .oneshot(auth_request("GET", "/adapters", &raw_token))
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);

        let detail = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/adapters/{adapter_id}"),
                &raw_token,
            ))
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);

        let status = token_app
            .oneshot(auth_request(
                "GET",
                &format!("/adapters/{adapter_id}/status"),
                &raw_token,
            ))
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn secrets_endpoints_require_secrets_admin_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = token_mode_app_with_storage(storage.clone());
        let read_only_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("reader"),
            &["entities:read"],
        );
        let admin_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Admin,
            Some("secret-admin"),
            &["secrets:admin"],
        );

        let denied = app
            .clone()
            .oneshot(auth_request("GET", "/secrets/connectors", &read_only_token))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let created = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/secrets/connectors",
                json!({
                    "secret_key": "protected-secret",
                    "secret_type": "mqtt_basic_auth",
                    "username": "mqtt-user",
                    "secret_value": "secret-pass"
                }),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created = to_json(created).await;
        let secret_id = created["id"].as_str().unwrap();

        let listed = app
            .clone()
            .oneshot(auth_request("GET", "/secrets/connectors", &admin_token))
            .await
            .unwrap();
        assert_eq!(listed.status(), StatusCode::OK);

        let fetched = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/secrets/connectors/{secret_id}"),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);

        let deleted = app
            .oneshot(auth_request(
                "DELETE",
                &format!("/secrets/connectors/{secret_id}"),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn executor_polling_requires_executors_poll_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&dev_app, "token-executor-pump-01", "aion:Pump").await;
        let command = create_test_command(&dev_app, &pump_id, "StartPump").await;
        let executor = create_test_executor(&dev_app, "token-executor-01").await;
        let executor_id = executor["id"].as_str().unwrap();
        put_executor_capabilities(&dev_app, executor_id, &["StartPump"]).await;
        put_executor_scope_for_target(&dev_app, executor_id, &pump_id).await;
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Executor,
            Some("token-executor-01"),
            &["executors:heartbeat"],
        );

        let denied = token_app
            .oneshot(auth_request(
                "GET",
                &format!("/executors/{executor_id}/commands/pending"),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let body = to_json(denied).await;
        assert!(body["error"].as_str().unwrap().contains("executors:poll"));
        assert_eq!(command["command_type"], "StartPump");
    }

    #[tokio::test]
    async fn smartsentinel_report_requires_executor_report_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        let service_id = smartsentinel_service_entity(&dev_app).await;
        let executor = register_smartsentinel_executor(
            &dev_app,
            "smartsentinel-token-executor-01",
            &service_id,
            &["sentinel:RunDiagnostic"],
        )
        .await;
        let executor_id = executor["executor"]["id"].as_str().unwrap();
        let command = create_test_command(&dev_app, &service_id, "sentinel:RunDiagnostic").await;
        let command_id = command["id"].as_str().unwrap();
        let _ = claim_smartsentinel_command(&dev_app, executor_id, command_id).await;
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Executor,
            Some("smartsentinel-token-executor-01"),
            &["smartsentinel:executor_poll"],
        );

        let denied = token_app
            .oneshot(auth_json_request(
                "POST",
                &format!(
                    "/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/report"
                ),
                json!({
                    "action_type": "sentinel:RunDiagnostic",
                    "status": "executed",
                    "verified": true,
                    "result_payload": {"dry_run": true}
                }),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let body = to_json(denied).await;
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("smartsentinel:executor_report"));
    }

    #[tokio::test]
    async fn token_management_requires_scope_or_bootstrap_admin_token() {
        let storage = Arc::new(InMemoryStorage::new());
        let scoped_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("token-admin"),
            &["auth:tokens:admin"],
        );
        let non_admin_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("token-user"),
            &["entities:read"],
        );
        let app = token_mode_app_with_storage(storage.clone());

        let forbidden = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/auth/tokens",
                json!({
                    "token_name": "forbidden",
                    "principal_type": "service",
                    "scopes": []
                }),
                &non_admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        let allowed = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/auth/tokens",
                json!({
                    "token_name": "allowed",
                    "principal_type": "service",
                    "scopes": ["adapters:register"]
                }),
                &scoped_token,
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::CREATED);

        let bootstrap_token = "bootstrap-admin-secret";
        let bootstrap_app = token_mode_app_with_bootstrap(storage, bootstrap_token);
        let bootstrap_allowed = bootstrap_app
            .oneshot(auth_json_request(
                "POST",
                "/auth/tokens",
                json!({
                    "token_name": "bootstrap-created",
                    "principal_type": "service",
                    "scopes": ["adapters:heartbeat"]
                }),
                bootstrap_token,
            ))
            .await
            .unwrap();
        assert_eq!(bootstrap_allowed.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn dev_mode_allows_mcp_tools_without_token() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/mcp/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_rejects_mcp_tools_without_token() {
        let app = token_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/mcp/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn token_mode_rejects_mcp_tools_without_required_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("mcp-reader-without-scope"),
            &["entities:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request("GET", "/mcp/tools", &raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn token_mode_allows_mcp_tools_with_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("mcp-reader"),
            &["mcp:tools"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request("GET", "/mcp/tools", &raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_mcp_tool_invocation_with_mcp_tools_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("mcp-tool-invoker-without-scope"),
            &["entities:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_json_request(
                "POST",
                "/mcp/tools/build_ai_context",
                json!({
                    "arguments": {
                        "entity_id": Uuid::nil(),
                        "include_observations": false,
                        "include_events": false,
                        "include_commands": false
                    }
                }),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn token_mode_protects_mcp_json_rpc_tools_list_with_mcp_tools_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let missing_scope_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("json-rpc-without-scope"),
            &["entities:read"],
        );
        let allowed_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("json-rpc-with-scope"),
            &["mcp:tools"],
        );
        let app = token_mode_app_with_storage(storage);

        let denied = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/list",
                    "params": {}
                }),
                &missing_scope_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let allowed = app
            .oneshot(auth_json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/list",
                    "params": {}
                }),
                &allowed_token,
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_ai_context_with_ai_context_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let entity_id = create_test_entity(&dev_app, "secured-ai-context-01", "aion:Pump").await;
        let missing_scope_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("ai-context-without-scope"),
            &["entities:read"],
        );
        let allowed_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("ai-context-reader"),
            &["ai:context:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let denied = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/ai/context/entity/{entity_id}"),
                &missing_scope_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let allowed = app
            .oneshot(auth_request(
                "GET",
                &format!("/ai/context/entity/{entity_id}"),
                &allowed_token,
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_provenance_search_with_provenance_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let missing_scope_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("provenance-without-scope"),
            &["events:read"],
        );
        let allowed_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("provenance-reader"),
            &["provenance:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let denied = app
            .clone()
            .oneshot(auth_request(
                "GET",
                "/provenance/search?trace_id=trace-abc",
                &missing_scope_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let allowed = app
            .oneshot(auth_request(
                "GET",
                "/provenance/search?trace_id=trace-abc",
                &allowed_token,
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dev_mode_allows_events_without_token() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&app, "dev-events-pump-01", "aion:Pump").await;
        create_test_event(&app, "aion:DevEvent", Some(&pump_id), json!({})).await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_rejects_events_without_token() {
        let app = token_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn token_mode_rejects_events_without_required_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("events-without-scope"),
            &["entities:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request("GET", "/events", &raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn token_mode_allows_events_with_events_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&dev_app, "events-read-pump-01", "aion:Pump").await;
        create_test_event(&dev_app, "aion:ScopedEvent", Some(&pump_id), json!({})).await;
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("events-reader"),
            &["events:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request("GET", "/events", &raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_allows_event_detail_with_events_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&dev_app, "events-detail-pump-01", "aion:Pump").await;
        let event = create_test_event(
            &dev_app,
            "aion:ScopedEventDetail",
            Some(&pump_id),
            json!({}),
        )
        .await;
        let event_id = event["id"].as_str().unwrap();
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("event-detail-reader"),
            &["events:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request(
                "GET",
                &format!("/events/{event_id}"),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_rejects_raw_messages_without_token() {
        let app = token_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/raw-messages")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn token_mode_rejects_raw_messages_without_required_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("raw-messages-without-scope"),
            &["events:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request("GET", "/raw-messages", &raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn token_mode_allows_raw_messages_with_raw_messages_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&dev_app, "raw-read-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "raw-read-plot-01", "aion:Plot").await;
        ingest_test_senml(&dev_app, &sensor_id, &plot_id).await;
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("raw-messages-reader"),
            &["raw-messages:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request("GET", "/raw-messages", &raw_token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_allows_raw_message_detail_with_raw_messages_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&dev_app, "raw-detail-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "raw-detail-plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&dev_app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();
        let raw_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("raw-message-detail-reader"),
            &["raw-messages:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request(
                "GET",
                &format!("/raw-messages/{raw_message_id}"),
                &raw_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_all_satisfies_events_and_raw_messages_scope_checks() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&dev_app, "admin-events-pump-01", "aion:Pump").await;
        create_test_event(&dev_app, "aion:AdminScopedEvent", Some(&pump_id), json!({})).await;
        let sensor_id = create_test_entity(&dev_app, "admin-raw-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "admin-raw-plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&dev_app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();
        let admin_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Admin,
            Some("admin-all-events-raw"),
            &["admin:all"],
        );
        let app = token_mode_app_with_storage(storage);

        let events = app
            .clone()
            .oneshot(auth_request("GET", "/events", &admin_token))
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);

        let raw_messages = app
            .oneshot(auth_request(
                "GET",
                &format!("/raw-messages/{raw_message_id}"),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(raw_messages.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_all_satisfies_mcp_ai_context_and_provenance_scope_checks() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let entity_id = create_test_entity(&dev_app, "admin-all-context-01", "aion:Tank").await;
        let admin_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Admin,
            Some("admin-all"),
            &["admin:all"],
        );
        let app = token_mode_app_with_storage(storage);

        let mcp = app
            .clone()
            .oneshot(auth_request("GET", "/mcp/tools", &admin_token))
            .await
            .unwrap();
        assert_eq!(mcp.status(), StatusCode::OK);

        let ai_context = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/ai/context/entity/{entity_id}"),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(ai_context.status(), StatusCode::OK);

        let provenance = app
            .oneshot(auth_request(
                "GET",
                "/provenance/search?trace_id=trace-admin",
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(provenance.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn selected_unprotected_endpoints_still_work_in_token_mode_without_token() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = token_mode_app_with_storage(storage);

        let whoami = app
            .oneshot(
                Request::builder()
                    .uri("/auth/whoami")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(whoami.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_entity_writes_and_assigns_principal_tenant() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = token_mode_app_with_storage(storage.clone());
        let missing_scope_token = store_api_token_for_tenant(
            &storage,
            Uuid::new_v4(),
            ApiTokenPrincipalType::Service,
            Some("entity-write-missing"),
            &["entities:read"],
        );
        let tenant_id = Uuid::new_v4();
        let write_token = store_api_token_for_tenant(
            &storage,
            tenant_id,
            ApiTokenPrincipalType::Service,
            Some("entity-writer"),
            &["entities:write"],
        );
        let body = json!({
            "entity_key": "tenant-write-entity-01",
            "entity_type": "aion:Sensor",
            "jsonld": {
                "@context": {"aion": "https://aioncore.org/ns#"},
                "@id": "urn:aion:test:tenant-write-entity-01",
                "@type": "aion:Sensor"
            }
        });

        let forbidden = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/entities",
                body.clone(),
                &missing_scope_token,
            ))
            .await
            .unwrap();
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

        let created = to_json(
            app.clone()
                .oneshot(auth_json_request("POST", "/entities", body, &write_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(created["tenant_id"], tenant_id.to_string());
    }

    #[tokio::test]
    async fn token_mode_enforces_relationship_and_observation_write_tenants() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let entity_a =
            create_test_entity(&tenant_a_app, "tenant-a-rel-source", "aion:Sensor").await;
        let entity_a_target =
            create_test_entity(&tenant_a_app, "tenant-a-rel-target", "aion:Plot").await;
        let entity_b = create_test_entity(&tenant_b_app, "tenant-b-rel-target", "aion:Plot").await;
        let tenant_a_relationship_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-relationship-writer"),
            &["relationships:write", "observations:write"],
        );
        let admin_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Admin,
            Some("write-admin"),
            &["admin:all"],
        );

        let denied_relationship = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/relationships",
                json!({
                    "source_entity_id": entity_a,
                    "relationship_type": "aion:connectedTo",
                    "target_entity_id": entity_b,
                    "jsonld": {"@type": "aion:Relationship"}
                }),
                &tenant_a_relationship_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied_relationship.status(), StatusCode::FORBIDDEN);

        let admin_relationship = to_json(
            app.clone()
                .oneshot(auth_json_request(
                    "POST",
                    "/relationships",
                    json!({
                        "source_entity_id": entity_a,
                        "relationship_type": "aion:connectedTo",
                        "target_entity_id": entity_b,
                        "jsonld": {"@type": "aion:Relationship"}
                    }),
                    &admin_token,
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(admin_relationship["tenant_id"], tenant_a.to_string());

        let denied_observation = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/observations",
                json!({
                    "producer_entity_id": entity_a,
                    "feature_of_interest_id": entity_b,
                    "observed_property": "temperature",
                    "value": {"type": "number", "value": 21.5},
                    "unit": "Cel",
                    "observed_at": "2026-05-05T12:00:00Z",
                    "received_at": "2026-05-05T12:00:05Z",
                    "protocol": "http",
                    "payload_format": "json_mapping",
                    "quality": {},
                    "metadata": {}
                }),
                &tenant_a_relationship_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied_observation.status(), StatusCode::FORBIDDEN);

        let same_tenant_observation = app
            .oneshot(auth_json_request(
                "POST",
                "/observations",
                json!({
                    "producer_entity_id": entity_a,
                    "feature_of_interest_id": entity_a_target,
                    "observed_property": "temperature",
                    "value": {"type": "number", "value": 21.5},
                    "unit": "Cel",
                    "observed_at": "2026-05-05T12:00:00Z",
                    "received_at": "2026-05-05T12:00:05Z",
                    "protocol": "http",
                    "payload_format": "json_mapping",
                    "quality": {},
                    "metadata": {}
                }),
                &tenant_a_relationship_token,
            ))
            .await
            .unwrap();
        assert_eq!(same_tenant_observation.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn token_mode_enforces_command_action_and_lifecycle_write_tenants() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let pump_a = create_test_entity(&tenant_a_app, "tenant-a-command-pump", "aion:Pump").await;
        let pump_b = create_test_entity(&tenant_b_app, "tenant-b-command-pump", "aion:Pump").await;
        let tenant_a_command_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-command-writer"),
            &["commands:create", "commands:write", "actions:write"],
        );
        let tenant_b_command_token = store_api_token_for_tenant(
            &storage,
            tenant_b,
            ApiTokenPrincipalType::Service,
            Some("tenant-b-command-writer"),
            &["commands:create", "commands:write", "actions:write"],
        );
        let missing_create_scope = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-command-missing-create"),
            &["commands:read"],
        );

        let missing_scope = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/commands",
                json!({
                    "target_entity_id": pump_a,
                    "command_type": "StartPump",
                    "payload": {"target_state": "running"}
                }),
                &missing_create_scope,
            ))
            .await
            .unwrap();
        assert_eq!(missing_scope.status(), StatusCode::FORBIDDEN);

        let cross_tenant_create = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/commands",
                json!({
                    "target_entity_id": pump_a,
                    "command_type": "StartPump",
                    "payload": {"target_state": "running"}
                }),
                &tenant_b_command_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_create.status(), StatusCode::FORBIDDEN);

        let command = to_json(
            app.clone()
                .oneshot(auth_json_request(
                    "POST",
                    "/commands",
                    json!({
                        "target_entity_id": pump_a,
                        "command_type": "StartPump",
                        "payload": {"target_state": "running"}
                    }),
                    &tenant_a_command_token,
                ))
                .await
                .unwrap(),
        )
        .await;
        let command_id = command["id"].as_str().unwrap();

        let denied_lifecycle = app
            .clone()
            .oneshot(auth_request(
                "POST",
                &format!("/commands/{command_id}/cancel"),
                &tenant_b_command_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied_lifecycle.status(), StatusCode::FORBIDDEN);

        let action = to_json(
            app.clone()
                .oneshot(auth_json_request(
                    "POST",
                    "/actions",
                    json!({
                        "command_id": command_id,
                        "action_type": "StartPump",
                        "status": "completed",
                        "started_at": "2026-05-05T12:00:00Z",
                        "finished_at": "2026-05-05T12:01:00Z"
                    }),
                    &tenant_a_command_token,
                ))
                .await
                .unwrap(),
        )
        .await;
        let action_id = action["id"].as_str().unwrap();

        let denied_action = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/actions",
                json!({
                    "command_id": command_id,
                    "action_type": "StartPump",
                    "status": "completed",
                    "started_at": "2026-05-05T12:00:00Z",
                    "finished_at": "2026-05-05T12:01:00Z",
                    "executor_entity_id": pump_b
                }),
                &tenant_b_command_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied_action.status(), StatusCode::FORBIDDEN);

        let denied_result = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/action-results",
                json!({
                    "command_id": command_id,
                    "action_id": action_id,
                    "status": "succeeded",
                    "verified": true,
                    "result_payload": {"ok": true},
                    "observed_at": "2026-05-05T12:01:30Z"
                }),
                &tenant_b_command_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied_result.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn token_mode_enforces_rule_policy_capability_and_executor_write_scopes() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let tank_a =
            create_test_entity(&tenant_a_app, "tenant-a-rule-tank-write", "aion:Tank").await;
        let pump_a =
            create_test_entity(&tenant_a_app, "tenant-a-rule-pump-write", "aion:Pump").await;
        let pump_b =
            create_test_entity(&tenant_b_app, "tenant-b-rule-pump-write", "aion:Pump").await;
        let executor_b = create_test_executor(&tenant_b_app, "tenant-b-executor-write").await;
        let executor_b_id = executor_b["id"].as_str().unwrap();

        let rule_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-rule-writer"),
            &[
                "rules:write",
                "policies:write",
                "capabilities:write",
                "executors:write",
            ],
        );
        let missing_scope_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-rule-missing"),
            &["rules:read"],
        );

        let missing_rule_scope = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/rules",
                json!({
                    "name": "low-water",
                    "trigger_type": "observation_created",
                    "target_entity_id": tank_a,
                    "observed_property": "level_pct",
                    "condition": {"comparison": "less_than", "value": 20.0},
                    "action": {
                        "type": "create_command",
                        "target_entity_id": pump_a,
                        "command_type": "StartPump",
                        "payload": {"target_state": "running"}
                    }
                }),
                &missing_scope_token,
            ))
            .await
            .unwrap();
        assert_eq!(missing_rule_scope.status(), StatusCode::FORBIDDEN);

        let cross_tenant_rule = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/rules",
                json!({
                    "name": "cross-tenant-low-water",
                    "trigger_type": "observation_created",
                    "target_entity_id": tank_a,
                    "observed_property": "level_pct",
                    "condition": {"comparison": "less_than", "value": 20.0},
                    "action": {
                        "type": "create_command",
                        "target_entity_id": pump_b,
                        "command_type": "StartPump",
                        "payload": {"target_state": "running"}
                    }
                }),
                &rule_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_rule.status(), StatusCode::FORBIDDEN);

        let cross_tenant_policy = app
            .clone()
            .oneshot(auth_json_request(
                "PUT",
                "/policies",
                json!([{
                    "target_entity_id": pump_b,
                    "command_type": "StartPump",
                    "requires_approval": true,
                    "auto_execute_allowed": false
                }]),
                &rule_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_policy.status(), StatusCode::FORBIDDEN);

        let cross_tenant_capability = app
            .clone()
            .oneshot(auth_json_request(
                "PUT",
                &format!("/entities/{pump_b}/capabilities"),
                json!([{
                    "capability_name": "pump:start",
                    "command_type": "StartPump"
                }]),
                &rule_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_capability.status(), StatusCode::FORBIDDEN);

        let executor_scope_write = app
            .clone()
            .oneshot(auth_json_request(
                "PUT",
                &format!("/executors/{executor_b_id}/scopes"),
                json!([{
                    "entity_type": "aion:Pump"
                }]),
                &rule_token,
            ))
            .await
            .unwrap();
        assert_eq!(executor_scope_write.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_all_satisfies_new_write_scopes_and_tenant_checks() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let source_a = create_test_entity(&tenant_a_app, "admin-write-source-a", "aion:Pump").await;
        let target_b =
            create_test_entity(&tenant_b_app, "admin-write-target-b", "aion:Valve").await;
        let executor_b = create_test_executor(&tenant_b_app, "admin-write-executor-b").await;
        let executor_b_id = executor_b["id"].as_str().unwrap();
        let admin_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Admin,
            Some("platform-admin-write"),
            &["admin:all"],
        );

        let relationship = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/relationships",
                json!({
                    "source_entity_id": source_a,
                    "relationship_type": "aion:connectedTo",
                    "target_entity_id": target_b,
                    "jsonld": {"@type": "aion:Relationship"}
                }),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(relationship.status(), StatusCode::CREATED);

        let executor_caps = app
            .clone()
            .oneshot(auth_json_request(
                "PUT",
                &format!("/executors/{executor_b_id}/capabilities"),
                json!([{
                    "command_type": "StartPump",
                    "protocol": "http"
                }]),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(executor_caps.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dev_mode_allows_entities_read_without_token() {
        let app = app();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/entities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_rejects_entities_read_without_token() {
        let app = token_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/entities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn token_mode_rejects_entities_read_with_missing_scope_and_allows_entities_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        create_test_entity(&dev_app, "entities-read-sensor-01", "aion:Sensor").await;
        let missing_scope_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("entities-missing-scope"),
            &["observations:read"],
        );
        let entities_read_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("entities-reader"),
            &["entities:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let rejected = app
            .clone()
            .oneshot(auth_request("GET", "/entities", &missing_scope_token))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::FORBIDDEN);

        let allowed = app
            .oneshot(auth_request("GET", "/entities", &entities_read_token))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_entity_context_with_entities_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let entity_id = create_test_entity(&dev_app, "entity-context-auth-01", "aion:Tank").await;
        let token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("entity-context-reader"),
            &["entities:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request(
                "GET",
                &format!("/entities/{entity_id}/context"),
                &token,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_observations_read_with_observations_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let sensor_id = create_test_entity(&dev_app, "obs-auth-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "obs-auth-plot-01", "aion:Plot").await;
        ingest_test_senml(&dev_app, &sensor_id, &plot_id).await;
        let token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("observations-reader"),
            &["observations:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request(
                "GET",
                &format!("/observations?feature_of_interest_id={plot_id}"),
                &token,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dev_mode_allows_timeseries_query_without_token() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor_id = create_test_entity(&app, "timeseries-dev-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "timeseries-dev-plot-01", "aion:Plot").await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "soil.moisture",
            json!({"type": "number", "value": 12.3}),
            Some("%"),
            "2026-05-05T12:00:00Z",
        )
        .await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/timeseries/query?entity_id={plot_id}&observed_property=soil.moisture"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_rejects_timeseries_query_without_token() {
        let app = token_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/timeseries/query?entity_id=00000000-0000-0000-0000-000000000001&observed_property=temperature")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn token_mode_requires_timeseries_read_scope_and_allows_it() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let sensor_id =
            create_test_entity(&dev_app, "timeseries-auth-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&dev_app, "timeseries-auth-plot-01", "aion:Plot").await;
        create_test_observation_at(
            &dev_app,
            &sensor_id,
            &plot_id,
            "temperature",
            json!({"type": "number", "value": 21.4}),
            Some("Cel"),
            "2026-05-05T12:00:00Z",
        )
        .await;
        let missing_scope_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("timeseries-missing-scope"),
            &["observations:read"],
        );
        let timeseries_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("timeseries-reader"),
            &["timeseries:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let rejected = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/timeseries/query?entity_id={plot_id}&observed_property=temperature"),
                &missing_scope_token,
            ))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::FORBIDDEN);

        let allowed = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/timeseries/query?entity_id={plot_id}&observed_property=temperature"),
                &timeseries_token,
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);

        let properties = app
            .oneshot(auth_request(
                "GET",
                &format!("/timeseries/entities/{plot_id}/properties"),
                &timeseries_token,
            ))
            .await
            .unwrap();
        assert_eq!(properties.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dev_mode_dashboard_overview_works_without_token() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor_id = create_test_entity(&app, "dashboard-dev-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "dashboard-dev-plot-01", "aion:Plot").await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "temperature",
            json!({"type": "number", "value": 22.5}),
            Some("Cel"),
            "2026-05-05T12:00:00Z",
        )
        .await;
        create_http_connector(
            &app,
            "dashboard-dev-http-01",
            Some(&sensor_id),
            Some(&plot_id),
        )
        .await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body["entities_count"], 2);
        assert_eq!(body["observations_count"], 1);
        assert_eq!(body["connectors_count"], 1);
        assert_eq!(body["enabled_connectors_count"], 1);
    }

    #[tokio::test]
    async fn token_mode_dashboard_overview_without_token_returns_401() {
        let app = token_mode_app_with_storage(Arc::new(InMemoryStorage::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/dashboard/overview")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn token_mode_dashboard_overview_with_wrong_scope_returns_403() {
        let storage = Arc::new(InMemoryStorage::new());
        let token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("dashboard-wrong-scope"),
            &["timeseries:read"],
        );
        let app = token_mode_app_with_storage(storage);
        let response = app
            .oneshot(auth_request("GET", "/dashboard/overview", &token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn token_mode_dashboard_overview_with_dashboard_read_succeeds() {
        let storage = Arc::new(InMemoryStorage::new());
        let token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("dashboard-reader"),
            &["dashboard:read"],
        );
        let app = token_mode_app_with_storage(storage);
        let response = app
            .oneshot(auth_request("GET", "/dashboard/overview", &token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_all_satisfies_dashboard_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Admin,
            Some("dashboard-admin"),
            &["admin:all"],
        );
        let app = token_mode_app_with_storage(storage);
        let response = app
            .oneshot(auth_request("GET", "/dashboard/overview", &token))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn dashboard_timeseries_entities_lists_entities_with_observed_properties() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor = create_test_entity(&app, "dashboard-ts-sensor-01", "aion:Sensor").await;
        let named_plot = create_native_entity(
            &app,
            json!({
                "entity_key": "dashboard-ts-plot-01",
                "entity_type": "aion:Plot",
                "jsonld": {
                    "@context": {"aion": "https://aioncore.org/ns#"},
                    "@id": "urn:aion:test:dashboard-ts-plot-01",
                    "@type": "aion:Plot",
                    "aion:name": "North Plot"
                }
            }),
        )
        .await;
        let plot_id = named_plot["id"].as_str().unwrap().to_string();

        create_test_observation_at(
            &app,
            &sensor,
            &plot_id,
            "temperature",
            json!({"type": "number", "value": 21.0}),
            Some("Cel"),
            "2026-05-05T12:00:00Z",
        )
        .await;
        create_test_observation_at(
            &app,
            &sensor,
            &plot_id,
            "soil.moisture",
            json!({"type": "number", "value": 18.5}),
            Some("%"),
            "2026-05-05T12:05:00Z",
        )
        .await;

        let body = get_json(&app, "/dashboard/timeseries/entities").await;
        let entity = body["entities"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["entity_key"] == "dashboard-ts-plot-01")
            .unwrap();

        assert_eq!(entity["display_name"], "North Plot");
        assert_eq!(entity["observed_property_count"], 2);
        assert_eq!(entity["observation_count"], 2);
        assert_eq!(entity["last_observed_at"], "2026-05-05T12:05:00Z");
    }

    #[tokio::test]
    async fn dashboard_connector_overview_returns_status_without_secret_values() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let secret = create_connector_secret(
            &app,
            "dashboard-mqtt-secret",
            "farm-user",
            "super-secret-value",
        )
        .await;
        create_mqtt_connector_with_secret(
            &app,
            "dashboard-mqtt-01",
            true,
            Some("mqtt://farm-user:super-secret-value@broker.example:1883"),
            Some("sensors/+/up"),
            Some("senml-json"),
            Some(secret["id"].as_str().unwrap()),
        )
        .await;

        let body = get_json(&app, "/dashboard/connectors/overview").await;
        let connector = body["connectors"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["connector_key"] == "dashboard-mqtt-01")
            .unwrap();
        let serialized = serde_json::to_string(&body).unwrap();

        assert_eq!(connector["secret_configured"], true);
        assert_eq!(connector["topic_filter"], "sensors/+/up");
        assert_eq!(connector["payload_format"], "senml-json");
        assert_eq!(connector["broker_url"], "mqtt://broker.example:1883");
        assert!(!serialized.contains("super-secret-value"));
        assert!(!serialized.contains("secret_value"));
    }

    #[tokio::test]
    async fn tenant_filtering_applies_for_non_admin_dashboard_tokens() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);

        let sensor_a =
            create_test_entity(&tenant_a_app, "dashboard-tenant-a-sensor", "aion:Sensor").await;
        let plot_a =
            create_test_entity(&tenant_a_app, "dashboard-tenant-a-plot", "aion:Plot").await;
        create_test_observation_at(
            &tenant_a_app,
            &sensor_a,
            &plot_a,
            "temperature",
            json!({"type": "number", "value": 20.0}),
            Some("Cel"),
            "2026-05-05T12:00:00Z",
        )
        .await;
        create_http_connector(
            &tenant_a_app,
            "dashboard-tenant-a-http",
            Some(&sensor_a),
            Some(&plot_a),
        )
        .await;

        let sensor_b =
            create_test_entity(&tenant_b_app, "dashboard-tenant-b-sensor", "aion:Sensor").await;
        let plot_b =
            create_test_entity(&tenant_b_app, "dashboard-tenant-b-plot", "aion:Plot").await;
        create_test_observation_at(
            &tenant_b_app,
            &sensor_b,
            &plot_b,
            "temperature",
            json!({"type": "number", "value": 30.0}),
            Some("Cel"),
            "2026-05-05T12:10:00Z",
        )
        .await;
        create_http_connector(
            &tenant_b_app,
            "dashboard-tenant-b-http",
            Some(&sensor_b),
            Some(&plot_b),
        )
        .await;

        let tenant_a_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("dashboard-tenant-a-reader"),
            &["dashboard:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let overview = to_json(
            app.clone()
                .oneshot(auth_request("GET", "/dashboard/overview", &tenant_a_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(overview["entities_count"], 2);
        assert_eq!(overview["observations_count"], 1);
        assert_eq!(overview["connectors_count"], 1);

        let timeseries = to_json(
            app.clone()
                .oneshot(auth_request(
                    "GET",
                    "/dashboard/timeseries/entities",
                    &tenant_a_token,
                ))
                .await
                .unwrap(),
        )
        .await;
        let timeseries_entities = timeseries["entities"].as_array().unwrap();
        assert_eq!(timeseries_entities.len(), 1);
        assert_eq!(
            timeseries_entities[0]["entity_key"],
            "dashboard-tenant-a-plot"
        );

        let connectors = to_json(
            app.clone()
                .oneshot(auth_request(
                    "GET",
                    "/dashboard/connectors/overview",
                    &tenant_a_token,
                ))
                .await
                .unwrap(),
        )
        .await;
        let connector_items = connectors["connectors"].as_array().unwrap();
        assert_eq!(connector_items.len(), 1);
        assert_eq!(
            connector_items[0]["connector_key"],
            "dashboard-tenant-a-http"
        );
    }

    #[tokio::test]
    async fn token_mode_protects_commands_read_with_commands_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&dev_app, "commands-auth-pump-01", "aion:Pump").await;
        create_test_command(&dev_app, &pump_id, "StartPump").await;
        let token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("commands-reader"),
            &["commands:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request(
                "GET",
                &format!("/commands?target_entity_id={pump_id}&status=pending"),
                &token,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_actions_read_with_actions_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&dev_app, "actions-auth-pump-01", "aion:Pump").await;
        let command = create_test_command(&dev_app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let action = dev_app
            .clone()
            .oneshot(json_request(
                "POST",
                "/actions",
                json!({
                    "command_id": command_id,
                    "action_type": "StartPump",
                    "status": "completed",
                    "started_at": "2026-05-05T12:00:00Z",
                    "finished_at": "2026-05-05T12:01:00Z"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(action.status(), StatusCode::CREATED);
        let action = to_json(action).await;
        let action_id = action["id"].as_str().unwrap();
        let result = dev_app
            .clone()
            .oneshot(json_request(
                "POST",
                "/action-results",
                json!({
                    "command_id": command_id,
                    "action_id": action_id,
                    "status": "succeeded",
                    "verified": true,
                    "result_payload": {"ok": true},
                    "observed_at": "2026-05-05T12:01:30Z"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(result.status(), StatusCode::CREATED);
        let token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("actions-reader"),
            &["actions:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let actions = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/actions?command_id={command_id}"),
                &token,
            ))
            .await
            .unwrap();
        assert_eq!(actions.status(), StatusCode::OK);

        let action_results = app
            .oneshot(auth_request(
                "GET",
                &format!("/action-results?command_id={command_id}"),
                &token,
            ))
            .await
            .unwrap();
        assert_eq!(action_results.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_rules_read_with_rules_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let tank_id = create_test_entity(&dev_app, "rules-auth-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&dev_app, "rules-auth-pump-01", "aion:Pump").await;
        create_low_water_command_rule(&dev_app, &tank_id, &pump_id, true, 20.0).await;
        let token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("rules-reader"),
            &["rules:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let response = app
            .oneshot(auth_request("GET", "/rules", &token))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_policies_and_capabilities_reads_with_dedicated_scopes() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&dev_app, "policy-auth-pump-01", "aion:Pump").await;
        put_start_pump_policy(&dev_app, &pump_id, true).await;
        let capabilities_response = dev_app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{pump_id}/capabilities"),
                json!([
                    {
                        "capability_name": "pump:start",
                        "command_type": "StartPump",
                        "protocol": "http"
                    }
                ]),
            ))
            .await
            .unwrap();
        assert_eq!(capabilities_response.status(), StatusCode::OK);
        let policies_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("policies-reader"),
            &["policies:read"],
        );
        let capabilities_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("capabilities-reader"),
            &["capabilities:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let policies = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/policies?target_entity_id={pump_id}&command_type=StartPump"),
                &policies_token,
            ))
            .await
            .unwrap();
        assert_eq!(policies.status(), StatusCode::OK);

        let capabilities = app
            .oneshot(auth_request(
                "GET",
                &format!("/entities/{pump_id}/capabilities"),
                &capabilities_token,
            ))
            .await
            .unwrap();
        assert_eq!(capabilities.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_protects_executor_inspection_reads_with_executors_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&dev_app, "executor-read-pump-01", "aion:Pump").await;
        let executor = create_test_executor(&dev_app, "executor-read-01").await;
        let executor_id = executor["id"].as_str().unwrap();
        let capabilities = dev_app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/executors/{executor_id}/capabilities"),
                json!([
                    {
                        "command_type": "StartPump",
                        "protocol": "http"
                    }
                ]),
            ))
            .await
            .unwrap();
        assert_eq!(capabilities.status(), StatusCode::OK);
        let scopes = dev_app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/executors/{executor_id}/scopes"),
                json!([
                    {
                        "target_entity_id": pump_id,
                        "metadata": {"source": "test"}
                    }
                ]),
            ))
            .await
            .unwrap();
        assert_eq!(scopes.status(), StatusCode::OK);
        let token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("executors-reader"),
            &["executors:read"],
        );
        let app = token_mode_app_with_storage(storage);

        let list = app
            .clone()
            .oneshot(auth_request("GET", "/executors", &token))
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);

        let detail = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/executors/{executor_id}"),
                &token,
            ))
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);

        let capabilities = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/executors/{executor_id}/capabilities"),
                &token,
            ))
            .await
            .unwrap();
        assert_eq!(capabilities.status(), StatusCode::OK);

        let scopes = app
            .oneshot(auth_request(
                "GET",
                &format!("/executors/{executor_id}/scopes"),
                &token,
            ))
            .await
            .unwrap();
        assert_eq!(scopes.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_all_satisfies_new_read_scope_checks() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let pump_id = create_test_entity(&dev_app, "admin-all-read-pump-01", "aion:Pump").await;
        let sensor_id =
            create_test_entity(&dev_app, "admin-all-read-sensor-01", "aion:Sensor").await;
        ingest_test_senml(&dev_app, &sensor_id, &pump_id).await;
        create_test_command(&dev_app, &pump_id, "StartPump").await;
        put_start_pump_policy(&dev_app, &pump_id, true).await;
        let executor = create_test_executor(&dev_app, "admin-all-read-executor-01").await;
        let executor_id = executor["id"].as_str().unwrap();
        let admin_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Admin,
            Some("admin-all-read"),
            &["admin:all"],
        );
        let app = token_mode_app_with_storage(storage);

        for uri in [
            "/entities".to_string(),
            format!("/entities/{pump_id}/context"),
            format!("/observations?feature_of_interest_id={pump_id}"),
            format!("/timeseries/query?entity_id={pump_id}&observed_property=temperature"),
            format!("/timeseries/entities/{pump_id}/properties"),
            format!("/commands?target_entity_id={pump_id}&status=pending"),
            "/rules".to_string(),
            format!("/policies?target_entity_id={pump_id}&command_type=StartPump"),
            format!("/entities/{pump_id}/capabilities"),
            "/executors".to_string(),
            format!("/executors/{executor_id}/capabilities"),
        ] {
            let response = app
                .clone()
                .oneshot(auth_request("GET", &uri, &admin_token))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "uri={uri}");
        }
    }

    #[tokio::test]
    async fn dev_mode_keeps_cross_tenant_bypass_behavior_for_entity_reads() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let entity_id =
            create_test_entity(&tenant_a_app, "tenant-a-dev-bypass-entity", "aion:Sensor").await;
        let tenant_b_token = store_api_token_for_tenant(
            &storage,
            tenant_b,
            ApiTokenPrincipalType::Service,
            Some("tenant-b-reader"),
            &["entities:read"],
        );

        let response = tenant_a_app
            .oneshot(auth_request(
                "GET",
                &format!("/entities/{entity_id}"),
                &tenant_b_token,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_enforces_tenant_ownership_for_entities_and_admin_bypass() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let entity_a = create_test_entity(&tenant_a_app, "tenant-a-entity-01", "aion:Sensor").await;
        let entity_b = create_test_entity(&tenant_b_app, "tenant-b-entity-01", "aion:Sensor").await;
        let tenant_a_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-reader"),
            &["entities:read"],
        );
        let tenant_b_token = store_api_token_for_tenant(
            &storage,
            tenant_b,
            ApiTokenPrincipalType::Service,
            Some("tenant-b-reader"),
            &["entities:read"],
        );
        let admin_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Admin,
            Some("platform-admin"),
            &["admin:all"],
        );

        let allowed = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/entities/{entity_a}"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);

        let denied = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/entities/{entity_a}"),
                &tenant_b_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let tenant_a_entities = to_json(
            app.clone()
                .oneshot(auth_request("GET", "/entities", &tenant_a_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(tenant_a_entities.as_array().unwrap().len(), 1);
        assert_eq!(tenant_a_entities[0]["id"], entity_a);

        let admin_entities = to_json(
            app.clone()
                .oneshot(auth_request("GET", "/entities", &admin_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(admin_entities.as_array().unwrap().len(), 2);

        let admin_detail = app
            .oneshot(auth_request(
                "GET",
                &format!("/entities/{entity_b}"),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(admin_detail.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn entity_context_filters_cross_tenant_relationships() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let source_id =
            create_test_entity(&tenant_a_app, "tenant-a-context-source", "aion:Pump").await;
        let same_tenant_target =
            create_test_entity(&tenant_a_app, "tenant-a-context-target", "aion:Valve").await;
        let cross_tenant_target =
            create_test_entity(&tenant_b_app, "tenant-b-context-target", "aion:Valve").await;
        let source_uuid = Uuid::parse_str(&source_id).unwrap();
        let same_tenant_target_uuid = Uuid::parse_str(&same_tenant_target).unwrap();
        let cross_tenant_target_uuid = Uuid::parse_str(&cross_tenant_target).unwrap();

        storage
            .create_relationship(
                Relationship::new(
                    tenant_a,
                    source_uuid,
                    "aion:connectedTo".to_string(),
                    same_tenant_target_uuid,
                    json!({"@type": "aion:Relationship"}),
                    Utc::now(),
                )
                .unwrap(),
            )
            .unwrap();
        storage
            .create_relationship(
                Relationship::new(
                    tenant_a,
                    source_uuid,
                    "aion:connectedTo".to_string(),
                    cross_tenant_target_uuid,
                    json!({"@type": "aion:Relationship"}),
                    Utc::now(),
                )
                .unwrap(),
            )
            .unwrap();

        let admin_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Admin,
            Some("platform-admin"),
            &["admin:all"],
        );
        let response = to_json(
            app.oneshot(auth_request(
                "GET",
                &format!("/entities/{source_id}/context"),
                &admin_token,
            ))
            .await
            .unwrap(),
        )
        .await;

        let outgoing = response["outgoing_relationships"].as_array().unwrap();
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0]["target_entity_id"], same_tenant_target);
    }

    #[tokio::test]
    async fn token_mode_filters_observations_commands_actions_rules_policies_and_events_by_tenant()
    {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let sensor_a =
            create_test_entity(&tenant_a_app, "tenant-a-obs-sensor", "aion:Sensor").await;
        let plot_a = create_test_entity(&tenant_a_app, "tenant-a-obs-plot", "aion:Plot").await;
        let sensor_b =
            create_test_entity(&tenant_b_app, "tenant-b-obs-sensor", "aion:Sensor").await;
        let plot_b = create_test_entity(&tenant_b_app, "tenant-b-obs-plot", "aion:Plot").await;
        let ingest_a = ingest_test_senml(&tenant_a_app, &sensor_a, &plot_a).await;
        let ingest_b = ingest_test_senml(&tenant_b_app, &sensor_b, &plot_b).await;
        let command_a = create_test_command(&tenant_a_app, &plot_a, "StartPump").await;
        let command_b = create_test_command(&tenant_b_app, &plot_b, "StartPump").await;
        let command_a_id = command_a["id"].as_str().unwrap();
        let command_b_id = command_b["id"].as_str().unwrap();
        let action_a = to_json(
            tenant_a_app
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/actions",
                    json!({
                        "command_id": command_a_id,
                        "action_type": "StartPump",
                        "status": "completed",
                        "started_at": "2026-05-05T12:00:00Z",
                        "finished_at": "2026-05-05T12:01:00Z"
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        let action_b = to_json(
            tenant_b_app
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/actions",
                    json!({
                        "command_id": command_b_id,
                        "action_type": "StartPump",
                        "status": "completed",
                        "started_at": "2026-05-05T12:00:00Z",
                        "finished_at": "2026-05-05T12:01:00Z"
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        let action_a_id = action_a["id"].as_str().unwrap();
        let action_b_id = action_b["id"].as_str().unwrap();
        tenant_a_app
            .clone()
            .oneshot(json_request(
                "POST",
                "/action-results",
                json!({
                    "command_id": command_a_id,
                    "action_id": action_a_id,
                    "status": "succeeded",
                    "verified": true,
                    "result_payload": {"ok": true},
                    "observed_at": "2026-05-05T12:01:30Z"
                }),
            ))
            .await
            .unwrap();
        tenant_b_app
            .clone()
            .oneshot(json_request(
                "POST",
                "/action-results",
                json!({
                    "command_id": command_b_id,
                    "action_id": action_b_id,
                    "status": "succeeded",
                    "verified": true,
                    "result_payload": {"ok": true},
                    "observed_at": "2026-05-05T12:01:30Z"
                }),
            ))
            .await
            .unwrap();
        let tank_a =
            create_test_entity(&tenant_a_app, "tenant-a-rule-tank", "aion:WaterTank").await;
        let pump_a = create_test_entity(&tenant_a_app, "tenant-a-rule-pump", "aion:Pump").await;
        let tank_b =
            create_test_entity(&tenant_b_app, "tenant-b-rule-tank", "aion:WaterTank").await;
        let pump_b = create_test_entity(&tenant_b_app, "tenant-b-rule-pump", "aion:Pump").await;
        create_low_water_command_rule(&tenant_a_app, &tank_a, &pump_a, true, 20.0).await;
        create_low_water_command_rule(&tenant_b_app, &tank_b, &pump_b, true, 20.0).await;
        put_start_pump_policy(&tenant_a_app, &plot_a, true).await;
        put_start_pump_policy(&tenant_b_app, &plot_b, true).await;

        let tenant_a_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-reader"),
            &[
                "observations:read",
                "commands:read",
                "actions:read",
                "rules:read",
                "policies:read",
                "events:read",
                "raw-messages:read",
            ],
        );

        let observations = to_json(
            app.clone()
                .oneshot(auth_request("GET", "/observations", &tenant_a_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(observations.as_array().unwrap().len(), 2);

        let commands = to_json(
            app.clone()
                .oneshot(auth_request("GET", "/commands", &tenant_a_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(commands.as_array().unwrap().len(), 1);
        assert_eq!(commands[0]["id"], command_a_id);

        let denied_command_detail = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/commands/{command_b_id}"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied_command_detail.status(), StatusCode::FORBIDDEN);

        let actions = to_json(
            app.clone()
                .oneshot(auth_request(
                    "GET",
                    &format!("/actions?command_id={command_a_id}"),
                    &tenant_a_token,
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(actions.as_array().unwrap().len(), 1);

        let action_results = to_json(
            app.clone()
                .oneshot(auth_request(
                    "GET",
                    &format!("/action-results?command_id={command_a_id}"),
                    &tenant_a_token,
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(action_results.as_array().unwrap().len(), 1);

        let rules = to_json(
            app.clone()
                .oneshot(auth_request("GET", "/rules", &tenant_a_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(rules.as_array().unwrap().len(), 1);

        let policies = to_json(
            app.clone()
                .oneshot(auth_request("GET", "/policies", &tenant_a_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(policies.as_array().unwrap().len(), 1);

        let raw_messages = to_json(
            app.clone()
                .oneshot(auth_request("GET", "/raw-messages", &tenant_a_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(raw_messages.as_array().unwrap().len(), 1);

        let tenant_a_events = to_json(
            app.clone()
                .oneshot(auth_request(
                    "GET",
                    &format!(
                        "/events?raw_message_id={}",
                        ingest_a["raw_message_id"].as_str().unwrap()
                    ),
                    &tenant_a_token,
                ))
                .await
                .unwrap(),
        )
        .await;
        assert!(!tenant_a_events.as_array().unwrap().is_empty());

        let cross_tenant_event_id = query_events_by_raw_message(
            &tenant_b_app,
            ingest_b["raw_message_id"].as_str().unwrap(),
        )
        .await[0]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let denied_event = app
            .oneshot(auth_request(
                "GET",
                &format!("/events/{cross_tenant_event_id}"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied_event.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn token_mode_denies_cross_tenant_timeseries_queries_for_non_admins() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let sensor_a =
            create_test_entity(&tenant_a_app, "timeseries-tenant-a-sensor", "aion:Sensor").await;
        let plot_a =
            create_test_entity(&tenant_a_app, "timeseries-tenant-a-plot", "aion:Plot").await;
        let sensor_b =
            create_test_entity(&tenant_b_app, "timeseries-tenant-b-sensor", "aion:Sensor").await;
        let plot_b =
            create_test_entity(&tenant_b_app, "timeseries-tenant-b-plot", "aion:Plot").await;
        create_test_observation_at(
            &tenant_a_app,
            &sensor_a,
            &plot_a,
            "temperature",
            json!({"type": "number", "value": 10.0}),
            Some("Cel"),
            "2026-05-05T12:00:00Z",
        )
        .await;
        create_test_observation_at(
            &tenant_b_app,
            &sensor_b,
            &plot_b,
            "temperature",
            json!({"type": "number", "value": 20.0}),
            Some("Cel"),
            "2026-05-05T12:00:00Z",
        )
        .await;
        let tenant_b_token = store_api_token_for_tenant(
            &storage,
            tenant_b,
            ApiTokenPrincipalType::Service,
            Some("tenant-b-timeseries-reader"),
            &["timeseries:read"],
        );

        let denied = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/timeseries/query?entity_id={plot_a}&observed_property=temperature"),
                &tenant_b_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);

        let denied_properties = app
            .oneshot(auth_request(
                "GET",
                &format!("/timeseries/entities/{plot_a}/properties"),
                &tenant_b_token,
            ))
            .await
            .unwrap();
        assert_eq!(denied_properties.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn timeseries_query_returns_chronological_points_and_filters_by_property_and_time_range()
    {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor_id = create_test_entity(&app, "timeseries-order-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "timeseries-order-plot-01", "aion:Plot").await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "temperature",
            json!({"type": "number", "value": 30.0}),
            Some("Cel"),
            "2026-05-05T12:02:00Z",
        )
        .await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "humidity",
            json!({"type": "number", "value": 40.0}),
            Some("%"),
            "2026-05-05T12:00:30Z",
        )
        .await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "temperature",
            json!({"type": "number", "value": 10.0}),
            Some("Cel"),
            "2026-05-05T12:00:00Z",
        )
        .await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "temperature",
            json!({"type": "number", "value": 20.0}),
            Some("Cel"),
            "2026-05-05T12:01:00Z",
        )
        .await;

        let response = get_json(
            &app,
            &format!(
                "/timeseries/query?entity_id={plot_id}&observed_property=temperature&from=2026-05-05T12:00:00Z&to=2026-05-05T12:02:00Z"
            ),
        )
        .await;
        let points = response["points"].as_array().unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0]["time"], "2026-05-05T12:00:00Z");
        assert_eq!(points[1]["time"], "2026-05-05T12:01:00Z");
        assert_eq!(points[2]["time"], "2026-05-05T12:02:00Z");
        assert!(points
            .iter()
            .all(|point| point["value"]["value"].as_f64().is_some()));

        let filtered = get_json(
            &app,
            &format!(
                "/timeseries/query?entity_id={plot_id}&observed_property=temperature&from=2026-05-05T12:00:30Z&to=2026-05-05T12:01:30Z"
            ),
        )
        .await;
        let filtered_points = filtered["points"].as_array().unwrap();
        assert_eq!(filtered_points.len(), 1);
        assert_eq!(filtered_points[0]["time"], "2026-05-05T12:01:00Z");
    }

    #[tokio::test]
    async fn timeseries_properties_endpoint_lists_properties_for_entity() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor_id = create_test_entity(&app, "timeseries-props-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "timeseries-props-plot-01", "aion:Plot").await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "soil.moisture",
            json!({"type": "number", "value": 15.0}),
            Some("%"),
            "2026-05-05T12:00:00Z",
        )
        .await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "temperature",
            json!({"type": "number", "value": 21.0}),
            Some("Cel"),
            "2026-05-05T12:01:00Z",
        )
        .await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "soil.moisture",
            json!({"type": "number", "value": 17.0}),
            Some("%"),
            "2026-05-05T12:02:00Z",
        )
        .await;

        let response = get_json(&app, &format!("/timeseries/entities/{plot_id}/properties")).await;
        let properties = response["properties"].as_array().unwrap();
        assert_eq!(properties.len(), 2);
        assert_eq!(properties[0]["observed_property"], "soil.moisture");
        assert_eq!(properties[0]["count"], 2);
        assert_eq!(properties[0]["units"], json!(["%"]));
        assert_eq!(properties[0]["first_observed_at"], "2026-05-05T12:00:00Z");
        assert_eq!(properties[0]["last_observed_at"], "2026-05-05T12:02:00Z");
        assert_eq!(properties[1]["observed_property"], "temperature");
    }

    #[tokio::test]
    async fn timeseries_query_supports_last_count_avg_min_and_max_aggregations() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor_id = create_test_entity(&app, "timeseries-agg-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "timeseries-agg-plot-01", "aion:Plot").await;
        for (time, value) in [
            ("2026-05-05T12:00:00Z", 10.0),
            ("2026-05-05T12:01:00Z", 20.0),
            ("2026-05-05T12:02:00Z", 30.0),
        ] {
            create_test_observation_at(
                &app,
                &sensor_id,
                &plot_id,
                "temperature",
                json!({"type": "number", "value": value}),
                Some("Cel"),
                time,
            )
            .await;
        }

        let last = get_json(
            &app,
            &format!("/timeseries/query?entity_id={plot_id}&observed_property=temperature&aggregation=last"),
        )
        .await;
        assert_eq!(last["points"][0]["time"], "2026-05-05T12:02:00Z");
        assert_eq!(last["points"][0]["value"]["value"], 30.0);

        let count = get_json(
            &app,
            &format!("/timeseries/query?entity_id={plot_id}&observed_property=temperature&aggregation=count"),
        )
        .await;
        assert_eq!(count["points"][0]["value"]["value"], 3.0);

        let avg = get_json(
            &app,
            &format!("/timeseries/query?entity_id={plot_id}&observed_property=temperature&aggregation=avg"),
        )
        .await;
        assert_eq!(avg["points"][0]["value"]["value"], 20.0);

        let min = get_json(
            &app,
            &format!("/timeseries/query?entity_id={plot_id}&observed_property=temperature&aggregation=min"),
        )
        .await;
        assert_eq!(min["points"][0]["value"]["value"], 10.0);

        let max = get_json(
            &app,
            &format!("/timeseries/query?entity_id={plot_id}&observed_property=temperature&aggregation=max"),
        )
        .await;
        assert_eq!(max["points"][0]["value"]["value"], 30.0);
    }

    #[tokio::test]
    async fn timeseries_numeric_aggregation_handles_non_numeric_values_safely() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);
        let sensor_id = create_test_entity(&app, "timeseries-text-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "timeseries-text-plot-01", "aion:Plot").await;
        create_test_observation_at(
            &app,
            &sensor_id,
            &plot_id,
            "status",
            json!({"type": "text", "value": "ok"}),
            None,
            "2026-05-05T12:00:00Z",
        )
        .await;

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/timeseries/query?entity_id={plot_id}&observed_property=status&aggregation=avg"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_json(response).await;
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("numeric aggregation"));
    }

    #[tokio::test]
    async fn token_mode_enforces_tenant_ownership_for_capabilities_and_executor_reads() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let app = token_mode_app_with_storage(storage.clone());
        let entity_b =
            create_test_entity(&tenant_b_app, "tenant-b-capability-entity", "aion:Pump").await;
        tenant_b_app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{entity_b}/capabilities"),
                json!([
                    {
                        "capability_name": "pump:start",
                        "command_type": "StartPump",
                        "protocol": "http"
                    }
                ]),
            ))
            .await
            .unwrap();
        let executor_b = create_test_executor(&tenant_b_app, "tenant-b-executor-01").await;
        let executor_b_id = executor_b["id"].as_str().unwrap();
        tenant_b_app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/executors/{executor_b_id}/capabilities"),
                json!([
                    {
                        "command_type": "StartPump",
                        "protocol": "http"
                    }
                ]),
            ))
            .await
            .unwrap();
        tenant_b_app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/executors/{executor_b_id}/scopes"),
                json!([
                    {
                        "entity_type": "aion:Pump",
                        "metadata": {"source": "test"}
                    }
                ]),
            ))
            .await
            .unwrap();

        let tenant_a_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-reader"),
            &["capabilities:read", "executors:read"],
        );

        let capability_denied = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/entities/{entity_b}/capabilities"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(capability_denied.status(), StatusCode::FORBIDDEN);

        let executor_denied = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/executors/{executor_b_id}"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(executor_denied.status(), StatusCode::FORBIDDEN);

        let executor_caps_denied = app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/executors/{executor_b_id}/capabilities"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(executor_caps_denied.status(), StatusCode::FORBIDDEN);

        let executor_scopes_denied = app
            .oneshot(auth_request(
                "GET",
                &format!("/executors/{executor_b_id}/scopes"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(executor_scopes_denied.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn existing_endpoints_still_work_without_credentials_in_dev_mode() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/entities",
                json!({
                    "entity_key": "dev-sensor-01",
                    "entity_type": "aion:Sensor",
                    "jsonld": {
                        "@context": {
                            "aion": "https://aioncore.ai/ontology#"
                        },
                        "@id": "urn:aion:test:dev-sensor-01",
                        "@type": "aion:Sensor",
                        "name": "Development Sensor"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn disabled_mode_still_allows_existing_endpoints_without_credentials() {
        let state = AppState::with_backend_storage_and_auth(
            Arc::new(InMemoryStorage::new()),
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Disabled,
                bootstrap_admin_token_hash: None,
            },
            Uuid::nil(),
        );
        let app = app_with_state(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/entities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body, json!([]));
    }

    #[tokio::test]
    async fn ingests_smartsentinel_snapshot_and_materializes_records() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_sample_snapshot(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let summary = to_json(response).await;
        assert_eq!(summary["snapshot_id"], "snap-001");
        assert_eq!(summary["node_id"], "fog-01");
        assert_eq!(summary["entities_created"], 2);
        assert_eq!(summary["entities_updated"], 0);
        assert_eq!(summary["entities_reused"], 0);
        assert_eq!(summary["relationships_created"], 1);
        assert_eq!(summary["relationships_reused"], 0);
        assert_eq!(summary["relationships_skipped"], 0);
        assert_eq!(summary["observations_created"], 2);
        assert_eq!(summary["events_created"], 1);
        assert_eq!(summary["validation_errors"].as_array().unwrap().len(), 0);
        let raw_message_id = summary["raw_message_id"].as_str().unwrap();

        let raw_messages = get_json(
            &app,
            "/raw-messages?payload_format=smartsentinel-snapshot-json",
        )
        .await;
        assert_eq!(raw_messages.as_array().unwrap().len(), 1);
        assert_eq!(raw_messages[0]["raw_message_id"], raw_message_id);
        assert_eq!(
            raw_messages[0]["payload_format"],
            SMARTSENTINEL_PAYLOAD_FORMAT
        );

        let entities = get_json(&app, "/entities").await;
        let host_id = entity_id_by_key(&entities, "smartsentinel:fog-01:host:fog-01");
        let service_id = entity_id_by_key(&entities, "smartsentinel:fog-01:service:mosquitto");

        let host_context = get_json(&app, &format!("/entities/{host_id}/context")).await;
        assert_eq!(
            host_context["outgoing_relationships"][0]["relationship_type"],
            "sentinel:runs"
        );

        let observations = get_json(
            &app,
            &format!("/observations?feature_of_interest_id={service_id}"),
        )
        .await;
        assert!(observations.as_array().unwrap().iter().any(|observation| {
            observation["observed_property"] == "sentinel:ServiceStatus"
                && observation["value"]["value"] == "healthy"
        }));

        let events = get_json(&app, &format!("/events?raw_message_id={raw_message_id}")).await;
        assert!(events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "sentinel:ServiceDegraded"
                && event["target_entity_id"] == service_id
        }));
        assert!(events
            .as_array()
            .unwrap()
            .iter()
            .any(|event| { event["event_type"] == "aion:SmartSentinelSnapshotMapped" }));

        let ai_context = get_json(&app, &format!("/ai/context/entity/{service_id}")).await;
        assert_eq!(
            ai_context["target_entity"]["entity_key"],
            "smartsentinel:fog-01:service:mosquitto"
        );
        assert!(!ai_context["recent_observations"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn repeated_smartsentinel_snapshot_reuses_relationship_and_entities() {
        let app = app();
        let first_response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_sample_snapshot(),
            ))
            .await
            .unwrap();
        assert_eq!(first_response.status(), StatusCode::CREATED);

        let second_response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_sample_snapshot(),
            ))
            .await
            .unwrap();
        assert_eq!(second_response.status(), StatusCode::CREATED);
        let summary = to_json(second_response).await;

        assert_eq!(summary["entities_created"], 0);
        assert_eq!(summary["entities_updated"], 0);
        assert_eq!(summary["entities_reused"], 2);
        assert_eq!(summary["relationships_created"], 0);
        assert_eq!(summary["relationships_reused"], 1);
        assert_eq!(summary["relationships_skipped"], 0);

        let entities = get_json(&app, "/entities").await;
        let host_id = entity_id_by_key(&entities, "smartsentinel:fog-01:host:fog-01");
        let host_context = get_json(&app, &format!("/entities/{host_id}/context")).await;
        assert_eq!(
            host_context["outgoing_relationships"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn smartsentinel_snapshot_updates_existing_entity_jsonld() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_sample_snapshot(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let mut updated = smartsentinel_sample_snapshot();
        updated["entities"][1]["status"] = json!("degraded");
        updated["entities"][1]["properties"] = json!({"version": "2.0.0"});

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                updated,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let summary = to_json(response).await;
        assert_eq!(summary["entities_created"], 0);
        assert_eq!(summary["entities_updated"], 1);
        assert_eq!(summary["entities_reused"], 1);

        let entities = get_json(&app, "/entities").await;
        let service = entity_by_key(&entities, "smartsentinel:fog-01:service:mosquitto");
        assert_eq!(service["jsonld"]["sentinel:status"], "degraded");
        assert_eq!(service["jsonld"]["sentinel:properties"]["version"], "2.0.0");
    }

    #[tokio::test]
    async fn invalid_smartsentinel_snapshot_preserves_raw_and_records_failure_event() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                json!({
                    "snapshot_id": "snap-bad",
                    "node_id": "fog-01",
                    "observed_at": "not-a-date"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_json(response).await;
        assert!(body["validation_errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["code"] == "observed_at_invalid"));

        let raw_messages = get_json(
            &app,
            "/raw-messages?payload_format=smartsentinel-snapshot-json",
        )
        .await;
        let raw_message_id = raw_messages[0]["raw_message_id"].as_str().unwrap();
        assert_eq!(raw_messages[0]["normalization_status"], "failed");
        assert!(raw_messages[0]["normalization_error"]
            .as_str()
            .unwrap()
            .contains("validation failed"));

        let events = get_json(&app, &format!("/events?raw_message_id={raw_message_id}")).await;
        assert!(events
            .as_array()
            .unwrap()
            .iter()
            .any(|event| { event["event_type"] == "aion:SmartSentinelSnapshotMappingFailed" }));
    }

    #[tokio::test]
    async fn invalid_smartsentinel_snapshot_missing_node_id_returns_structured_validation_error() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                json!({
                    "snapshot_id": "snap-bad",
                    "observed_at": "2026-04-29T12:00:00Z"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_json(response).await;
        assert!(body["validation_errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["path"] == "$.node_id" && issue["code"] == "node_id_missing"));

        let raw_messages = get_json(
            &app,
            "/raw-messages?payload_format=smartsentinel-snapshot-json",
        )
        .await;
        assert_eq!(raw_messages[0]["normalization_status"], "failed");
    }

    #[tokio::test]
    async fn invalid_smartsentinel_relationship_reports_unknown_target() {
        let app = app();
        let mut snapshot = smartsentinel_sample_snapshot();
        snapshot["relationships"][0]["target"] = json!("service:missing");

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                snapshot,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_json(response).await;
        assert!(body["validation_errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["code"] == "relationship_target_unknown"));
    }

    #[tokio::test]
    async fn invalid_smartsentinel_observation_reports_validation_error() {
        let app = app();
        let mut snapshot = smartsentinel_sample_snapshot();
        snapshot["observations"][0]["observed_property"] = json!("");

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                snapshot,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_json(response).await;
        assert!(body["validation_errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["code"] == "observation_observed_property_missing"));
    }

    #[tokio::test]
    async fn smartsentinel_endpoint_does_not_change_normal_http_ingestion() {
        let app = app();
        let entity = create_native_entity(
            &app,
            json!({
                "@context": {"aion": "https://aioncore.org/ns#"},
                "@id": "urn:aion:test:device:normal-01",
                "@type": "aion:Device",
                "entity_key": "normal-01"
            }),
        )
        .await;
        let entity_id = entity["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": entity_id,
                    "feature_of_interest_id": entity_id,
                    "payload_format": "canonical-json",
                    "protocol": "http",
                    "content_type": "application/json",
                    "payload": {
                        "observed_property": "temperature",
                        "value": 21.5,
                        "observed_at": "2026-04-29T12:00:00Z"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_json(response).await;
        assert_eq!(body["observations"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn smartsentinel_snapshot_preserves_provenance_and_evidence_metadata() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_snapshot_with_provenance(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let summary = to_json(response).await;
        assert_eq!(summary["provenance_present"], true);
        assert_eq!(summary["evidence_count"], 2);
        assert_eq!(summary["external_ref_count"], 1);
        assert_eq!(summary["correlation_id"], "corr-123");
        assert_eq!(summary["trace_id"], "trace-abc");
        assert_eq!(summary["run_id"], "run-42");
        assert_eq!(summary["cycle_id"], "cycle-7");
        let raw_message_id = summary["raw_message_id"].as_str().unwrap();

        let events = get_json(&app, &format!("/events?raw_message_id={raw_message_id}")).await;
        assert!(events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "aion:SmartSentinelSnapshotReceived"
                && event["metadata"]["provenance"]["correlation_id"] == "corr-123"
                && event["metadata"]["evidence_count"] == 2
        }));
        assert!(events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "aion:SmartSentinelSnapshotMapped"
                && event["metadata"]["trace_id"] == "trace-abc"
                && event["metadata"]["external_ref_count"] == 1
        }));

        let sentinel_event = events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_type"] == "sentinel:IncidentOpened")
            .unwrap();
        assert_eq!(sentinel_event["metadata"]["incident_id"], "inc-001");
        assert_eq!(sentinel_event["metadata"]["alert_id"], "alert-001");
        assert_eq!(sentinel_event["metadata"]["workflow_id"], "wf-remediate");
        assert_eq!(sentinel_event["metadata"]["evidence_refs"][0], "ev-log-1");
        assert_eq!(sentinel_event["metadata"]["uri_fetch_attempted"], false);

        let entities = get_json(&app, "/entities").await;
        let service_id = entity_id_by_key(&entities, "smartsentinel:fog-02:service:api");
        let observations = get_json(
            &app,
            &format!("/observations?feature_of_interest_id={service_id}"),
        )
        .await;
        assert!(observations.as_array().unwrap().iter().any(|observation| {
            observation["metadata"]["evidence_refs"][0] == "ev-metric-1"
                && observation["metadata"]["provenance"]["run_id"] == "run-42"
                && observation["metadata"]["uri_fetch_attempted"] == false
        }));

        let ai_context = get_json(&app, &format!("/ai/context/entity/{service_id}")).await;
        assert!(ai_context["recent_events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| {
                event["metadata"]["incident_id"] == "inc-001"
                    && event["metadata"]["evidence_refs"][0] == "ev-log-1"
            }));
        assert!(ai_context["recent_observations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|observation| observation["metadata"]["evidence_refs"][0] == "ev-metric-1"));
    }

    #[tokio::test]
    async fn smartsentinel_events_can_be_queried_by_external_references() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_snapshot_with_provenance(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        let incident_events = get_json(&app, "/events?incident_id=inc-001").await;
        assert!(incident_events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "sentinel:IncidentOpened"
                && event["metadata"]["incident_id"] == "inc-001"
        }));

        let alert_events = get_json(&app, "/events?alert_id=alert-001").await;
        assert!(alert_events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "sentinel:IncidentOpened"
                && event["metadata"]["alert_id"] == "alert-001"
        }));

        let evidence_events = get_json(&app, "/events?evidence_id=ev-log-1").await;
        assert!(evidence_events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "sentinel:IncidentOpened"
                && event["metadata"]["evidence_refs"][0] == "ev-log-1"
        }));

        let external_events = get_json(&app, "/events?external_id=log-001").await;
        assert!(external_events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "sentinel:IncidentOpened"
                && event["metadata"]["evidence"][0]["external_id"] == "log-001"
        }));
    }

    #[tokio::test]
    async fn registers_edge_adapter_creates_entity_and_emits_events() {
        let app = app();
        let adapter_key = format!("edge-adapter-register-{}", Uuid::new_v4());

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": adapter_key,
                    "display_name": "Fog Gateway",
                    "adapter_type": "edge",
                    "status": "online",
                    "version": "1.2.3",
                    "host_id": "host-01",
                    "site_id": "site-01",
                    "environment": "fog",
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let registration = to_json(response).await;
        let adapter_id = registration["adapter"]["id"].as_str().unwrap();
        assert_eq!(registration["reused"], false);
        assert_eq!(registration["adapter"]["adapter_key"], adapter_key);
        assert_eq!(registration["status"]["status"], "online");
        assert_eq!(registration["entity"]["entity_type"], "aion:EdgeAdapter");
        assert_eq!(registration["entity"]["jsonld"]["adapter_key"], adapter_key);

        let adapters = get_json(&app, "/adapters").await;
        assert!(adapters.as_array().unwrap().iter().any(|adapter| {
            adapter["id"] == adapter_id && adapter["adapter_key"] == adapter_key
        }));

        let entity = get_json(
            &app,
            &format!(
                "/entities?entity_type=aion:EdgeAdapter&entity_key=edge-adapter:{adapter_key}"
            ),
        )
        .await;
        assert!(entity.as_array().unwrap().iter().any(|entity| {
            entity["entity_key"] == format!("edge-adapter:{adapter_key}")
                && entity["jsonld"]["adapter_key"] == adapter_key
        }));

        let registered_events =
            get_json(&app, "/events?event_type=aion:EdgeAdapterRegistered").await;
        assert!(registered_events.as_array().unwrap().iter().any(|event| {
            event["metadata"]["adapter_key"] == adapter_key
                && event["metadata"]["status_report"]["status"] == "online"
        }));
        let status_changed_events =
            get_json(&app, "/events?event_type=aion:EdgeAdapterStatusChanged").await;
        assert!(status_changed_events
            .as_array()
            .unwrap()
            .iter()
            .any(|event| {
                event["metadata"]["adapter_key"] == adapter_key
                    && event["metadata"]["metadata"]["current_status"] == "online"
            }));
    }

    #[tokio::test]
    async fn re_registering_same_edge_adapter_key_reuses_existing_record() {
        let app = app();
        let adapter_key = format!("edge-adapter-reuse-{}", Uuid::new_v4());

        let first = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": adapter_key,
                    "adapter_type": "fog",
                    "status": "offline"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = to_json(first).await;
        let adapter_id = first_body["adapter"]["id"].as_str().unwrap().to_string();

        let second = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": adapter_key,
                    "display_name": "Updated Fog Gateway",
                    "adapter_type": "fog",
                    "status": "degraded",
                    "metadata": {
                        "source": "test",
                        "revision": 2
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let second_body = to_json(second).await;
        assert_eq!(second_body["reused"], true);
        assert_eq!(second_body["adapter"]["id"], adapter_id);
        assert_eq!(
            second_body["adapter"]["display_name"],
            "Updated Fog Gateway"
        );
        assert_eq!(second_body["adapter"]["status"], "degraded");

        let adapters = get_json(&app, "/adapters").await;
        assert_eq!(adapters.as_array().unwrap().len(), 1);
        let fetched = get_json(&app, &format!("/adapters/{adapter_id}")).await;
        assert_eq!(fetched["id"], adapter_id);
        assert_eq!(fetched["adapter_key"], adapter_key);
    }

    #[tokio::test]
    async fn edge_adapter_heartbeat_updates_status_and_last_seen_at() {
        let app = app();
        let adapter_key = format!("edge-adapter-heartbeat-{}", Uuid::new_v4());

        let register = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": adapter_key,
                    "adapter_type": "edge",
                    "status": "online"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(register.status(), StatusCode::CREATED);
        let adapter = to_json(register).await;
        let adapter_id = adapter["adapter"]["id"].as_str().unwrap().to_string();

        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/adapters/{adapter_id}/heartbeat"),
                json!({
                    "status": "degraded",
                    "version": "1.2.4",
                    "host_id": "host-02",
                    "site_id": "site-01",
                    "environment": "fog",
                    "observed_at": "2026-04-29T15:00:00Z",
                    "uptime_seconds": 3600,
                    "active_connectors": 3,
                    "active_plugins": 2,
                    "dlq_depth": 7,
                    "dlq_oldest_record_at": "2026-04-29T14:30:00Z",
                    "last_publish_success_at": "2026-04-29T14:59:00Z",
                    "last_publish_failure_at": "2026-04-29T14:58:30Z",
                    "last_error": "broker unavailable",
                    "metadata": {
                        "source": "test",
                        "dlq_replayed": false
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let heartbeat = to_json(response).await;
        assert_eq!(heartbeat["adapter"]["status"], "degraded");
        assert_eq!(heartbeat["adapter"]["last_seen_at"], "2026-04-29T15:00:00Z");
        assert_eq!(heartbeat["status"]["status"], "degraded");
        assert_eq!(heartbeat["status"]["dlq_depth"], 7);
        assert_eq!(heartbeat["status"]["last_error"], "broker unavailable");

        let fetched = get_json(&app, &format!("/adapters/{adapter_id}")).await;
        assert_eq!(fetched["status"], "degraded");
        assert_eq!(fetched["last_seen_at"], "2026-04-29T15:00:00Z");

        let status = get_json(&app, &format!("/adapters/{adapter_id}/status")).await;
        assert_eq!(status["status"]["dlq_depth"], 7);
        assert_eq!(status["status"]["active_connectors"], 3);
        assert_eq!(status["status"]["active_plugins"], 2);
        assert_eq!(status["status"]["last_error"], "broker unavailable");

        let heartbeat_events = get_json(&app, "/events?event_type=aion:EdgeAdapterHeartbeat").await;
        assert!(heartbeat_events.as_array().unwrap().iter().any(|event| {
            event["metadata"]["adapter_key"] == adapter_key
                && event["metadata"]["status_report"]["dlq_depth"] == 7
        }));
        let status_changed_events =
            get_json(&app, "/events?event_type=aion:EdgeAdapterStatusChanged").await;
        assert!(status_changed_events
            .as_array()
            .unwrap()
            .iter()
            .any(|event| {
                event["metadata"]["adapter_key"] == adapter_key
                    && event["metadata"]["metadata"]["previous_status"] == "online"
                    && event["metadata"]["metadata"]["current_status"] == "degraded"
            }));
    }

    #[tokio::test]
    async fn edge_adapter_status_endpoint_returns_dlq_fields() {
        let app = app();
        let adapter_key = format!("edge-adapter-status-{}", Uuid::new_v4());

        let register = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/adapters",
                json!({
                    "adapter_key": adapter_key,
                    "adapter_type": "lab",
                    "status": "online"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(register.status(), StatusCode::CREATED);
        let adapter = to_json(register).await;
        let adapter_id = adapter["adapter"]["id"].as_str().unwrap().to_string();

        app.clone()
            .oneshot(json_request(
                "PUT",
                &format!("/adapters/{adapter_id}/heartbeat"),
                json!({
                    "status": "online",
                    "observed_at": "2026-04-29T15:15:00Z",
                    "dlq_depth": 12,
                    "dlq_oldest_record_at": "2026-04-29T15:00:00Z",
                    "last_publish_success_at": "2026-04-29T15:14:00Z",
                    "metadata": {
                        "source": "test",
                        "queue": "offline"
                    }
                }),
            ))
            .await
            .unwrap();

        let status = get_json(&app, &format!("/adapters/{adapter_id}/status")).await;
        assert_eq!(status["adapter"]["adapter_key"], adapter_key);
        assert_eq!(status["status"]["dlq_depth"], 12);
        assert_eq!(
            status["status"]["dlq_oldest_record_at"],
            "2026-04-29T15:00:00Z"
        );
        assert_eq!(
            status["status"]["last_publish_success_at"],
            "2026-04-29T15:14:00Z"
        );
        assert_eq!(status["status"]["metadata"]["queue"], "offline");
    }

    #[tokio::test]
    async fn smartsentinel_lifecycle_events_can_be_queried_by_provenance() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_snapshot_with_provenance(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        let trace_events = get_json(&app, "/events?trace_id=trace-abc").await;
        assert!(trace_events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "aion:SmartSentinelSnapshotMapped"
                && event["metadata"]["trace_id"] == "trace-abc"
        }));

        let run_events = get_json(&app, "/events?run_id=run-42").await;
        assert!(run_events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "aion:SmartSentinelSnapshotReceived"
                && event["metadata"]["run_id"] == "run-42"
        }));

        let cycle_events = get_json(&app, "/events?cycle_id=cycle-7").await;
        assert!(cycle_events.as_array().unwrap().iter().any(|event| {
            event["event_type"] == "aion:SmartSentinelSnapshotMapped"
                && event["metadata"]["cycle_id"] == "cycle-7"
        }));
    }

    #[tokio::test]
    async fn smartsentinel_raw_messages_can_be_queried_by_provenance() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_snapshot_with_provenance(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let summary = to_json(response).await;
        let raw_message_id = summary["raw_message_id"].as_str().unwrap();

        let raw_messages = get_json(
            &app,
            "/raw-messages?trace_id=trace-abc&run_id=run-42&cycle_id=cycle-7&snapshot_id=snap-prov-001&node_id=fog-02&connector_profile=smartsentinel",
        )
        .await;
        assert!(raw_messages.as_array().unwrap().iter().any(|raw_message| {
            raw_message["raw_message_id"] == raw_message_id
                && raw_message["payload_format"] == "smartsentinel-snapshot-json"
                && raw_message["connector_profile"] == "smartsentinel"
        }));
    }

    #[tokio::test]
    async fn provenance_search_returns_matching_events_raw_messages_and_observations() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_snapshot_with_provenance(),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);

        let results = get_json(&app, "/provenance/search?trace_id=trace-abc").await;
        assert!(results["counts"]["matching_events"].as_u64().unwrap() >= 2);
        assert!(results["counts"]["matching_raw_messages"].as_u64().unwrap() >= 1);
        assert!(results["counts"]["matching_observations"].as_u64().unwrap() >= 1);
        assert!(results["matching_events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["metadata"]["trace_id"] == "trace-abc"));
        assert!(results["matching_raw_messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|raw_message| raw_message["payload_format"] == "smartsentinel-snapshot-json"));
        assert!(results["matching_observations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|observation| {
                observation["metadata"]["trace_id"] == "trace-abc"
                    && observation["metadata"]["uri_fetch_attempted"] == false
            }));
    }

    #[tokio::test]
    async fn invalid_smartsentinel_evidence_uri_is_warning_not_fetch() {
        let app = app();
        let mut snapshot = smartsentinel_snapshot_with_provenance();
        snapshot["evidence"][0]["uri"] = json!({"not": "a string"});

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                snapshot,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let summary = to_json(response).await;
        assert_eq!(summary["evidence_count"], 1);
        assert!(summary["validation_warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["code"] == "evidence_uri_invalid"));
        assert!(summary["skipped_items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["reason"] == "evidence_uri_invalid"));
    }

    #[test]
    fn storage_backend_config_defaults_to_memory() {
        assert_eq!(
            StorageBackendConfig::from_env_vars(None, None).unwrap(),
            StorageBackendConfig::Memory
        );
    }

    #[test]
    fn storage_backend_config_accepts_explicit_memory() {
        assert_eq!(
            StorageBackendConfig::from_env_vars(Some("memory".to_string()), None).unwrap(),
            StorageBackendConfig::Memory
        );
    }

    #[test]
    fn storage_backend_config_requires_database_url_for_postgres() {
        let error = StorageBackendConfig::from_env_vars(Some("postgres".to_string()), None)
            .expect_err("postgres should require a database URL");
        assert!(error.to_string().contains("AIONCORE_DATABASE_URL"));
    }

    #[test]
    fn storage_backend_config_rejects_unknown_backend() {
        let error = StorageBackendConfig::from_env_vars(Some("sqlite".to_string()), None)
            .expect_err("unknown backend should fail");
        assert!(error
            .to_string()
            .contains("unknown AIONCORE_STORAGE_BACKEND"));
    }

    #[test]
    fn auth_config_defaults_to_dev() {
        assert_eq!(
            AuthConfig::from_env_vars(None, None).unwrap().mode,
            AuthMode::Dev
        );
    }

    #[test]
    fn auth_config_accepts_explicit_dev() {
        assert_eq!(
            AuthConfig::from_env_vars(Some("dev".to_string()), None)
                .unwrap()
                .mode,
            AuthMode::Dev
        );
    }

    #[test]
    fn auth_config_accepts_explicit_disabled() {
        assert_eq!(
            AuthConfig::from_env_vars(Some("disabled".to_string()), None)
                .unwrap()
                .mode,
            AuthMode::Disabled
        );
    }

    #[test]
    fn auth_config_recognizes_token_mode() {
        assert_eq!(
            AuthConfig::from_env_vars(Some("token".to_string()), None)
                .unwrap()
                .mode,
            AuthMode::Token
        );
    }

    #[test]
    fn auth_config_rejects_unknown_mode() {
        let error = AuthConfig::from_env_vars(Some("jwt".to_string()), None)
            .expect_err("unknown auth mode should fail");
        assert!(error.to_string().contains("unknown AIONCORE_AUTH_MODE"));
    }

    #[test]
    fn auth_config_rejects_short_bootstrap_admin_token() {
        let error = AuthConfig::from_env_vars(
            Some("token".to_string()),
            Some("short-bootstrap-token".to_string()),
        )
        .expect_err("short bootstrap token should fail");
        assert!(error
            .to_string()
            .contains("AIONCORE_BOOTSTRAP_ADMIN_TOKEN must be at least 24 characters long"));
    }

    #[test]
    fn token_auth_mode_initializes_successfully() {
        let state = AppState::from_config_and_auth(
            StorageBackendConfig::Memory,
            AuthConfig {
                mode: AuthMode::Token,
                bootstrap_admin_token_hash: None,
            },
        )
        .expect("token auth mode should initialize");
        assert_eq!(state.auth.mode, AuthMode::Token);
    }

    #[tokio::test]
    #[ignore]
    async fn postgres_runtime_backend_reports_postgres_storage_when_configured() {
        let Some(database_url) = std::env::var("AIONCORE_TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return;
        };

        let state = AppState::from_config(StorageBackendConfig::Postgres { database_url })
            .expect("postgres state should initialize");
        let app = app_with_state(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body["storage"], "postgres");
    }

    #[tokio::test]
    #[ignore]
    async fn postgres_runtime_end_to_end_validation() {
        let Some(database_url) = std::env::var("AIONCORE_TEST_DATABASE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return;
        };

        let state = AppState::from_config(StorageBackendConfig::Postgres { database_url })
            .expect("postgres state should initialize");
        let app = app_with_state(state);

        let ready_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ready_response.status(), StatusCode::OK);
        let ready = to_json(ready_response).await;
        assert_eq!(ready["ready"], true);
        assert_eq!(ready["storage"], "postgres");

        let tank = create_test_entity(&app, "runtime-tank-01", "aion:WaterTank").await;
        let pump = create_test_entity(&app, "runtime-pump-01", "aion:Pump").await;

        let relationship_response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/relationships",
                json!({
                    "source_entity_id": tank,
                    "relationship_type": "feeds",
                    "target_entity_id": pump,
                    "jsonld": {}
                }),
            ))
            .await
            .unwrap();
        assert_eq!(relationship_response.status(), StatusCode::CREATED);

        let ingest_response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": tank,
                    "feature_of_interest_id": pump,
                    "payload_format": "senml-json",
                    "protocol": "http",
                    "content_type": "application/senml+json",
                    "observed_at": "2026-04-27T13:00:00Z",
                    "payload": {
                        "e": [{
                            "n": "WaterTankLevel",
                            "v": 12,
                            "u": "%"
                        }]
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(ingest_response.status(), StatusCode::OK);

        let observations_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/observations?feature_of_interest_id={pump}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(observations_response.status(), StatusCode::OK);
        let observations = to_json(observations_response).await;
        assert!(!observations.as_array().unwrap().is_empty());

        let policy_response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                "/policies",
                json!([{
                    "target_entity_id": pump,
                    "command_type": "StartPump",
                    "requires_approval": false,
                    "auto_execute_allowed": false,
                    "metadata": {"source": "runtime-e2e"}
                }]),
            ))
            .await
            .unwrap();
        assert_eq!(policy_response.status(), StatusCode::OK);

        let command_response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/commands",
                json!({
                    "target_entity_id": pump,
                    "command_type": "StartPump",
                    "payload": {"target_state": "running"},
                    "requested_by": "runtime-test",
                    "reason": "runtime e2e"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(command_response.status(), StatusCode::CREATED);
        let command = to_json(command_response).await;
        let command_id = command["id"].as_str().unwrap().to_string();

        let command_lookup = app
            .oneshot(
                Request::builder()
                    .uri(format!("/commands/{command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(command_lookup.status(), StatusCode::OK);
        assert_eq!(to_json(command_lookup).await["id"], command_id);
    }

    #[tokio::test]
    async fn creates_entity_from_envelope_and_returns_context() {
        let app = app();
        let entity_body = json!({
            "entity_key": "sensor-01",
            "entity_type": "aion:Sensor",
            "jsonld": {
                "@context": {"aion": "https://aioncore.org/ns#"},
                "@id": "urn:aion:sensor:sensor-01",
                "@type": "aion:Sensor",
                "name": "Sensor 01"
            }
        });

        let response = app
            .clone()
            .oneshot(json_request("POST", "/entities", entity_body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let entity = to_json(response).await;
        let entity_id = entity["id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/entities/{entity_id}/context"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        assert_eq!(context["entity"]["entity_key"], "sensor-01");
        assert_eq!(
            context["outgoing_relationships"].as_array().unwrap().len(),
            0
        );
        assert_eq!(
            context["incoming_relationships"].as_array().unwrap().len(),
            0
        );
    }

    #[tokio::test]
    async fn creates_entity_from_native_jsonld() {
        let response = app()
            .oneshot(json_ld_request(
                "POST",
                "/entities",
                json!({
                    "@context": {"aion": "https://aioncore.org/ns#"},
                    "@id": "urn:aion:sensor:sensor-ld-01",
                    "@type": "aion:Sensor",
                    "name": "Sensor LD 01"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let entity = to_json(response).await;
        assert_eq!(entity["entity_key"], "sensor-ld-01");
        assert_eq!(entity["entity_type"], "aion:Sensor");
        assert_eq!(entity["jsonld"]["@id"], "urn:aion:sensor:sensor-ld-01");
        assert_eq!(entity["jsonld"]["name"], "Sensor LD 01");
    }

    #[test]
    fn derives_entity_key_from_native_jsonld_fields_first() {
        let explicit = json!({
            "entity_key": "explicit-zone-key",
            "aion:entityKey": "semantic-zone-key"
        });
        assert_eq!(
            routes::entities::extract_jsonld_entity_key(explicit.as_object().unwrap()).as_deref(),
            Some("explicit-zone-key")
        );

        let semantic = json!({
            "aion:entityKey": "semantic-zone-key"
        });
        assert_eq!(
            routes::entities::extract_jsonld_entity_key(semantic.as_object().unwrap()).as_deref(),
            Some("semantic-zone-key")
        );
    }

    #[test]
    fn derives_semantic_entity_key_from_jsonld_id() {
        assert_eq!(
            routes::entities::derive_entity_key("urn:aion:farm:01:zone:01").as_deref(),
            Some("zone-01")
        );
        assert_eq!(
            routes::entities::derive_entity_key("urn:aion:farm:01:soil-sensor:01").as_deref(),
            Some("soil-sensor-01")
        );
        assert_eq!(
            routes::entities::derive_entity_key("urn:aion:sensor:runtime-jsonld-01").as_deref(),
            Some("runtime-jsonld-01")
        );
    }

    #[tokio::test]
    async fn creates_native_jsonld_entities_with_numeric_suffixes_without_conflict() {
        let app = app();

        let zone_response = app
            .clone()
            .oneshot(json_ld_request(
                "POST",
                "/entities",
                json!({
                    "@context": {"aion": "https://aioncore.org/ns#"},
                    "@id": "urn:aion:farm:01:zone:01",
                    "@type": "aion:IrrigationZone",
                    "name": "Zone 01"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(zone_response.status(), StatusCode::CREATED);
        let zone = to_json(zone_response).await;
        assert_eq!(zone["entity_key"], "zone-01");

        let sensor_response = app
            .oneshot(json_ld_request(
                "POST",
                "/entities",
                json!({
                    "@context": {"aion": "https://aioncore.org/ns#"},
                    "@id": "urn:aion:farm:01:soil-sensor:01",
                    "@type": "aion:SoilSensor",
                    "name": "Soil Sensor 01"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(sensor_response.status(), StatusCode::CREATED);
        let sensor = to_json(sensor_response).await;
        assert_eq!(sensor["entity_key"], "soil-sensor-01");
    }

    #[tokio::test]
    async fn creates_and_queries_observation() {
        let app = app();
        let sensor_id = create_test_entity(&app, "sensor-01", "aion:Sensor").await;
        let room_id = create_test_entity(&app, "room-01", "aion:Room").await;

        let observation_body = json!({
            "producer_entity_id": sensor_id,
            "feature_of_interest_id": room_id,
            "observed_property": "temperature",
            "value": {"type": "number", "value": 21.4},
            "unit": "Cel",
            "observed_at": "2026-04-27T13:00:00Z",
            "received_at": "2026-04-27T13:00:01Z",
            "protocol": "http",
            "payload_format": "json_mapping",
            "quality": {},
            "metadata": {}
        });

        let response = app
            .clone()
            .oneshot(json_request("POST", "/observations", observation_body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/observations?feature_of_interest_id={room_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let observations = to_json(response).await;
        assert_eq!(observations.as_array().unwrap().len(), 1);
        assert_eq!(observations[0]["observed_property"], "temperature");
    }

    #[tokio::test]
    async fn creates_payload_profile() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;

        let response = app
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{sensor_id}/payload-profile"),
                json!({
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "attribute_mapping": {
                        "m": {
                            "observed_property": "aion:SoilMoisture",
                            "unit": "%"
                        }
                    },
                    "metadata": {
                        "profile_version": 1
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let profile = to_json(response).await;
        assert_eq!(profile["entity_id"], sensor_id);
        assert_eq!(profile["payload_format"], "ultralight");
        assert_eq!(
            profile["attribute_mapping"]["m"]["observed_property"],
            "aion:SoilMoisture"
        );
    }

    #[tokio::test]
    async fn retrieves_payload_profile() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{sensor_id}/payload-profile"),
                json!({
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "attribute_mapping": {
                        "t": {
                            "observed_property": "aion:SoilTemperature",
                            "unit": "Cel"
                        }
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/entities/{sensor_id}/payload-profile"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let profile = to_json(response).await;
        assert_eq!(profile["entity_id"], sensor_id);
        assert_eq!(
            profile["attribute_mapping"]["t"]["observed_property"],
            "aion:SoilTemperature"
        );
    }

    #[tokio::test]
    async fn manages_capabilities_commands_actions_and_results() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-01", "aion:Pump").await;
        let executor_id = create_test_entity(&app, "executor-01", "aion:Executor").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{pump_id}/capabilities"),
                json!([
                    {
                        "capability_name": "StartPump",
                        "command_type": "StartPump",
                        "protocol": "http",
                        "metadata": {
                            "description": "Start pump motor"
                        }
                    },
                    {
                        "capability_name": "StopPump",
                        "command_type": "StopPump",
                        "protocol": "http"
                    }
                ]),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let capabilities = to_json(response).await;
        assert_eq!(capabilities.as_array().unwrap().len(), 2);
        assert_eq!(capabilities[0]["entity_id"], pump_id);
        assert_eq!(capabilities[0]["capability_name"], "StartPump");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/entities/{pump_id}/capabilities"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let capabilities = to_json(response).await;
        assert_eq!(capabilities.as_array().unwrap().len(), 2);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/commands",
                json!({
                    "target_entity_id": pump_id,
                    "command_type": "StartPump",
                    "payload": {
                        "target_state": "running"
                    },
                    "requested_by": "operator@example.com",
                    "reason": "water tank below minimum level"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let command = to_json(response).await;
        let command_id = command["id"].as_str().unwrap();
        assert_eq!(command["target_entity_id"], pump_id);
        assert_eq!(command["status"], "pending");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/commands/{command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["id"], command_id);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/commands?target_entity_id={pump_id}&status=pending"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let commands = to_json(response).await;
        assert_eq!(commands.as_array().unwrap().len(), 1);
        assert_eq!(commands[0]["id"], command_id);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/actions",
                json!({
                    "command_id": command_id,
                    "executor_entity_id": executor_id,
                    "action_type": "StartPump",
                    "status": "started",
                    "started_at": "2026-04-27T13:00:00Z",
                    "metadata": {
                        "external_correlation_id": "exec-001"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let action = to_json(response).await;
        let action_id = action["id"].as_str().unwrap();
        assert_eq!(action["command_id"], command_id);
        assert_eq!(action["executor_entity_id"], executor_id);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/actions/{action_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["id"], action_id);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/actions?command_id={command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let actions = to_json(response).await;
        assert_eq!(actions.as_array().unwrap().len(), 1);
        assert_eq!(actions[0]["command_id"], command_id);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/action-results",
                json!({
                    "command_id": command_id,
                    "action_id": action_id,
                    "status": "succeeded",
                    "verified": true,
                    "result_payload": {
                        "pump_state": "running"
                    },
                    "observed_at": "2026-04-27T13:00:05Z",
                    "metadata": {
                        "verification_source": "simulated_executor"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let result = to_json(response).await;
        assert_eq!(result["command_id"], command_id);
        assert_eq!(result["action_id"], action_id);
        assert_eq!(result["verified"], true);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/action-results?action_id={action_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let results = to_json(response).await;
        assert_eq!(results.as_array().unwrap().len(), 1);
        assert_eq!(results[0]["action_id"], action_id);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/action-results?command_id={command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let results = to_json(response).await;
        assert_eq!(results.as_array().unwrap().len(), 1);
        assert_eq!(results[0]["command_id"], command_id);
        assert_eq!(results[0]["action_id"], action_id);
    }

    #[tokio::test]
    async fn claims_pending_command_and_rejects_second_claim() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-claim-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({"claimed_by": "executor-01"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "claimed");
        assert_eq!(command["claimed_by"], "executor-01");

        let response = app
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({"claimed_by": "executor-02"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(to_json(response).await["error"]
            .as_str()
            .unwrap()
            .contains("only be claimed when status is pending"));
    }

    #[tokio::test]
    async fn releases_claimed_command_back_to_pending() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-release-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        claim_test_command(&app, command_id, "executor-01").await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/release"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "pending");
        assert!(command["claimed_by"].is_null());
        assert!(command["claimed_at"].is_null());
    }

    #[tokio::test]
    async fn marks_claimed_command_executed() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-executed-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        claim_test_command(&app, command_id, "executor-01").await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/mark-executed"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "executed");
        assert!(command["completed_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn marks_claimed_command_failed() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-failed-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        claim_test_command(&app, command_id, "executor-01").await;

        let response = app
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/mark-failed"),
                json!({"failure_reason": "controller timeout"}),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "failed");
        assert_eq!(command["failure_reason"], "controller timeout");
        assert!(command["completed_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn cancels_pending_command() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-cancel-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let command = to_json(response).await;
        assert_eq!(command["status"], "cancelled");
        assert!(command["completed_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn policy_requires_approval_before_claim() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-policy-01", "aion:Pump").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                "/policies",
                json!([
                    {
                        "target_entity_id": pump_id,
                        "command_type": "StartPump",
                        "requires_approval": true,
                        "auto_execute_allowed": false,
                        "metadata": {
                            "reason": "physical actuation"
                        }
                    }
                ]),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let policies = to_json(response).await;
        assert_eq!(policies.as_array().unwrap().len(), 1);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/policies?target_entity_id={pump_id}&command_type=StartPump"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await.as_array().unwrap().len(), 1);

        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        assert_eq!(command["approval_status"], "required");
        assert_eq!(command["policy_decision"]["requires_approval"], true);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({"claimed_by": "executor-01"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(to_json(response).await["error"]
            .as_str()
            .unwrap()
            .contains("requires approval"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/approve"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["approval_status"], "approved");

        let claimed = claim_test_command(&app, command_id, "executor-01").await;
        assert_eq!(claimed["status"], "claimed");
    }

    #[tokio::test]
    async fn rejected_command_cannot_be_claimed() {
        let app = app();
        let pump_id = create_test_entity(&app, "pump-rejected-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/reject"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["approval_status"], "rejected");

        let response = app
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({"claimed_by": "executor-01"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(to_json(response).await["error"]
            .as_str()
            .unwrap()
            .contains("rejected"));
    }

    #[tokio::test]
    async fn creates_retrieves_and_filters_events() {
        let app = app();
        let source_id = create_test_entity(&app, "event-source-01", "aion:Sensor").await;
        let target_id = create_test_entity(&app, "event-target-01", "aion:Pump").await;
        let command = create_test_command(&app, &target_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/events",
                json!({
                    "event_type": "aion:ManualAuditEvent",
                    "severity": "warning",
                    "source_entity_id": source_id,
                    "target_entity_id": target_id,
                    "message": "Manual audit event",
                    "occurred_at": "2026-04-27T13:00:00Z",
                    "correlation_id": "manual-event-001",
                    "command_id": command_id,
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let event = to_json(response).await;
        let event_id = event["id"].as_str().unwrap();
        assert_eq!(event["event_type"], "aion:ManualAuditEvent");
        assert_eq!(event["severity"], "warning");
        assert_eq!(event["target_entity_id"], target_id);
        assert_eq!(event["command_id"], command_id);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/events/{event_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_json(response).await["id"], event_id);

        for uri in [
            format!("/events?target_entity_id={target_id}"),
            "/events?event_type=aion:ManualAuditEvent".to_string(),
            "/events?severity=warning".to_string(),
            format!("/events?command_id={command_id}"),
            "/events?correlation_id=manual-event-001".to_string(),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let events = to_json(response).await;
            assert!(events
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["id"] == event_id));
        }
    }

    #[tokio::test]
    async fn ingestion_success_creates_payload_ingested_event() {
        let app = app();
        let sensor_id = create_test_entity(&app, "event-soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "event-plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/events?raw_message_id={raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let events = to_json(response).await;
        assert_eq!(events.as_array().unwrap().len(), 1);
        assert_eq!(events[0]["event_type"], "aion:PayloadIngested");
        assert_eq!(events[0]["severity"], "info");
        assert_eq!(events[0]["raw_message_id"], raw_message_id);
        assert_eq!(events[0]["source_entity_id"], sensor_id);
        assert_eq!(events[0]["target_entity_id"], plot_id);
    }

    #[tokio::test]
    async fn command_lifecycle_transitions_create_events() {
        let app = app();
        let pump_id = create_test_entity(&app, "event-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/approve"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        claim_test_command(&app, command_id, "executor-01").await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/mark-executed"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/events?command_id={command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let events = to_json(response).await;
        let event_types = events
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["event_type"].as_str().unwrap())
            .collect::<Vec<_>>();

        assert!(event_types.contains(&"aion:CommandCreated"));
        assert!(event_types.contains(&"aion:CommandApproved"));
        assert!(event_types.contains(&"aion:CommandClaimed"));
        assert!(event_types.contains(&"aion:CommandExecuted"));
    }

    #[tokio::test]
    async fn builds_ai_context_for_entity_with_relationships_only() {
        let app = app();
        let tank_id = create_test_entity(&app, "context-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "context-pump-01", "aion:Pump").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/relationships",
                json!({
                    "source_entity_id": pump_id,
                    "relationship_type": "aion:fills",
                    "target_entity_id": tank_id,
                    "jsonld": {
                        "@type": "aion:Relationship"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{tank_id}?include_observations=false&include_events=false&include_commands=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        assert_eq!(context["target_entity"]["id"], tank_id);
        assert_eq!(
            context["incoming_relationships"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            context["outgoing_relationships"].as_array().unwrap().len(),
            0
        );
        assert_eq!(context["recent_observations"].as_array().unwrap().len(), 0);
        assert_eq!(context["recent_events"].as_array().unwrap().len(), 0);
        assert_eq!(context["related_commands"].as_array().unwrap().len(), 0);
        assert_eq!(context["metadata"]["llm_invoked"], false);
    }

    #[tokio::test]
    async fn builds_ai_context_with_observations() {
        let app = app();
        let sensor_id = create_test_entity(&app, "context-level-sensor-01", "aion:Sensor").await;
        let tank_id = create_test_entity(&app, "context-tank-02", "aion:WaterTank").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/observations",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": tank_id,
                    "observed_property": "water_level",
                    "value": {
                        "type": "number",
                        "value": 42.0
                    },
                    "unit": "%",
                    "observed_at": "2026-04-27T13:00:00Z",
                    "received_at": "2026-04-27T13:00:01Z",
                    "protocol": "http",
                    "payload_format": "json_mapping",
                    "quality": {},
                    "metadata": {}
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{tank_id}?include_events=false&include_commands=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        let observations = context["recent_observations"].as_array().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0]["feature_of_interest_id"], tank_id);
        assert_eq!(observations[0]["observed_property"], "water_level");
    }

    #[tokio::test]
    async fn builds_ai_context_with_events() {
        let app = app();
        let tank_id = create_test_entity(&app, "context-tank-03", "aion:WaterTank").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/events",
                json!({
                    "event_type": "aion:LowWaterLevel",
                    "severity": "warning",
                    "target_entity_id": tank_id,
                    "message": "Water level is below threshold",
                    "occurred_at": "2026-04-27T13:00:00Z",
                    "metadata": {
                        "threshold": 30
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{tank_id}?include_observations=false&include_commands=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        let events = context["recent_events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_type"], "aion:LowWaterLevel");
        assert_eq!(events[0]["target_entity_id"], tank_id);
    }

    #[tokio::test]
    async fn builds_ai_context_with_command_action_result_history() {
        let app = app();
        let pump_id = create_test_entity(&app, "context-pump-02", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        claim_test_command(&app, command_id, "executor-01").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/actions",
                json!({
                    "command_id": command_id,
                    "executor_entity_id": pump_id,
                    "action_type": "StartPump",
                    "status": "started",
                    "started_at": "2026-04-27T13:01:00Z",
                    "metadata": {
                        "executor": "test"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let action = to_json(response).await;
        let action_id = action["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/action-results",
                json!({
                    "command_id": command_id,
                    "action_id": action_id,
                    "status": "succeeded",
                    "verified": true,
                    "result_payload": {
                        "pump_state": "running"
                    },
                    "observed_at": "2026-04-27T13:01:30Z",
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{pump_id}?include_observations=false&include_events=false&include_commands=true"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        assert_eq!(context["related_commands"].as_array().unwrap().len(), 1);
        assert_eq!(context["related_commands"][0]["id"], command_id);
        assert_eq!(context["related_actions"].as_array().unwrap().len(), 1);
        assert_eq!(context["related_actions"][0]["command_id"], command_id);
        assert_eq!(
            context["related_action_results"].as_array().unwrap().len(),
            1
        );
        assert_eq!(
            context["related_action_results"][0]["command_id"],
            command_id
        );
        assert_eq!(context["related_action_results"][0]["action_id"], action_id);
    }

    #[tokio::test]
    async fn ai_context_limit_is_respected() {
        let app = app();
        let sensor_id = create_test_entity(&app, "context-level-sensor-02", "aion:Sensor").await;
        let tank_id = create_test_entity(&app, "context-tank-04", "aion:WaterTank").await;

        for (observed_at, value) in [
            ("2026-04-27T13:00:00Z", 41.0),
            ("2026-04-27T13:05:00Z", 39.5),
        ] {
            let response = app
                .clone()
                .oneshot(json_request(
                    "POST",
                    "/observations",
                    json!({
                        "producer_entity_id": sensor_id,
                        "feature_of_interest_id": tank_id,
                        "observed_property": "water_level",
                        "value": {
                            "type": "number",
                            "value": value
                        },
                        "unit": "%",
                        "observed_at": observed_at,
                        "received_at": observed_at,
                        "protocol": "http",
                        "payload_format": "json_mapping",
                        "quality": {},
                        "metadata": {}
                    }),
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ai/context/entity/{tank_id}?limit=1&include_events=false&include_commands=false"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let context = to_json(response).await;
        let observations = context["recent_observations"].as_array().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0]["value"]["value"], 39.5);
    }

    #[tokio::test]
    async fn lists_mcp_tool_definitions() {
        let app = app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/mcp/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tools = to_json(response).await;
        let tool_names = tools
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>();

        assert!(tool_names.contains(&"list_entities"));
        assert!(tool_names.contains(&"get_entity"));
        assert!(tool_names.contains(&"build_ai_context"));
    }

    #[tokio::test]
    async fn invokes_mcp_list_entities() {
        let app = app();
        let tank_id = create_test_entity(&app, "mcp-tank-01", "aion:WaterTank").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/list_entities",
                json!({
                    "arguments": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tool_response = to_json(response).await;
        assert!(tool_response["error"].is_null());
        assert!(tool_response["result"]["content"]["entities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entity| entity["id"] == tank_id
                && entity["entity_key"] == "mcp-tank-01"
                && entity["entity_type"] == "aion:WaterTank"));
    }

    #[tokio::test]
    async fn invokes_mcp_get_entity() {
        let app = app();
        let pump_id = create_test_entity(&app, "mcp-pump-01", "aion:Pump").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/get_entity",
                json!({
                    "arguments": {
                        "entity_id": pump_id
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tool_response = to_json(response).await;
        assert_eq!(tool_response["result"]["content"]["entity"]["id"], pump_id);
        assert_eq!(
            tool_response["result"]["content"]["entity"]["entity_type"],
            "aion:Pump"
        );
    }

    #[tokio::test]
    async fn invokes_mcp_build_ai_context() {
        let app = app();
        let tank_id = create_test_entity(&app, "mcp-context-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "mcp-context-pump-01", "aion:Pump").await;
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/relationships",
                json!({
                    "source_entity_id": pump_id,
                    "relationship_type": "aion:fills",
                    "target_entity_id": tank_id,
                    "jsonld": {
                        "@type": "aion:Relationship"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/build_ai_context",
                json!({
                    "arguments": {
                        "entity_id": tank_id,
                        "include_observations": false,
                        "include_events": false,
                        "include_commands": false,
                        "limit": 5
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let tool_response = to_json(response).await;
        let context = &tool_response["result"]["content"]["context"];
        assert_eq!(context["target_entity"]["id"], tank_id);
        assert_eq!(
            context["incoming_relationships"].as_array().unwrap().len(),
            1
        );
        assert_eq!(context["metadata"]["llm_invoked"], false);
    }

    #[tokio::test]
    async fn mcp_invalid_tool_name_returns_clear_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/no_such_tool",
                json!({
                    "arguments": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let tool_response = to_json(response).await;
        assert!(tool_response["result"].is_null());
        assert_eq!(tool_response["error"]["code"], "not_found");
        assert!(tool_response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unknown MCP tool"));
    }

    #[tokio::test]
    async fn mcp_missing_required_tool_argument_returns_clear_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp/tools/get_entity",
                json!({
                    "arguments": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let tool_response = to_json(response).await;
        assert!(tool_response["result"].is_null());
        assert_eq!(tool_response["error"]["code"], "missing_argument");
        assert_eq!(tool_response["error"]["message"], "entity_id is required");
    }

    #[tokio::test]
    async fn mcp_json_rpc_tools_list_returns_tool_definitions() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/list",
                    "params": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        let tools = response["result"]["tools"].as_array().unwrap();
        assert!(tools.iter().any(|tool| tool["name"] == "list_entities"
            && tool["inputSchema"]["additionalProperties"] == false));
        assert!(tools.iter().any(|tool| tool["name"] == "build_ai_context"
            && tool["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("entity_id"))));
    }

    #[tokio::test]
    async fn mcp_json_rpc_tools_call_build_ai_context_works() {
        let app = app();
        let tank_id = create_test_entity(&app, "json-rpc-tank-01", "aion:WaterTank").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "build_ai_context",
                        "arguments": {
                            "entity_id": tank_id,
                            "include_observations": false,
                            "include_events": false,
                            "include_commands": false
                        }
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["id"], 2);
        assert_eq!(response["result"]["isError"], false);
        assert_eq!(
            response["result"]["structuredContent"]["context"]["target_entity"]["id"],
            tank_id
        );
        let text = response["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("json-rpc-tank-01"));
    }

    #[tokio::test]
    async fn mcp_json_rpc_tools_call_list_entities_works() {
        let app = app();
        let entity_id = create_test_entity(&app, "json-rpc-entity-01", "aion:Sensor").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": "list-entities",
                    "method": "tools/call",
                    "params": {
                        "name": "list_entities",
                        "arguments": {}
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["id"], "list-entities");
        assert!(response["result"]["structuredContent"]["entities"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |entity| entity["id"] == entity_id && entity["entity_key"] == "json-rpc-entity-01"
            ));
    }

    #[tokio::test]
    async fn mcp_json_rpc_unknown_method_returns_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "resources/list",
                    "params": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["error"]["code"], -32601);
        assert!(response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unknown JSON-RPC method"));
    }

    #[tokio::test]
    async fn mcp_json_rpc_unknown_tool_returns_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "tools/call",
                    "params": {
                        "name": "no_such_tool",
                        "arguments": {}
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["error"]["code"], -32602);
        assert_eq!(response["error"]["data"]["code"], "not_found");
        assert_eq!(response["error"]["data"]["isError"], true);
    }

    #[tokio::test]
    async fn mcp_json_rpc_missing_required_tool_argument_returns_error() {
        let app = app();

        let response = app
            .oneshot(json_request(
                "POST",
                "/mcp",
                json!({
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "tools/call",
                    "params": {
                        "name": "build_ai_context",
                        "arguments": {}
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["error"]["code"], -32602);
        assert_eq!(response["error"]["data"]["code"], "missing_argument");
        assert_eq!(response["error"]["message"], "entity_id is required");
    }

    #[tokio::test]
    async fn mcp_json_rpc_malformed_request_returns_error() {
        let app = app();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from("{not-json"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let response = to_json(response).await;
        assert_eq!(response["jsonrpc"], "2.0");
        assert!(response["id"].is_null());
        assert_eq!(response["error"]["code"], -32700);
    }

    #[tokio::test]
    async fn creates_and_retrieves_rule() {
        let app = app();
        let tank_id = create_test_entity(&app, "rule-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "rule-pump-01", "aion:Pump").await;

        let rule = create_low_water_command_rule(&app, &tank_id, &pump_id, true, 20.0).await;
        let rule_id = rule["id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/rules/{rule_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let fetched = to_json(response).await;
        assert_eq!(fetched["id"], rule_id);
        assert_eq!(fetched["trigger_type"], "observation_created");
        assert_eq!(fetched["action"]["type"], "create_command");
    }

    #[tokio::test]
    async fn disabled_rule_does_not_run() {
        let app = app();
        let tank_id = create_test_entity(&app, "rule-disabled-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "rule-disabled-pump-01", "aion:Pump").await;
        create_low_water_command_rule(&app, &tank_id, &pump_id, false, 20.0).await;

        create_water_level_observation(&app, &tank_id, 12.0).await;

        let commands = query_pending_commands(&app, &pump_id).await;
        assert!(commands.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn observation_rule_with_less_than_condition_matches() {
        let app = app();
        let tank_id = create_test_entity(&app, "rule-match-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "rule-match-pump-01", "aion:Pump").await;
        create_low_water_command_rule(&app, &tank_id, &pump_id, true, 20.0).await;

        create_water_level_observation(&app, &tank_id, 12.0).await;

        let commands = query_pending_commands(&app, &pump_id).await;
        assert_eq!(commands.as_array().unwrap().len(), 1);
        assert_eq!(commands[0]["command_type"], "StartPump");
    }

    #[tokio::test]
    async fn observation_rule_with_less_than_condition_does_not_match() {
        let app = app();
        let tank_id = create_test_entity(&app, "rule-no-match-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "rule-no-match-pump-01", "aion:Pump").await;
        create_low_water_command_rule(&app, &tank_id, &pump_id, true, 20.0).await;

        create_water_level_observation(&app, &tank_id, 42.0).await;

        let commands = query_pending_commands(&app, &pump_id).await;
        assert!(commands.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn matched_observation_rule_creates_command() {
        let app = app();
        let tank_id = create_test_entity(&app, "rule-command-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "rule-command-pump-01", "aion:Pump").await;
        let rule = create_low_water_command_rule(&app, &tank_id, &pump_id, true, 20.0).await;

        let observation = create_water_level_observation(&app, &tank_id, 12.0).await;

        let commands = query_pending_commands(&app, &pump_id).await;
        assert_eq!(commands.as_array().unwrap().len(), 1);
        assert_eq!(commands[0]["payload"]["rule_id"], rule["id"]);
        assert_eq!(commands[0]["payload"]["observation_id"], observation["id"]);
    }

    #[tokio::test]
    async fn matched_observation_rule_creates_event() {
        let app = app();
        let tank_id = create_test_entity(&app, "rule-event-tank-01", "aion:WaterTank").await;
        let rule = create_low_water_event_rule(&app, &tank_id, true, 20.0).await;

        create_water_level_observation(&app, &tank_id, 12.0).await;

        let events = query_events_by_type(&app, "aion:LowWaterLevel").await;
        assert_eq!(events.as_array().unwrap().len(), 1);
        assert_eq!(events[0]["target_entity_id"], tank_id);
        assert_eq!(events[0]["metadata"]["rule_id"], rule["id"]);
    }

    #[tokio::test]
    async fn event_triggered_rule_creates_command() {
        let app = app();
        let tank_id =
            create_test_entity(&app, "rule-event-command-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "rule-event-command-pump-01", "aion:Pump").await;
        create_event_command_rule(&app, &tank_id, &pump_id).await;

        create_test_event(&app, "aion:LowWaterLevel", Some(&tank_id), json!({})).await;

        let commands = query_pending_commands(&app, &pump_id).await;
        assert_eq!(commands.as_array().unwrap().len(), 1);
        assert_eq!(commands[0]["command_type"], "StartPump");
    }

    #[tokio::test]
    async fn generated_commands_preserve_policy_behavior() {
        let app = app();
        let tank_id = create_test_entity(&app, "rule-policy-tank-01", "aion:WaterTank").await;
        let pump_id = create_test_entity(&app, "rule-policy-pump-01", "aion:Pump").await;
        put_start_pump_policy(&app, &pump_id, true).await;
        create_low_water_command_rule(&app, &tank_id, &pump_id, true, 20.0).await;

        create_water_level_observation(&app, &tank_id, 12.0).await;

        let commands = query_pending_commands(&app, &pump_id).await;
        let command_id = commands[0]["id"].as_str().unwrap();
        assert_eq!(commands[0]["approval_status"], "required");
        assert_eq!(commands[0]["policy_decision"]["requires_approval"], true);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({"claimed_by": "executor-01"}),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/approve"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let claimed = claim_test_command(&app, command_id, "executor-01").await;
        assert_eq!(claimed["status"], "claimed");
    }

    #[tokio::test]
    async fn rule_generated_events_include_rule_id_metadata() {
        let app = app();
        let tank_id = create_test_entity(&app, "rule-meta-tank-01", "aion:WaterTank").await;
        let rule = create_low_water_event_rule(&app, &tank_id, true, 20.0).await;

        create_water_level_observation(&app, &tank_id, 12.0).await;

        let events = query_events_by_type(&app, "aion:LowWaterLevel").await;
        assert_eq!(events[0]["metadata"]["source"], "rule_engine");
        assert_eq!(events[0]["metadata"]["rule_generated"], true);
        assert_eq!(events[0]["metadata"]["rule_id"], rule["id"]);
    }

    #[tokio::test]
    async fn no_recursive_event_loop_occurs() {
        let app = app();
        let tank_id = create_test_entity(&app, "rule-loop-tank-01", "aion:WaterTank").await;
        create_loop_event_rule(&app, &tank_id).await;

        create_test_event(&app, "aion:Loop", Some(&tank_id), json!({})).await;

        let events = query_events_by_type(&app, "aion:Loop").await;
        assert_eq!(events.as_array().unwrap().len(), 2);
        assert_eq!(
            events
                .as_array()
                .unwrap()
                .iter()
                .filter(|event| event["metadata"]["rule_generated"] == true)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn registers_executor() {
        let app = app();

        let executor = create_test_executor(&app, "edge-agent-01").await;

        assert_eq!(executor["agent_key"], "edge-agent-01");
        assert_eq!(executor["agent_type"], "edge");
        assert_eq!(executor["status"], "online");
    }

    #[tokio::test]
    async fn sets_executor_capabilities() {
        let app = app();
        let executor = create_test_executor(&app, "edge-agent-cap-01").await;
        let executor_id = executor["id"].as_str().unwrap();

        let capabilities = put_executor_capabilities(&app, executor_id, &["StartPump"]).await;

        assert_eq!(capabilities.as_array().unwrap().len(), 1);
        assert_eq!(capabilities[0]["command_type"], "StartPump");
    }

    #[tokio::test]
    async fn sets_executor_scopes() {
        let app = app();
        let pump_id = create_test_entity(&app, "executor-scope-pump-01", "aion:Pump").await;
        let executor = create_test_executor(&app, "edge-agent-scope-01").await;
        let executor_id = executor["id"].as_str().unwrap();

        let scopes = put_executor_scope_for_target(&app, executor_id, &pump_id).await;

        assert_eq!(scopes.as_array().unwrap().len(), 1);
        assert_eq!(scopes[0]["target_entity_id"], pump_id);
    }

    #[tokio::test]
    async fn polling_returns_compatible_pending_commands() {
        let app = app();
        let pump_id = create_test_entity(&app, "executor-compatible-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let executor = create_test_executor(&app, "edge-agent-compatible-01").await;
        let executor_id = executor["id"].as_str().unwrap();
        put_executor_capabilities(&app, executor_id, &["StartPump"]).await;
        put_executor_scope_for_target(&app, executor_id, &pump_id).await;

        let commands = poll_executor_commands(&app, executor_id).await;

        assert!(commands
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == command["id"]));
    }

    #[tokio::test]
    async fn polling_does_not_return_incompatible_command_type() {
        let app = app();
        let pump_id = create_test_entity(&app, "executor-type-pump-01", "aion:Pump").await;
        create_test_command(&app, &pump_id, "StopPump").await;
        let executor = create_test_executor(&app, "edge-agent-type-01").await;
        let executor_id = executor["id"].as_str().unwrap();
        put_executor_capabilities(&app, executor_id, &["StartPump"]).await;
        put_executor_scope_for_target(&app, executor_id, &pump_id).await;

        let commands = poll_executor_commands(&app, executor_id).await;

        assert!(commands.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn polling_does_not_return_out_of_scope_target_entity() {
        let app = app();
        let pump_id = create_test_entity(&app, "executor-out-pump-01", "aion:Pump").await;
        let other_pump_id = create_test_entity(&app, "executor-out-pump-02", "aion:Pump").await;
        create_test_command(&app, &pump_id, "StartPump").await;
        let executor = create_test_executor(&app, "edge-agent-out-01").await;
        let executor_id = executor["id"].as_str().unwrap();
        put_executor_capabilities(&app, executor_id, &["StartPump"]).await;
        put_executor_scope_for_target(&app, executor_id, &other_pump_id).await;

        let commands = poll_executor_commands(&app, executor_id).await;

        assert!(commands.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn executor_claim_blocked_if_approval_required() {
        let app = app();
        let pump_id = create_test_entity(&app, "executor-approval-pump-01", "aion:Pump").await;
        put_start_pump_policy(&app, &pump_id, true).await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let executor = create_compatible_executor(&app, "edge-agent-approval-01", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/executors/{executor_id}/commands/{}/claim",
                        command["id"].as_str().unwrap()
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn approved_command_can_be_claimed_by_compatible_executor() {
        let app = app();
        let pump_id = create_test_entity(&app, "executor-claim-pump-01", "aion:Pump").await;
        put_start_pump_policy(&app, &pump_id, true).await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "edge-agent-claim-01", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        approve_test_command(&app, command_id).await;

        let claimed = claim_executor_test_command(&app, executor_id, command_id).await;

        assert_eq!(claimed["status"], "claimed");
        assert_eq!(claimed["claimed_by"], "edge-agent-claim-01");
    }

    #[tokio::test]
    async fn complete_command_creates_action_result_and_event() {
        let app = app();
        let pump_id = create_test_entity(&app, "executor-complete-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "edge-agent-complete-01", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command(&app, executor_id, command_id).await;

        let completed = complete_executor_test_command(&app, executor_id, command_id).await;

        assert_eq!(completed["command"]["status"], "executed");
        assert_eq!(completed["action"]["status"], "completed");
        assert_eq!(completed["action_result"]["status"], "succeeded");
        let events = query_events_by_type(&app, "aion:ExecutorCompletedCommand").await;
        assert_eq!(events.as_array().unwrap().len(), 1);
        assert_eq!(events[0]["command_id"], command_id);
    }

    #[tokio::test]
    async fn fail_command_marks_failed_and_creates_event() {
        let app = app();
        let pump_id = create_test_entity(&app, "executor-fail-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "edge-agent-fail-01", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command(&app, executor_id, command_id).await;

        let failed = fail_executor_test_command(&app, executor_id, command_id).await;

        assert_eq!(failed["command"]["status"], "failed");
        assert_eq!(failed["command"]["failure_reason"], "executor timeout");
        assert_eq!(failed["action_result"]["status"], "failed");
        let events = query_events_by_type(&app, "aion:ExecutorFailedCommand").await;
        assert_eq!(events.as_array().unwrap().len(), 1);
        assert_eq!(events[0]["command_id"], command_id);
    }

    #[tokio::test]
    async fn claim_creates_active_lease() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "lease-agent-01", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();

        let claimed =
            claim_executor_test_command_with_lease(&app, executor_id, command_id, 60, None).await;
        let lease = get_command_lease(&app, command_id).await;

        assert_eq!(claimed["status"], "claimed");
        assert_eq!(lease["lease_status"], "active");
        assert_eq!(lease["executor_id"], executor_id);
        assert_eq!(claimed["lease_expires_at"], lease["expires_at"]);
    }

    #[tokio::test]
    async fn second_executor_cannot_claim_command_with_active_lease() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-block-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let first = create_compatible_executor(&app, "lease-agent-first", &pump_id).await;
        let second = create_compatible_executor(&app, "lease-agent-second", &pump_id).await;
        claim_executor_test_command(&app, first["id"].as_str().unwrap(), command_id).await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/executors/{}/commands/{command_id}/claim",
                        second["id"].as_str().unwrap()
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn lease_refresh_extends_expires_at() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-refresh-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "lease-agent-refresh", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command_with_lease(&app, executor_id, command_id, 60, None).await;
        let before = get_command_lease(&app, command_id).await;

        let refreshed = refresh_command_lease(&app, command_id, executor_id, 120).await;

        assert!(refreshed["expires_at"].as_str().unwrap() > before["expires_at"].as_str().unwrap());
    }

    #[tokio::test]
    async fn lease_release_returns_command_to_pending() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-release-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "lease-agent-release", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command(&app, executor_id, command_id).await;

        let lease = release_command_lease(&app, command_id, executor_id).await;
        let commands = query_pending_commands(&app, &pump_id).await;

        assert_eq!(lease["lease_status"], "released");
        assert_eq!(commands.as_array().unwrap().len(), 1);
        assert_eq!(commands[0]["id"], command_id);
    }

    #[tokio::test]
    async fn complete_command_marks_lease_completed() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-complete-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "lease-agent-complete", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command(&app, executor_id, command_id).await;

        complete_executor_test_command(&app, executor_id, command_id).await;
        let lease = get_command_lease(&app, command_id).await;

        assert_eq!(lease["lease_status"], "completed");
    }

    #[tokio::test]
    async fn fail_command_marks_lease_failed() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-fail-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "lease-agent-fail", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command(&app, executor_id, command_id).await;

        fail_executor_test_command(&app, executor_id, command_id).await;
        let lease = get_command_lease(&app, command_id).await;

        assert_eq!(lease["lease_status"], "failed");
    }

    #[tokio::test]
    async fn registers_smartsentinel_executor_with_capabilities_and_scopes() {
        let app = app();
        let service_id = smartsentinel_service_entity(&app).await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/executors/register",
                json!({
                    "agent_key": "sentinel-agent-register",
                    "display_name": "Sentinel Agent",
                    "capabilities": ["sentinel:RestartService", "sentinel:RunDiagnostic"],
                    "scopes": [
                        {"target_entity_id": service_id},
                        {"entity_type": "sentinel:Service"},
                        {"relationship_type": "sentinel:runs"}
                    ],
                    "metadata": {"node_id": "fog-01"}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let registered = to_json(response).await;
        assert_eq!(registered["executor"]["agent_type"], "smartsentinel");
        assert_eq!(
            registered["executor"]["agent_key"],
            "sentinel-agent-register"
        );
        assert_eq!(registered["capabilities"].as_array().unwrap().len(), 2);
        assert_eq!(registered["scopes"].as_array().unwrap().len(), 3);

        let response = app
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/executors/register",
                json!({
                    "agent_key": "sentinel-agent-register",
                    "display_name": "Sentinel Agent Reused",
                    "capabilities": ["sentinel:RestartService"],
                    "scopes": [{"target_entity_id": service_id}]
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let reused = to_json(response).await;
        assert_eq!(reused["reused"], true);
        assert_eq!(reused["executor"]["id"], registered["executor"]["id"]);
    }

    #[tokio::test]
    async fn smartsentinel_poll_returns_compatible_operational_command() {
        let app = app();
        let service_id = smartsentinel_service_entity(&app).await;
        let command = create_test_command(&app, &service_id, "sentinel:RestartService").await;
        let executor = register_smartsentinel_executor(
            &app,
            "sentinel-agent-poll",
            &service_id,
            &["sentinel:RestartService"],
        )
        .await;
        let executor_id = executor["executor"]["id"].as_str().unwrap();

        let commands = poll_smartsentinel_commands(&app, executor_id).await;

        assert!(commands.as_array().unwrap().iter().any(|item| {
            item["command"]["id"] == command["id"]
                && item["target_entity"]["entity_type"] == "sentinel:Service"
                && item["command"]["approval_status"] == "not_required"
        }));
    }

    #[tokio::test]
    async fn smartsentinel_claim_respects_approval_policy() {
        let app = app();
        let service_id = smartsentinel_service_entity(&app).await;
        put_command_policy(&app, &service_id, "sentinel:RestartService", true).await;
        let command = create_test_command(&app, &service_id, "sentinel:RestartService").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = register_smartsentinel_executor(
            &app,
            "sentinel-agent-approval",
            &service_id,
            &["sentinel:RestartService"],
        )
        .await;
        let executor_id = executor["executor"]["id"].as_str().unwrap();

        let blocked = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/claim"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(blocked.status(), StatusCode::BAD_REQUEST);

        approve_test_command(&app, command_id).await;
        let claimed = claim_smartsentinel_command(&app, executor_id, command_id).await;
        assert_eq!(claimed["command"]["status"], "claimed");
        assert_eq!(claimed["command"]["claimed_by"], "sentinel-agent-approval");
    }

    #[tokio::test]
    async fn smartsentinel_report_executed_creates_result_and_event() {
        let app = app();
        let service_id = smartsentinel_service_entity(&app).await;
        let command = create_test_command(&app, &service_id, "sentinel:RunDiagnostic").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = register_smartsentinel_executor(
            &app,
            "sentinel-agent-executed",
            &service_id,
            &["sentinel:RunDiagnostic"],
        )
        .await;
        let executor_id = executor["executor"]["id"].as_str().unwrap();
        claim_smartsentinel_command(&app, executor_id, command_id).await;

        let reported =
            report_smartsentinel_command(&app, executor_id, command_id, "executed").await;

        assert_eq!(reported["command"]["status"], "executed");
        assert_eq!(reported["action"]["action_type"], "sentinel:RunDiagnostic");
        assert_eq!(reported["action"]["status"], "executed");
        assert_eq!(reported["action_result"]["status"], "executed");
        assert_eq!(reported["action_result"]["verified"], true);
        assert_eq!(
            reported["event"]["event_type"],
            "aion:SmartSentinelCommandReported"
        );
        assert_eq!(
            reported["action_result"]["metadata"]["evidence_refs"],
            json!(["ev-log-1"])
        );
        assert_eq!(reported["event"]["metadata"]["incident_id"], "inc-001");
        let lease = get_command_lease(&app, command_id).await;
        assert_eq!(lease["lease_status"], "completed");
    }

    #[tokio::test]
    async fn smartsentinel_report_failed_marks_command_failed() {
        let app = app();
        let service_id = smartsentinel_service_entity(&app).await;
        let command = create_test_command(&app, &service_id, "sentinel:RestartService").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = register_smartsentinel_executor(
            &app,
            "sentinel-agent-failed",
            &service_id,
            &["sentinel:RestartService"],
        )
        .await;
        let executor_id = executor["executor"]["id"].as_str().unwrap();
        claim_smartsentinel_command(&app, executor_id, command_id).await;

        let reported = report_smartsentinel_command(&app, executor_id, command_id, "failed").await;

        assert_eq!(reported["command"]["status"], "failed");
        assert_eq!(
            reported["command"]["failure_reason"],
            "SmartSentinel dry-run execution failed"
        );
        assert_eq!(reported["action_result"]["status"], "failed");
        let lease = get_command_lease(&app, command_id).await;
        assert_eq!(lease["lease_status"], "failed");
    }

    #[tokio::test]
    async fn smartsentinel_bridge_does_not_change_generic_executor_api() {
        let app = app();
        let service_id = smartsentinel_service_entity(&app).await;
        let command = create_test_command(&app, &service_id, "sentinel:NotifyOperator").await;
        register_smartsentinel_executor(
            &app,
            "sentinel-agent-generic-isolation",
            &service_id,
            &["sentinel:NotifyOperator"],
        )
        .await;
        let generic = create_test_executor(&app, "edge-agent-still-generic").await;
        let generic_id = generic["id"].as_str().unwrap();
        put_executor_capabilities(&app, generic_id, &["sentinel:NotifyOperator"]).await;
        put_executor_scope_for_target(&app, generic_id, &service_id).await;

        let generic_commands = poll_executor_commands(&app, generic_id).await;

        assert!(generic_commands
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == command["id"]));
    }

    #[tokio::test]
    async fn recover_expired_leases_returns_command_to_pending_when_retry_limit_not_exceeded() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-retry-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "lease-agent-retry", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command_with_lease(&app, executor_id, command_id, 1, Some(2)).await;
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

        let recovery = recover_expired_leases(&app).await;
        let commands = query_pending_commands(&app, &pump_id).await;

        assert_eq!(recovery["retried_command_ids"].as_array().unwrap().len(), 1);
        assert_eq!(commands[0]["retry_count"], 1);
    }

    #[tokio::test]
    async fn recover_expired_leases_marks_command_failed_when_retry_limit_exceeded() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-limit-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "lease-agent-limit", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command_with_lease(&app, executor_id, command_id, 1, Some(0)).await;
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

        let recovery = recover_expired_leases(&app).await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/commands/{command_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let command = to_json(response).await;

        assert_eq!(recovery["failed_command_ids"].as_array().unwrap().len(), 1);
        assert_eq!(command["status"], "failed");
    }

    #[tokio::test]
    async fn expired_lease_emits_event() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-expired-event-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor =
            create_compatible_executor(&app, "lease-agent-expired-event", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command_with_lease(&app, executor_id, command_id, 1, Some(2)).await;
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

        recover_expired_leases(&app).await;

        let expired = query_events_by_type(&app, "aion:CommandLeaseExpired").await;
        assert_eq!(expired.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn retry_scheduled_emits_event() {
        let app = app();
        let pump_id = create_test_entity(&app, "lease-retry-event-pump-01", "aion:Pump").await;
        let command = create_test_command(&app, &pump_id, "StartPump").await;
        let command_id = command["id"].as_str().unwrap();
        let executor = create_compatible_executor(&app, "lease-agent-retry-event", &pump_id).await;
        let executor_id = executor["id"].as_str().unwrap();
        claim_executor_test_command_with_lease(&app, executor_id, command_id, 1, Some(2)).await;
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

        recover_expired_leases(&app).await;

        let retried = query_events_by_type(&app, "aion:CommandRetryScheduled").await;
        assert_eq!(retried.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ingests_senml_json_payload() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "senml-json",
                    "protocol": "http",
                    "content_type": "application/senml+json",
                    "payload": [
                        {
                            "bn": "urn:aion:farm:01:soil-sensor:01:",
                            "bt": 1777294800,
                            "n": "soil_moisture",
                            "u": "%",
                            "v": 18.5
                        },
                        {
                            "n": "soil_temperature",
                            "u": "Cel",
                            "v": 24.1
                        }
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert!(ingest["raw_message_id"].as_str().is_some());
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 2);
        assert_eq!(
            ingest["observations"][0]["observed_property"],
            "soil_moisture"
        );
        assert_eq!(
            ingest["observations"][1]["observed_property"],
            "soil_temperature"
        );
    }

    #[tokio::test]
    async fn creates_lists_and_gets_ingestion_connector() {
        let app = app();
        let connector = create_http_connector(&app, "http-connector-01", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();

        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ingestion/connectors")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);
        let connectors = to_json(list_response).await;
        assert_eq!(connectors.as_array().unwrap().len(), 1);
        assert_eq!(connectors[0]["connector_key"], "http-connector-01");

        let get_response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/ingestion/connectors/{connector_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        let fetched = to_json(get_response).await;
        assert_eq!(fetched["id"], connector_id);
        assert_eq!(fetched["connector_profile"], "custom");
    }

    #[tokio::test]
    async fn enables_and_disables_ingestion_connector() {
        let app = app();
        let connector = create_http_connector(&app, "http-connector-toggle", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();

        let enabled = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/ingestion/connectors/{connector_id}/enable"),
                json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(enabled.status(), StatusCode::OK);
        assert_eq!(to_json(enabled).await["enabled"], true);

        let disabled = app
            .oneshot(json_request(
                "PUT",
                &format!("/ingestion/connectors/{connector_id}/disable"),
                json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(disabled.status(), StatusCode::OK);
        assert_eq!(to_json(disabled).await["enabled"], false);
    }

    #[tokio::test]
    async fn disabled_connector_status_includes_profile() {
        let app = app();
        let connector = create_http_connector(&app, "http-connector-status", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        let disabled = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/ingestion/connectors/{connector_id}/disable"),
                json!({}),
            ))
            .await
            .unwrap();
        assert_eq!(disabled.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/ingestion/connectors/{connector_id}/status"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let status = to_json(response).await;
        assert_eq!(status["status"], "disabled");
        assert_eq!(status["connector_profile"], "custom");
    }

    #[tokio::test]
    async fn connector_http_ingestion_uses_payload_format_default_and_stores_metadata() {
        let app = app();
        let sensor_id = create_test_entity(&app, "connector-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "connector-plot-01", "aion:Plot").await;
        let connector = create_http_connector(
            &app,
            "http-connector-ingest",
            Some(&sensor_id),
            Some(&plot_id),
        )
        .await;
        let connector_id = connector["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": [
                        {"n": "soil_moisture", "u": "%", "v": 22.0}
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let raw_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/raw-messages/{raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(raw_response.status(), StatusCode::OK);
        let raw_message = to_json(raw_response).await;
        assert_eq!(raw_message["payload_format"], "senml-json");
        assert_eq!(raw_message["connector_id"], connector_id);
        assert_eq!(raw_message["connector_key"], "http-connector-ingest");
        assert_eq!(raw_message["connector_profile"], "custom");

        let event_response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/events?raw_message_id={raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(event_response.status(), StatusCode::OK);
        let events = to_json(event_response).await;
        let ingested = events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_type"] == "aion:PayloadIngested")
            .expect("payload ingested event should exist");
        assert_eq!(ingested["metadata"]["connector_id"], connector_id);
        assert_eq!(
            ingested["metadata"]["connector_key"],
            "http-connector-ingest"
        );
    }

    #[tokio::test]
    async fn connector_http_ingestion_decodes_ttn_v3_uplink_json() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-http-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-http-plot-01", "aion:Plot").await;
        let connector = create_ttn_connector(&app, "ttn-http-ingest", &sensor_id, &plot_id).await;
        let connector_id = connector["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();
        let observations = ingest["observations"].as_array().unwrap();
        assert_eq!(observations.len(), 3);
        assert!(observations
            .iter()
            .any(|observation| observation["observed_property"] == "ttn:temperature"));
        assert!(observations
            .iter()
            .any(|observation| observation["observed_property"] == "ttn:state"));
        assert!(observations
            .iter()
            .any(|observation| observation["observed_property"] == "ttn:battery_low"));
        let temperature = observations
            .iter()
            .find(|observation| observation["observed_property"] == "ttn:temperature")
            .unwrap();
        assert_eq!(temperature["unit"], "Cel");
        assert_eq!(temperature["metadata"]["ttn_device_id"], "soil-node-01");
        assert_eq!(temperature["metadata"]["ttn_application_id"], "farm-app");

        let raw_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/raw-messages/{raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(raw_response.status(), StatusCode::OK);
        let raw_message = to_json(raw_response).await;
        assert_eq!(raw_message["connector_id"], connector_id);
        assert_eq!(raw_message["connector_profile"], "ttn-v3");
        assert_eq!(raw_message["payload_format"], "ttn-uplink-json");

        let events = query_events_by_raw_message(&app, raw_message_id).await;
        let ingested = events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_type"] == "aion:PayloadIngested")
            .expect("payload ingested event should exist");
        assert_eq!(ingested["metadata"]["connector_profile"], "ttn-v3");
        assert_eq!(ingested["metadata"]["ttn_device_id"], "soil-node-01");
        assert_eq!(ingested["metadata"]["ttn_application_id"], "farm-app");
        assert_eq!(
            ingested["metadata"]["decoded_payload_keys"],
            json!(["battery_low", "location", "state", "temperature"])
        );
    }

    #[tokio::test]
    async fn creates_lists_and_toggles_ttn_device_mapping() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-map-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-map-plot-01", "aion:Plot").await;
        let connector = create_ttn_connector_with_defaults(&app, "ttn-map-api", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();

        let mapping = create_ttn_device_mapping(
            &app,
            connector_id,
            Some("farm-app"),
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;
        let mapping_id = mapping["id"].as_str().unwrap();
        assert_eq!(mapping["ttn_application_id"], "farm-app");
        assert!(mapping["enabled"].as_bool().unwrap());

        let list_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ingestion/connectors/{connector_id}/ttn-device-mappings"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);
        let mappings = to_json(list_response).await;
        assert_eq!(mappings.as_array().unwrap().len(), 1);

        let disable_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}/disable"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disable_response.status(), StatusCode::OK);
        assert!(!to_json(disable_response).await["enabled"]
            .as_bool()
            .unwrap());

        let enable_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}/enable"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(enable_response.status(), StatusCode::OK);
        assert!(to_json(enable_response).await["enabled"].as_bool().unwrap());

        let events = query_events_by_type(&app, "aion:TtnDeviceMappingCreated").await;
        assert_eq!(events.as_array().unwrap().len(), 1);
        assert_eq!(events[0]["metadata"]["ttn_device_id"], "soil-node-01");
    }

    #[tokio::test]
    async fn duplicate_enabled_ttn_mapping_is_rejected() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-dup-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-dup-plot-01", "aion:Plot").await;
        let connector = create_ttn_connector_with_defaults(&app, "ttn-dup-map", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        create_ttn_device_mapping(
            &app,
            connector_id,
            Some("farm-app"),
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings"),
                json!({
                    "ttn_application_id": "farm-app",
                    "ttn_device_id": "soil-node-01",
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let error = to_json(response).await;
        assert!(error["error"]
            .as_str()
            .unwrap()
            .contains("enabled TTN mapping conflict"));
    }

    #[tokio::test]
    async fn duplicate_enabled_fallback_ttn_mapping_is_rejected() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-dup-fallback-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-dup-fallback-plot-01", "aion:Plot").await;
        let connector =
            create_ttn_connector_with_defaults(&app, "ttn-dup-fallback-map", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        create_ttn_device_mapping(
            &app,
            connector_id,
            None,
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings"),
                json!({
                    "ttn_device_id": "soil-node-01",
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let error = to_json(response).await;
        assert!(error["error"].as_str().unwrap().contains("fallback device"));
    }

    #[tokio::test]
    async fn ttn_ingestion_without_producer_resolves_via_mapping() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-map-resolve-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-map-resolve-plot-01", "aion:Plot").await;
        let connector =
            create_ttn_connector_with_defaults(&app, "ttn-map-resolve", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        let mapping = create_ttn_device_mapping(
            &app,
            connector_id,
            Some("farm-app"),
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;
        let mapping_id = mapping["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        let observations = ingest["observations"].as_array().unwrap();
        assert_eq!(observations.len(), 3);
        assert!(observations.iter().all(|observation| {
            observation["producer_entity_id"] == sensor_id
                && observation["feature_of_interest_id"] == plot_id
        }));
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();
        let events = query_events_by_raw_message(&app, raw_message_id).await;
        let ingested = events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_type"] == "aion:PayloadIngested")
            .unwrap();
        assert_eq!(ingested["metadata"]["ttn_mapping_id"], mapping_id);
        assert_eq!(
            ingested["metadata"]["mapping_resolution"],
            "exact_application_match"
        );
        assert_eq!(ingested["metadata"]["ttn_device_id"], "soil-node-01");
        assert_eq!(ingested["metadata"]["ttn_application_id"], "farm-app");
        let resolved = query_events_by_type(&app, "aion:TtnDeviceMappingResolved").await;
        assert_eq!(resolved.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ttn_ingestion_explicit_producer_still_works_without_mapping() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-explicit-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-explicit-plot-01", "aion:Plot").await;
        let connector = create_ttn_connector_with_defaults(&app, "ttn-explicit", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 3);
        assert!(ingest["observations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|observation| observation["producer_entity_id"] == sensor_id));
    }

    #[tokio::test]
    async fn ttn_mapping_feature_is_used_when_request_omits_feature() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-feature-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-feature-plot-01", "aion:Plot").await;
        let connector =
            create_ttn_connector_with_defaults(&app, "ttn-feature-map", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        create_ttn_device_mapping(
            &app,
            connector_id,
            Some("farm-app"),
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "producer_entity_id": sensor_id,
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert!(ingest["observations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|observation| observation["feature_of_interest_id"] == plot_id));
    }

    #[tokio::test]
    async fn ttn_ingestion_without_mapping_preserves_failed_raw_message() {
        let app = app();
        let connector =
            create_ttn_connector_with_defaults(&app, "ttn-map-missing", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let raw_messages = query_raw_messages(&app).await;
        assert_eq!(raw_messages.as_array().unwrap().len(), 1);
        assert_eq!(raw_messages[0]["normalization_status"], "failed");
        assert_eq!(raw_messages[0]["connector_id"], connector_id);
        let missing = query_events_by_type(&app, "aion:TtnDeviceMappingMissing").await;
        assert_eq!(missing.as_array().unwrap().len(), 1);
        assert_eq!(missing[0]["metadata"]["ttn_device_id"], "soil-node-01");
        assert_eq!(missing[0]["metadata"]["ttn_application_id"], "farm-app");
        assert_eq!(missing[0]["metadata"]["connector_id"], connector_id);
        assert_eq!(
            missing[0]["metadata"]["mapping_resolution_error"],
            "ttn_device_mapping_missing"
        );
        let observations = query_observations_by_feature(&app, &Uuid::new_v4().to_string()).await;
        assert!(observations.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn disabled_ttn_mapping_is_ignored() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-disabled-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-disabled-plot-01", "aion:Plot").await;
        let connector =
            create_ttn_connector_with_defaults(&app, "ttn-disabled-map", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        let mapping = create_ttn_device_mapping(
            &app,
            connector_id,
            Some("farm-app"),
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;
        let mapping_id = mapping["id"].as_str().unwrap();
        let disable_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}/disable"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disable_response.status(), StatusCode::OK);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let missing = query_events_by_type(&app, "aion:TtnDeviceMappingMissing").await;
        assert_eq!(missing.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn ttn_mapping_with_application_id_is_preferred() {
        let app = app();
        let generic_sensor = create_test_entity(&app, "ttn-generic-sensor-01", "aion:Sensor").await;
        let app_sensor = create_test_entity(&app, "ttn-app-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-pref-plot-01", "aion:Plot").await;
        let connector = create_ttn_connector_with_defaults(&app, "ttn-pref-map", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        create_ttn_device_mapping(
            &app,
            connector_id,
            None,
            "soil-node-01",
            &generic_sensor,
            Some(&plot_id),
        )
        .await;
        let app_mapping = create_ttn_device_mapping(
            &app,
            connector_id,
            Some("farm-app"),
            "soil-node-01",
            &app_sensor,
            Some(&plot_id),
        )
        .await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert!(ingest["observations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|observation| observation["producer_entity_id"] == app_sensor));
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();
        let events = query_events_by_raw_message(&app, raw_message_id).await;
        let ingested = events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_type"] == "aion:PayloadIngested")
            .unwrap();
        assert_eq!(ingested["metadata"]["ttn_mapping_id"], app_mapping["id"]);
        assert_eq!(
            ingested["metadata"]["mapping_resolution"],
            "exact_application_match"
        );
    }

    #[tokio::test]
    async fn fallback_ttn_mapping_resolves_when_application_does_not_match() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-fallback-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-fallback-plot-01", "aion:Plot").await;
        let connector =
            create_ttn_connector_with_defaults(&app, "ttn-fallback-map", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        let mapping = create_ttn_device_mapping(
            &app,
            connector_id,
            None,
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();
        let events = query_events_by_raw_message(&app, raw_message_id).await;
        let ingested = events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_type"] == "aion:PayloadIngested")
            .unwrap();
        assert_eq!(ingested["metadata"]["ttn_mapping_id"], mapping["id"]);
        assert_eq!(
            ingested["metadata"]["mapping_resolution"],
            "fallback_device_match"
        );
    }

    #[tokio::test]
    async fn update_and_delete_ttn_mapping_change_resolution() {
        let app = app();
        let first_sensor = create_test_entity(&app, "ttn-update-sensor-01", "aion:Sensor").await;
        let second_sensor = create_test_entity(&app, "ttn-update-sensor-02", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-update-plot-01", "aion:Plot").await;
        let connector =
            create_ttn_connector_with_defaults(&app, "ttn-update-map", None, None).await;
        let connector_id = connector["id"].as_str().unwrap();
        let mapping = create_ttn_device_mapping(
            &app,
            connector_id,
            Some("farm-app"),
            "soil-node-01",
            &first_sensor,
            Some(&plot_id),
        )
        .await;
        let mapping_id = mapping["id"].as_str().unwrap();

        let update_response = app
            .clone()
            .oneshot(json_request(
                "PATCH",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}"),
                json!({
                    "producer_entity_id": second_sensor,
                    "metadata": {
                        "source": "updated"
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(update_response.status(), StatusCode::OK);
        let updated = to_json(update_response).await;
        assert_eq!(updated["producer_entity_id"], second_sensor);
        assert_eq!(updated["metadata"]["source"], "updated");

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert!(ingest["observations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|observation| observation["producer_entity_id"] == second_sensor));

        let delete_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!(
                        "/ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ingest"),
                json!({
                    "payload": ttn_uplink_payload()
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn creates_ttn_v3_connector() {
        let app = app();
        let response = app
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": "ttn-v3-demo",
                    "connector_type": "mqtt",
                    "connector_profile": "ttn-v3",
                    "enabled": false,
                    "broker_url": "mqtt://eu1.cloud.thethings.network:1883",
                    "topic_filter": "v3/demo-app/devices/+/up",
                    "payload_format": "ttn-uplink-json"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let connector = to_json(response).await;
        assert_eq!(connector["connector_profile"], "ttn-v3");
        assert_eq!(connector["payload_format"], "ttn-uplink-json");
    }

    #[tokio::test]
    async fn ttn_connector_validation_reports_missing_broker_url() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-validate-missing-broker",
            "ttn-v3",
            true,
            None,
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["valid"], false);
        assert_eq!(validation["readiness"], "invalid");
        assert!(validation_issue_codes(&validation, "issues").contains(&"missing_broker_url"));
    }

    #[tokio::test]
    async fn ttn_connector_validation_reports_missing_topic_filter() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-validate-missing-topic",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            None,
            Some("ttn-uplink-json"),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["valid"], false);
        assert!(validation_issue_codes(&validation, "issues").contains(&"missing_topic_filter"));
    }

    #[tokio::test]
    async fn ttn_connector_validation_reports_unsupported_payload_format() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-validate-bad-format",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("canonical-json"),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["payload_format_supported"], false);
        assert!(validation_issue_codes(&validation, "issues")
            .contains(&"unsupported_ttn_payload_format"));
    }

    #[tokio::test]
    async fn valid_looking_ttn_connector_without_mappings_is_degraded_with_warning() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-validate-no-mappings",
            "ttn-v3",
            true,
            Some("mqtt://private.example.test:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["valid"], true);
        assert_eq!(validation["readiness"], "degraded");
        assert_eq!(validation["mapping_count"], 0);
        assert_eq!(validation["enabled_mapping_count"], 0);
        assert!(validation_issue_codes(&validation, "warnings")
            .contains(&"missing_ttn_device_mappings"));
    }

    #[tokio::test]
    async fn ttn_connector_validation_reports_mapping_counts() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-validation-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-validation-plot-01", "aion:Plot").await;
        let connector = create_mqtt_connector(
            &app,
            "ttn-validate-counts",
            "ttn-v3",
            true,
            Some("mqtt://private.example.test:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;
        create_ttn_device_mapping(
            &app,
            connector["id"].as_str().unwrap(),
            Some("farm-app"),
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["valid"], true);
        assert_eq!(validation["readiness"], "ready");
        assert_eq!(validation["mapping_count"], 1);
        assert_eq!(validation["enabled_mapping_count"], 1);
        assert!(validation["warnings"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn public_ttn_broker_without_secret_ref_warns_about_auth() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-validate-auth-warning",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert!(validation_issue_codes(&validation, "warnings").contains(&"missing_secret_ref"));
        assert_eq!(validation["has_secret_ref"], false);
        assert_eq!(validation["secret_configured"], false);
    }

    #[tokio::test]
    async fn ttn_connector_validation_reports_missing_secret_reference() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        let mut connector = IngestionConnector::new(
            state.tenant_id,
            "ttn-missing-secret-ref",
            IngestionConnectorType::Mqtt,
            ConnectorProfile::TtnV3,
            true,
            None,
            None,
            None,
            Some("mqtt://eu1.cloud.thethings.network:1883".to_string()),
            Some("ttn-missing-secret-ref-client".to_string()),
            Some("v3/demo-app/devices/+/up".to_string()),
            None,
            Some("ttn-uplink-json".to_string()),
            None,
            None,
            None,
            None,
            Utc::now(),
        )
        .unwrap();
        connector.secret_ref_id = Some(Uuid::new_v4());
        let connector = state
            .storage
            .create_ingestion_connector(connector)
            .expect("create connector with missing secret ref");

        let validation = validate_connector(&app, &connector.id.to_string()).await;
        assert_eq!(validation["valid"], false);
        assert_eq!(validation["readiness"], "invalid");
        assert!(validation_issue_codes(&validation, "issues").contains(&"secret_ref_not_found"));
        assert_eq!(validation["secret_configured"], false);
    }

    #[tokio::test]
    async fn ttn_connector_validation_reports_incompatible_secret_type() {
        let app = app();
        let secret = create_connector_secret_with_type(
            &app,
            "ttn-token-secret",
            "token",
            Some("token-user"),
            "secret-pass",
        )
        .await;
        let connector = create_ttn_connector_with_secret(
            &app,
            "ttn-incompatible-secret",
            secret["id"].as_str().unwrap(),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["secret_type"], "token");
        assert_eq!(validation["secret_configured"], false);
        assert!(validation_issue_codes(&validation, "issues").contains(&"incompatible_secret_type"));
        assert!(!validation.to_string().contains("secret-pass"));
    }

    #[tokio::test]
    async fn ttn_connector_validation_reports_missing_secret_username() {
        let app = app();
        let secret = create_connector_secret_with_type(
            &app,
            "ttn-no-username-secret",
            "mqtt_basic_auth",
            None,
            "secret-pass",
        )
        .await;
        let connector = create_ttn_connector_with_secret(
            &app,
            "ttn-missing-secret-username",
            secret["id"].as_str().unwrap(),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["secret_type"], "mqtt_basic_auth");
        assert_eq!(validation["secret_configured"], false);
        assert!(validation_issue_codes(&validation, "issues").contains(&"missing_secret_username"));
        assert!(!validation.to_string().contains("secret-pass"));
    }

    #[tokio::test]
    async fn ttn_connector_validation_reports_configured_basic_auth_secret_without_value_leak() {
        let app = app();
        let secret = create_connector_secret_with_type(
            &app,
            "ttn-basic-auth-secret",
            "mqtt_basic_auth",
            Some("farm-app@tenant"),
            "secret-pass",
        )
        .await;
        let connector = create_ttn_connector_with_secret(
            &app,
            "ttn-valid-secret",
            secret["id"].as_str().unwrap(),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["has_secret_ref"], true);
        assert_eq!(validation["secret_configured"], true);
        assert_eq!(validation["secret_type"], "mqtt_basic_auth");
        assert!(!validation.to_string().contains("secret-pass"));
        assert!(!validation["operator_hints"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn disabled_ttn_connector_validation_is_not_invalid_solely_for_disabled_state() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-validate-disabled",
            "ttn-v3",
            false,
            Some("mqtt://private.example.test:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["valid"], true);
        assert_eq!(validation["readiness"], "degraded");
        assert!(validation_issue_codes(&validation, "warnings").contains(&"connector_disabled"));
    }

    #[tokio::test]
    async fn non_ttn_connector_validation_returns_generic_response() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "generic-validate",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let validation = validate_connector(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(validation["valid"], true);
        assert_eq!(validation["detected_profile"], "generic-mqtt");
        assert!(validation["operator_hints"].as_array().unwrap().is_empty());
        assert!(validation_issue_codes(&validation, "warnings")
            .contains(&"profile_specific_validation_unavailable"));
    }

    #[tokio::test]
    async fn complete_ttn_live_readiness_plan_is_safe_to_connect() {
        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-live-ready-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-live-ready-plot-01", "aion:Plot").await;
        let secret = create_connector_secret_with_type(
            &app,
            "ttn-live-ready-secret",
            "mqtt_basic_auth",
            Some("farm-app@tenant"),
            "secret-pass",
        )
        .await;
        let connector = create_ttn_connector_with_secret(
            &app,
            "ttn-live-ready",
            secret["id"].as_str().unwrap(),
        )
        .await;
        create_ttn_device_mapping(
            &app,
            connector["id"].as_str().unwrap(),
            Some("farm-app"),
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;

        let plan = get_ttn_live_readiness_plan(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(plan["dry_run"], true);
        assert_eq!(plan["safe_to_connect"], true);
        assert_eq!(plan["can_attempt_live_validation"], true);
        assert_eq!(plan["readiness"], "ready");
        assert!(plan["blockers"].as_array().unwrap().is_empty());
        assert!(plan_has_check(&plan, "no_network_call_performed", "pass"));
        assert!(!plan.to_string().contains("secret-pass"));
    }

    #[tokio::test]
    async fn ttn_live_readiness_plan_missing_broker_url_creates_blocker() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-live-missing-broker",
            "ttn-v3",
            true,
            None,
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let plan = get_ttn_live_readiness_plan(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(plan["safe_to_connect"], false);
        assert_eq!(plan["readiness"], "invalid");
        assert!(validation_issue_codes(&plan, "blockers").contains(&"missing_broker_url"));
    }

    #[tokio::test]
    async fn ttn_live_readiness_plan_missing_secret_creates_blocker() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-live-missing-secret",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let plan = get_ttn_live_readiness_plan(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(plan["safe_to_connect"], false);
        assert!(validation_issue_codes(&plan, "blockers").contains(&"missing_secret_ref"));
        assert!(plan["required_operator_steps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|step| step.as_str().unwrap().contains("mqtt_basic_auth")));
    }

    #[tokio::test]
    async fn ttn_live_readiness_plan_missing_mapping_creates_blocker() {
        let app = app();
        let secret = create_connector_secret_with_type(
            &app,
            "ttn-live-no-mapping-secret",
            "mqtt_basic_auth",
            Some("farm-app@tenant"),
            "secret-pass",
        )
        .await;
        let connector = create_ttn_connector_with_secret(
            &app,
            "ttn-live-no-mapping",
            secret["id"].as_str().unwrap(),
        )
        .await;

        let plan = get_ttn_live_readiness_plan(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(plan["safe_to_connect"], false);
        assert!(validation_issue_codes(&plan, "blockers")
            .contains(&"missing_enabled_ttn_device_mapping"));
        assert!(plan_has_check(
            &plan,
            "at_least_one_enabled_ttn_mapping",
            "fail"
        ));
    }

    #[tokio::test]
    async fn non_ttn_live_readiness_plan_is_not_applicable() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "generic-live-plan",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let plan = get_ttn_live_readiness_plan(&app, connector["id"].as_str().unwrap()).await;
        assert_eq!(plan["dry_run"], true);
        assert_eq!(plan["safe_to_connect"], false);
        assert_eq!(plan["can_attempt_live_validation"], false);
        assert!(validation_issue_codes(&plan, "warnings").contains(&"not_applicable"));
        assert!(plan_has_check(
            &plan,
            "connector_profile_is_ttn_v3",
            "skipped"
        ));
    }

    #[tokio::test]
    async fn disabled_ttn_live_readiness_plan_is_not_safe() {
        let app = app();
        let secret = create_connector_secret_with_type(
            &app,
            "ttn-live-disabled-secret",
            "mqtt_basic_auth",
            Some("farm-app@tenant"),
            "secret-pass",
        )
        .await;
        let connector = create_ttn_connector_with_secret(
            &app,
            "ttn-live-disabled",
            secret["id"].as_str().unwrap(),
        )
        .await;
        let connector_id = connector["id"].as_str().unwrap();
        let disable_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/ingestion/connectors/{connector_id}/disable"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disable_response.status(), StatusCode::OK);

        let plan = get_ttn_live_readiness_plan(&app, connector_id).await;
        assert_eq!(plan["safe_to_connect"], false);
        assert_eq!(plan["can_attempt_live_validation"], false);
        assert!(validation_issue_codes(&plan, "warnings").contains(&"connector_disabled"));
    }

    #[tokio::test]
    async fn ttn_live_validate_rejects_non_ttn_connector() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "generic-live-validate",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!(
                    "/ingestion/connectors/{}/ttn-live-validate",
                    connector["id"].as_str().unwrap()
                ),
                json!({}),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_json(response).await;
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("ttn-v3 connectors"));
    }

    #[tokio::test]
    async fn unsafe_ttn_live_validate_is_blocked_by_dry_run_plan() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-live-validate-unsafe",
            "ttn-v3",
            true,
            None,
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let response = post_ttn_live_validate(
            &app,
            connector["id"].as_str().unwrap(),
            json!({"timeout_seconds": 60}),
        )
        .await;

        assert_eq!(response["result"], "skipped");
        assert_eq!(response["attempted_live_connection"], false);
        assert_eq!(response["dry_run_passed"], false);
        assert_eq!(response["dry_run_plan_summary"]["safe_to_connect"], false);
        assert!(validation_issue_codes(&response, "errors").contains(&"missing_broker_url"));
    }

    #[tokio::test]
    async fn ttn_live_validate_dry_run_only_returns_without_network_attempt() {
        let app = app();
        let sensor_id =
            create_test_entity(&app, "ttn-preflight-dry-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-preflight-dry-plot-01", "aion:Plot").await;
        let secret = create_connector_secret_with_type(
            &app,
            "ttn-preflight-dry-secret",
            "mqtt_basic_auth",
            Some("farm-app@tenant"),
            "secret-pass",
        )
        .await;
        let connector = create_ttn_connector_with_secret(
            &app,
            "ttn-preflight-dry",
            secret["id"].as_str().unwrap(),
        )
        .await;
        create_ttn_device_mapping(
            &app,
            connector["id"].as_str().unwrap(),
            Some("farm-app"),
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;

        let response = post_ttn_live_validate(
            &app,
            connector["id"].as_str().unwrap(),
            json!({"dry_run_only": true, "timeout_seconds": 60}),
        )
        .await;

        assert_eq!(response["result"], "skipped");
        assert_eq!(response["attempted_live_connection"], false);
        assert_eq!(response["dry_run_passed"], true);
        assert_eq!(response["dry_run_plan_summary"]["safe_to_connect"], true);
        assert_eq!(response["secret_exposed"], false);
        assert!(!response.to_string().contains("secret-pass"));
    }

    #[tokio::test]
    async fn ttn_live_validate_response_never_exposes_secret_value() {
        let app = app();
        let sensor_id =
            create_test_entity(&app, "ttn-preflight-secret-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-preflight-secret-plot-01", "aion:Plot").await;
        let secret = create_connector_secret_with_type(
            &app,
            "ttn-preflight-redacted-secret",
            "mqtt_basic_auth",
            Some("farm-app@tenant"),
            "do-not-return-this-secret",
        )
        .await;
        let connector = create_ttn_connector_with_secret(
            &app,
            "ttn-preflight-redacted",
            secret["id"].as_str().unwrap(),
        )
        .await;
        create_ttn_device_mapping(
            &app,
            connector["id"].as_str().unwrap(),
            Some("farm-app"),
            "soil-node-01",
            &sensor_id,
            Some(&plot_id),
        )
        .await;

        let response = post_ttn_live_validate(
            &app,
            connector["id"].as_str().unwrap(),
            json!({"dry_run_only": true}),
        )
        .await;

        assert_eq!(response["secret_exposed"], false);
        assert!(!response.to_string().contains("do-not-return-this-secret"));
    }

    #[tokio::test]
    async fn ttn_live_validate_caps_timeout_seconds() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-live-validate-timeout-cap",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let response = post_ttn_live_validate(
            &app,
            connector["id"].as_str().unwrap(),
            json!({"timeout_seconds": 999}),
        )
        .await;

        assert_eq!(response["result"], "skipped");
        assert!(validation_issue_codes(&response, "warnings").contains(&"timeout_seconds_capped"));
    }

    #[tokio::test]
    async fn ttn_live_validate_missing_secret_prevents_connection() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "ttn-live-validate-missing-secret",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let response =
            post_ttn_live_validate(&app, connector["id"].as_str().unwrap(), json!({})).await;

        assert_eq!(response["result"], "skipped");
        assert_eq!(response["attempted_live_connection"], false);
        assert!(validation_issue_codes(&response, "errors").contains(&"missing_secret_ref"));
    }

    #[tokio::test]
    async fn ttn_live_validate_invalid_connector_returns_clear_error() {
        let response = app()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{}/ttn-live-validate", Uuid::new_v4()),
                json!({}),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_json(response).await;
        assert_eq!(body["error"], "record was not found");
    }

    #[tokio::test]
    #[ignore = "requires AIONCORE_TEST_TTN_LIVE=1 and live TTN MQTT credentials"]
    async fn opt_in_ttn_live_validate_can_connect_when_env_is_configured() {
        if env::var("AIONCORE_TEST_TTN_LIVE").ok().as_deref() != Some("1") {
            return;
        }

        let broker_url = env::var("AIONCORE_TEST_TTN_BROKER_URL")
            .expect("AIONCORE_TEST_TTN_BROKER_URL is required");
        let topic_filter = env::var("AIONCORE_TEST_TTN_TOPIC_FILTER")
            .expect("AIONCORE_TEST_TTN_TOPIC_FILTER is required");
        let username =
            env::var("AIONCORE_TEST_TTN_USERNAME").expect("AIONCORE_TEST_TTN_USERNAME is required");
        let password =
            env::var("AIONCORE_TEST_TTN_PASSWORD").expect("AIONCORE_TEST_TTN_PASSWORD is required");
        let application_id =
            env::var("AIONCORE_TEST_TTN_APPLICATION_ID").unwrap_or_else(|_| "test-app".to_string());
        let device_id =
            env::var("AIONCORE_TEST_TTN_DEVICE_ID").unwrap_or_else(|_| "test-device".to_string());

        let app = app();
        let sensor_id = create_test_entity(&app, "ttn-live-env-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "ttn-live-env-plot-01", "aion:Plot").await;
        let secret =
            create_connector_secret(&app, "ttn-live-env-secret", &username, &password).await;
        let connector = create_mqtt_connector(
            &app,
            "ttn-live-env",
            "ttn-v3",
            true,
            Some(&broker_url),
            Some(&topic_filter),
            Some("ttn-uplink-json"),
        )
        .await;
        patch_connector_secret_ref(
            &app,
            connector["id"].as_str().unwrap(),
            secret["id"].as_str().unwrap(),
        )
        .await;
        create_ttn_device_mapping(
            &app,
            connector["id"].as_str().unwrap(),
            Some(&application_id),
            &device_id,
            &sensor_id,
            Some(&plot_id),
        )
        .await;

        let response = post_ttn_live_validate(
            &app,
            connector["id"].as_str().unwrap(),
            json!({"timeout_seconds": 5, "expect_message": false}),
        )
        .await;

        assert_eq!(response["attempted_live_connection"], true);
        assert_eq!(response["secret_exposed"], false);
        assert!(!response.to_string().contains(&password));
    }

    #[tokio::test]
    async fn existing_http_ingestion_without_connector_still_works() {
        let app = app();
        let sensor_id = create_test_entity(&app, "no-connector-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "no-connector-plot-01", "aion:Plot").await;

        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        assert!(ingest["raw_message_id"].as_str().is_some());
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn empty_connector_list_returns_empty_worker_plan() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/ingestion/workers/plan")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let plan = to_json(response).await;
        assert_eq!(plan["specs"].as_array().unwrap().len(), 0);
        assert_eq!(plan["planned_workers"], 0);
    }

    #[tokio::test]
    async fn disabled_connector_is_marked_skipped_in_worker_plan() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "mqtt-disabled-plan",
            "generic-mqtt",
            false,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let plan = get_worker_plan(&app).await;
        assert_eq!(plan["specs"][0]["connector_id"], connector["id"]);
        assert_eq!(plan["specs"][0]["status"], "skipped");
        assert_eq!(plan["skipped_workers"], 1);
    }

    #[tokio::test]
    async fn valid_generic_mqtt_connector_produces_mqtt_subscriber_spec() {
        let app = app();
        create_mqtt_connector(
            &app,
            "mqtt-plan",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let plan = get_worker_plan(&app).await;
        assert_eq!(plan["planned_workers"], 1);
        assert_eq!(plan["specs"][0]["worker_kind"], "mqtt_subscriber");
        assert_eq!(plan["specs"][0]["status"], "planned");
        assert_eq!(plan["specs"][0]["broker_url"], "mqtt://127.0.0.1:1883");
    }

    #[tokio::test]
    async fn ttn_v3_uplink_json_connector_produces_valid_mqtt_spec() {
        let app = app();
        create_mqtt_connector(
            &app,
            "ttn-plan",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        let plan = get_worker_plan(&app).await;
        assert_eq!(plan["planned_workers"], 1);
        assert_eq!(plan["specs"][0]["worker_kind"], "mqtt_subscriber");
        assert_eq!(plan["specs"][0]["status"], "planned");
        assert!(plan["specs"][0]["validation_issues"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn worker_plan_includes_ttn_topic_shape_issue() {
        let app = app();
        create_mqtt_connector(
            &app,
            "ttn-plan-bad-topic",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            Some("aioncore/+/+/data"),
            Some("ttn-uplink-json"),
        )
        .await;

        let plan = get_worker_plan(&app).await;
        assert_eq!(plan["invalid_workers"], 1);
        assert_eq!(
            plan["specs"][0]["validation_issues"][0]["code"],
            "implausible_ttn_topic_filter"
        );
    }

    #[tokio::test]
    async fn ttn_v3_unsupported_payload_format_is_invalid() {
        let app = app();
        create_mqtt_connector(
            &app,
            "ttn-plan-unsupported",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("canonical-json"),
        )
        .await;

        let plan = get_worker_plan(&app).await;
        assert_eq!(plan["invalid_workers"], 1);
        assert_eq!(plan["specs"][0]["status"], "invalid");
        assert_eq!(
            plan["specs"][0]["validation_issues"][0]["code"],
            "unsupported_ttn_payload_format"
        );
    }

    #[tokio::test]
    async fn mqtt_connector_missing_broker_url_is_invalid() {
        let app = app();
        create_mqtt_connector(
            &app,
            "mqtt-invalid-plan",
            "generic-mqtt",
            true,
            None,
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let plan = get_worker_plan(&app).await;
        assert_eq!(plan["invalid_workers"], 1);
        assert_eq!(plan["specs"][0]["status"], "invalid");
        assert_eq!(
            plan["specs"][0]["validation_issues"][0]["code"],
            "missing_broker_url"
        );
    }

    #[tokio::test]
    async fn valid_http_connector_produces_http_listener_spec() {
        let app = app();
        create_http_connector(&app, "http-plan", None, None).await;

        let plan = get_worker_plan(&app).await;
        assert_eq!(plan["planned_workers"], 1);
        assert_eq!(plan["specs"][0]["worker_kind"], "http_listener");
        assert_eq!(plan["specs"][0]["status"], "planned");
        assert_eq!(
            plan["specs"][0]["http_path"],
            "/ingestion/connectors/{connector_id}/ingest"
        );
    }

    #[tokio::test]
    async fn future_connector_type_produces_unsupported_spec() {
        let app = app();
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": "future-plan",
                    "connector_type": "future",
                    "connector_profile": "custom",
                    "enabled": true
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let plan = get_worker_plan(&app).await;
        assert_eq!(plan["unsupported_workers"], 1);
        assert_eq!(plan["specs"][0]["worker_kind"], "unsupported");
        assert_eq!(plan["specs"][0]["status"], "unsupported");
    }

    #[tokio::test]
    async fn worker_plan_does_not_start_mqtt_worker() {
        let app = app();
        create_mqtt_connector(
            &app,
            "mqtt-no-start-plan",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let plan = get_worker_plan(&app).await;
        let ready_response = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ready_response.status(), StatusCode::OK);
        let ready = to_json(ready_response).await;
        assert_eq!(plan["planned_workers"], 1);
        assert_eq!(ready["mqtt"]["enabled"], false);
        assert_eq!(ready["mqtt"]["connected"], false);
        assert_eq!(ready["worker_plan"]["planned_workers"], 1);
    }

    #[test]
    fn connector_worker_config_defaults_disabled() {
        let config =
            ConnectorWorkerConfig::from_env_values(ConnectorWorkerEnvValues::default()).unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn connector_worker_config_parses_enabled() {
        let config = ConnectorWorkerConfig::from_env_values(ConnectorWorkerEnvValues {
            enabled: Some("true".to_string()),
        })
        .unwrap();
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn connector_workers_status_reports_disabled_by_default() {
        let app = app();
        create_mqtt_connector(
            &app,
            "mqtt-runtime-disabled",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let status = get_worker_status(&app).await;
        assert_eq!(status["connector_workers"]["enabled"], false);
        assert_eq!(status["workers"][0]["status"], "planned");
    }

    #[tokio::test]
    async fn connector_worker_reconciliation_is_disabled_when_config_is_false() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        create_mqtt_connector(
            &app,
            "mqtt-reconcile-disabled",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let response = reconcile_connector_workers(state.clone(), false)
            .await
            .unwrap();

        assert!(!response.connector_workers.enabled);
        assert_eq!(
            response.workers[0].status,
            ConnectorWorkerRuntimeState::Planned
        );
        assert_eq!(response.connector_workers.running, 0);
    }

    #[tokio::test]
    async fn enabled_valid_mqtt_connector_reconcile_records_start_intent_without_broker() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        create_mqtt_connector(
            &app,
            "mqtt-reconcile-start",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;
        set_connector_workers_enabled(&state, true);

        let response = reconcile_connector_workers(state.clone(), false)
            .await
            .unwrap();

        assert!(response.connector_workers.enabled);
        assert_eq!(response.actions[0].action, "started");
        assert_eq!(
            response.workers[0].status,
            ConnectorWorkerRuntimeState::Planned
        );
        assert_eq!(response.workers[0].last_reconciled_at.is_some(), true);
    }

    #[tokio::test]
    async fn disabling_connector_stops_running_worker_status() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        let connector = create_mqtt_connector(
            &app,
            "mqtt-reconcile-stop",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;
        set_connector_workers_enabled(&state, true);
        let connector_id = Uuid::parse_str(connector["id"].as_str().unwrap()).unwrap();
        let mut plan = build_ingestion_worker_plan(&state).unwrap();
        let spec = plan.specs.remove(0);
        let task = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        state.connector_worker_handles.write().unwrap().insert(
            connector_id,
            ConnectorWorkerHandle {
                signature: connector_worker_signature(&spec),
                task,
            },
        );
        let mut status = connector_runtime_status_from_spec(&spec);
        status.status = ConnectorWorkerRuntimeState::Running;
        status.connected = true;
        status.subscribed = true;
        status.started_at = Some(Utc::now());
        set_connector_worker_runtime_status(&state, status);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/ingestion/connectors/{connector_id}/disable"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let status = connector_workers_status(&state).unwrap();
        assert_eq!(
            status.workers[0].status,
            ConnectorWorkerRuntimeState::Stopped
        );
        assert!(status.workers[0].stopped_at.is_some());
        assert!(state.connector_worker_handles.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn manual_reconcile_endpoint_returns_worker_status() {
        let state = AppState::local();
        let app = app_with_state(state);
        create_mqtt_connector(
            &app,
            "mqtt-manual-reconcile",
            "generic-mqtt",
            true,
            None,
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/ingestion/workers/reconcile")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body["workers"][0]["status"], "invalid");
        assert_eq!(body["workers"][0]["last_reconciled_at"].is_null(), false);
    }

    #[tokio::test]
    async fn creating_connector_triggers_reconciliation_path() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        create_mqtt_connector(
            &app,
            "mqtt-create-reconcile",
            "generic-mqtt",
            true,
            None,
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let status = connector_workers_status(&state).unwrap();
        assert_eq!(
            status.workers[0].status,
            ConnectorWorkerRuntimeState::Invalid
        );
        assert!(status.workers[0].last_reconciled_at.is_some());
    }

    #[tokio::test]
    async fn connector_secret_responses_do_not_expose_secret_value() {
        let app = app();
        let secret =
            create_connector_secret(&app, "broker-secret", "mqtt-user", "secret-pass").await;
        assert_eq!(secret["secret_key"], "broker-secret");
        assert!(secret.get("secret_value").is_none());

        let secret_id = secret["id"].as_str().unwrap();
        let get_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/secrets/connectors/{secret_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        let get_body = to_json(get_response).await;
        assert!(get_body.get("secret_value").is_none());
        assert_ne!(get_body.to_string().contains("secret-pass"), true);

        let list_response = app
            .oneshot(
                Request::builder()
                    .uri("/secrets/connectors")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = to_json(list_response).await;
        assert!(list_body[0].get("secret_value").is_none());
        assert_ne!(list_body.to_string().contains("secret-pass"), true);
    }

    #[tokio::test]
    async fn connector_can_reference_secret_without_leaking_value() {
        let app = app();
        let secret =
            create_connector_secret(&app, "connector-broker-secret", "mqtt-user", "secret-pass")
                .await;
        let secret_id = secret["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": "mqtt-secret-ref",
                    "connector_type": "mqtt",
                    "connector_profile": "generic-mqtt",
                    "enabled": false,
                    "broker_url": "mqtt://127.0.0.1:1883",
                    "client_id": "mqtt-secret-ref-client",
                    "topic_filter": "aioncore/+/+/data",
                    "payload_format": "canonical-json",
                    "secret_ref_id": secret_id
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let connector = to_json(response).await;
        assert_eq!(connector["secret_ref_id"], secret_id);
        assert_ne!(connector.to_string().contains("secret-pass"), true);
    }

    #[tokio::test]
    async fn worker_status_and_readiness_do_not_expose_secret_value() {
        let app = app();
        let secret =
            create_connector_secret(&app, "status-broker-secret", "mqtt-user", "secret-pass").await;
        let secret_id = secret["id"].as_str().unwrap();
        create_mqtt_connector_with_secret(
            &app,
            "mqtt-status-secret-ref",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
            Some(secret_id),
        )
        .await;

        let status = get_worker_status(&app).await;
        let ready_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ready_response.status(), StatusCode::OK);
        let ready = to_json(ready_response).await;

        assert!(!status.to_string().contains("secret-pass"));
        assert!(!ready.to_string().contains("secret-pass"));
    }

    #[tokio::test]
    async fn deleting_connector_secret_clears_future_worker_auth_reference() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        let secret =
            create_connector_secret(&app, "delete-broker-secret", "mqtt-user", "secret-pass").await;
        let secret_id = secret["id"].as_str().unwrap();
        create_mqtt_connector_with_secret(
            &app,
            "mqtt-deleted-secret",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
            Some(secret_id),
        )
        .await;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/secrets/connectors/{secret_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let plan = build_ingestion_worker_plan(&state).unwrap();
        assert_eq!(plan.specs[0].secret_ref_id, None);
    }

    #[test]
    fn mqtt_connector_config_with_basic_auth_redacts_debug_output() {
        let config = mqtt_ingest::MqttIngestConfig::for_connector_with_basic_auth(
            "mqtt://127.0.0.1:1883".to_string(),
            "client".to_string(),
            "aioncore/+/+/data".to_string(),
            Some("canonical-json".to_string()),
            None,
            Some("mqtt-user".to_string()),
            "secret-pass".to_string(),
            mqtt_ingest::MqttConnectorMetadata {
                connector_id: Uuid::new_v4(),
                connector_key: "mqtt-secret".to_string(),
                connector_profile: ConnectorProfile::GenericMqtt,
            },
        );

        assert_eq!(config.username.as_deref(), Some("mqtt-user"));
        assert!(config.password_configured());
        let debug = format!("{config:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-pass"));
    }

    #[tokio::test]
    async fn patch_connector_updates_runtime_fields() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "mqtt-update-fields",
            "generic-mqtt",
            false,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;
        let connector_id = connector["id"].as_str().unwrap();

        let response = app
            .clone()
            .oneshot(json_request(
                "PATCH",
                &format!("/ingestion/connectors/{connector_id}"),
                json!({
                    "display_name": "Updated MQTT",
                    "enabled": true,
                    "broker_url": "mqtt://127.0.0.1:1884",
                    "topic_filter": "farm/+/telemetry",
                    "payload_format": "senml-json",
                    "metadata": {"site": "north"}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_json(response).await;
        assert_eq!(body["display_name"], "Updated MQTT");
        assert_eq!(body["enabled"], true);
        assert_eq!(body["broker_url"], "mqtt://127.0.0.1:1884");
        assert_eq!(body["topic_filter"], "farm/+/telemetry");
        assert_eq!(body["payload_format"], "senml-json");
        assert_eq!(body["metadata"]["site"], "north");
    }

    #[tokio::test]
    async fn patch_connector_rejects_immutable_identity_fields() {
        let app = app();
        let connector = create_mqtt_connector(
            &app,
            "mqtt-update-immutable",
            "generic-mqtt",
            false,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;
        let connector_id = connector["id"].as_str().unwrap();

        let response = app
            .oneshot(json_request(
                "PATCH",
                &format!("/ingestion/connectors/{connector_id}"),
                json!({
                    "connector_key": "changed"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn patch_invalid_connector_triggers_reconciliation_status() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        let connector = create_mqtt_connector(
            &app,
            "mqtt-update-invalid",
            "generic-mqtt",
            false,
            None,
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;
        let connector_id = connector["id"].as_str().unwrap();
        set_connector_workers_enabled(&state, true);

        let response = app
            .oneshot(json_request(
                "PATCH",
                &format!("/ingestion/connectors/{connector_id}"),
                json!({
                    "enabled": true
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let status = connector_workers_status(&state).unwrap();
        assert_eq!(
            status.workers[0].status,
            ConnectorWorkerRuntimeState::Invalid
        );
        assert!(status.workers[0].last_reconciled_at.is_some());
        assert!(status.workers[0]
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("broker_url"));
    }

    #[tokio::test]
    async fn patch_disabling_connector_stops_running_worker_status() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        let connector = create_mqtt_connector(
            &app,
            "mqtt-update-stop",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;
        let connector_id = Uuid::parse_str(connector["id"].as_str().unwrap()).unwrap();
        set_connector_workers_enabled(&state, true);
        let mut plan = build_ingestion_worker_plan(&state).unwrap();
        let spec = plan.specs.remove(0);
        let task = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        state.connector_worker_handles.write().unwrap().insert(
            connector_id,
            ConnectorWorkerHandle {
                signature: connector_worker_signature(&spec),
                task,
            },
        );
        let mut status = connector_runtime_status_from_spec(&spec);
        status.status = ConnectorWorkerRuntimeState::Running;
        status.connected = true;
        status.subscribed = true;
        status.started_at = Some(Utc::now());
        set_connector_worker_runtime_status(&state, status);

        let response = app
            .oneshot(json_request(
                "PATCH",
                &format!("/ingestion/connectors/{connector_id}"),
                json!({
                    "enabled": false
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let status = connector_workers_status(&state).unwrap();
        assert_eq!(
            status.workers[0].status,
            ConnectorWorkerRuntimeState::Stopped
        );
        assert!(state.connector_worker_handles.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn connector_worker_signature_changes_for_runtime_config_fields() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        let connector = create_mqtt_connector(
            &app,
            "mqtt-signature-change",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;
        let connector_id = Uuid::parse_str(connector["id"].as_str().unwrap()).unwrap();
        let original =
            connector_worker_signature(&build_ingestion_worker_plan(&state).unwrap().specs[0]);

        let mut stored = get_connector(&state, connector_id).unwrap();
        stored.broker_url = Some("mqtt://127.0.0.1:1884".to_string());
        stored.topic_filter = Some("farm/+/telemetry".to_string());
        stored.payload_format = Some("senml-json".to_string());
        stored.updated_at = Utc::now();
        state.storage.update_ingestion_connector(stored).unwrap();

        let changed =
            connector_worker_signature(&build_ingestion_worker_plan(&state).unwrap().specs[0]);
        assert_ne!(original, changed);
    }

    #[tokio::test]
    async fn connector_worker_backoff_state_can_be_represented() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        let connector = create_mqtt_connector(
            &app,
            "mqtt-backoff-state",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;
        let connector_id = Uuid::parse_str(connector["id"].as_str().unwrap()).unwrap();
        let spec = build_ingestion_worker_plan(&state).unwrap().specs.remove(0);
        set_connector_worker_runtime_status(&state, connector_runtime_status_from_spec(&spec));

        let next_reconnect_at = mark_connector_worker_reconnect_scheduled(
            &state,
            connector_id,
            "test reconnect".to_string(),
            std::time::Duration::from_secs(2),
        );

        let status = connector_workers_status(&state).unwrap();
        assert_eq!(
            status.workers[0].status,
            ConnectorWorkerRuntimeState::Reconnecting
        );
        assert_eq!(status.workers[0].reconnect_attempts, 1);
        assert_eq!(status.workers[0].next_reconnect_at, Some(next_reconnect_at));
        assert_eq!(status.connector_workers.degraded, 1);
    }

    #[tokio::test]
    async fn ttn_v3_connector_worker_is_startable_when_runtime_enabled() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        create_mqtt_connector(
            &app,
            "ttn-runtime-skip",
            "ttn-v3",
            true,
            Some("mqtt://eu1.cloud.thethings.network:1883"),
            Some("v3/demo-app/devices/+/up"),
            Some("ttn-uplink-json"),
        )
        .await;

        set_connector_workers_enabled(&state, true);
        reconcile_connector_workers(state.clone(), false)
            .await
            .unwrap();

        let status = connector_workers_status(&state).unwrap();
        assert!(status.connector_workers.enabled);
        assert_eq!(
            status.workers[0].status,
            ConnectorWorkerRuntimeState::Planned
        );
    }

    #[tokio::test]
    async fn invalid_mqtt_connector_worker_is_not_started() {
        let state = AppState::local();
        let app = app_with_state(state.clone());
        create_mqtt_connector(
            &app,
            "mqtt-runtime-invalid",
            "generic-mqtt",
            true,
            None,
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        start_connector_workers(state.clone(), ConnectorWorkerConfig { enabled: true })
            .await
            .unwrap();

        let status = connector_workers_status(&state).unwrap();
        assert_eq!(
            status.workers[0].status,
            ConnectorWorkerRuntimeState::Invalid
        );
        assert!(!status.workers[0].connected);
        assert!(!status.workers[0].subscribed);
    }

    #[tokio::test]
    async fn valid_generic_mqtt_connector_has_startable_worker_spec() {
        let app = app();
        create_mqtt_connector(
            &app,
            "mqtt-runtime-startable",
            "generic-mqtt",
            true,
            Some("mqtt://127.0.0.1:1883"),
            Some("aioncore/+/+/data"),
            Some("canonical-json"),
        )
        .await;

        let plan = get_worker_plan(&app).await;
        let spec = IngestionWorkerSpec {
            connector_id: Uuid::parse_str(plan["specs"][0]["connector_id"].as_str().unwrap())
                .unwrap(),
            connector_key: plan["specs"][0]["connector_key"]
                .as_str()
                .unwrap()
                .to_string(),
            connector_type: IngestionConnectorType::Mqtt,
            connector_profile: ConnectorProfile::GenericMqtt,
            enabled: true,
            worker_kind: IngestionWorkerKind::MqttSubscriber,
            broker_url: Some("mqtt://127.0.0.1:1883".to_string()),
            client_id: Some("mqtt-runtime-startable-client".to_string()),
            topic_filter: Some("aioncore/+/+/data".to_string()),
            http_path: None,
            payload_format: Some("canonical-json".to_string()),
            content_type: None,
            secret_ref_id: None,
            status: IngestionWorkerSpecStatus::Planned,
            validation_issues: Vec::new(),
            metadata: None,
        };
        assert_eq!(
            connector_worker_start_decision(&spec),
            ConnectorWorkerStartDecision::StartMqtt
        );
    }

    #[tokio::test]
    async fn queries_raw_message_by_id() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/raw-messages/{raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let raw_message = to_json(response).await;
        assert_eq!(raw_message["id"], raw_message_id);
        assert_eq!(raw_message["raw_message_id"], raw_message_id);
        assert_eq!(raw_message["protocol"], "http");
        assert_eq!(raw_message["content_type"], "application/senml+json");
        assert_eq!(raw_message["payload_format"], "senml-json");
        assert_eq!(raw_message["producer_entity_id"], sensor_id);
        assert_eq!(raw_message["feature_of_interest_id"], plot_id);
        assert_eq!(raw_message["normalization_status"], "normalized");
        assert_eq!(raw_message["decoder_metadata"]["decoder"], "senml-json");
        assert_eq!(raw_message["payload"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn queries_raw_messages_by_producer_entity_id() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/raw-messages?producer_entity_id={sensor_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let raw_messages = to_json(response).await;
        assert_eq!(raw_messages.as_array().unwrap().len(), 1);
        assert_eq!(raw_messages[0]["id"], raw_message_id);
    }

    #[tokio::test]
    async fn queries_raw_messages_by_feature_of_interest_id() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let other_plot_id = create_test_entity(&app, "plot-02", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        ingest_test_senml(&app, &sensor_id, &other_plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/raw-messages?feature_of_interest_id={plot_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let raw_messages = to_json(response).await;
        assert_eq!(raw_messages.as_array().unwrap().len(), 1);
        assert_eq!(raw_messages[0]["raw_message_id"], raw_message_id);
        assert_eq!(raw_messages[0]["feature_of_interest_id"], plot_id);
    }

    #[tokio::test]
    async fn queries_raw_messages_by_payload_format() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/raw-messages?payload_format=senml-json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let raw_messages = to_json(response).await;
        assert_eq!(raw_messages.as_array().unwrap().len(), 1);
        assert_eq!(raw_messages[0]["id"], raw_message_id);
    }

    #[tokio::test]
    async fn raw_message_is_linked_to_generated_observations() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;
        let ingest = ingest_test_senml(&app, &sensor_id, &plot_id).await;
        let raw_message_id = ingest["raw_message_id"].as_str().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/observations?raw_message_id={raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let observations = to_json(response).await;
        assert_eq!(observations.as_array().unwrap().len(), 2);
        assert!(observations
            .as_array()
            .unwrap()
            .iter()
            .all(|observation| observation["raw_message_id"] == raw_message_id));
    }

    #[tokio::test]
    async fn ingests_ultralight_payload() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "observed_at": "2026-04-27T13:00:00Z",
                    "payload": "m|18.5|t|24.1",
                    "mapping": {
                        "m": {
                            "observed_property": "aion:SoilMoisture",
                            "unit": "%"
                        },
                        "t": {
                            "observed_property": "aion:SoilTemperature",
                            "unit": "Cel"
                        }
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 2);
        assert_eq!(
            ingest["observations"][0]["observed_property"],
            "aion:SoilMoisture"
        );
        assert_eq!(ingest["observations"][0]["unit"], "%");
        assert_eq!(
            ingest["observations"][1]["observed_property"],
            "aion:SoilTemperature"
        );
    }

    #[tokio::test]
    async fn ingests_ultralight_payload_using_payload_profile() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/entities/{sensor_id}/payload-profile"),
                json!({
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "attribute_mapping": {
                        "m": {
                            "observed_property": "aion:SoilMoisture",
                            "unit": "%"
                        },
                        "t": {
                            "observed_property": "aion:SoilTemperature",
                            "unit": "Cel"
                        }
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "observed_at": "2026-04-27T13:00:00Z",
                    "payload": "m|18.5|t|24.1"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 2);
        assert_eq!(
            ingest["observations"][0]["observed_property"],
            "aion:SoilMoisture"
        );
        assert_eq!(
            ingest["observations"][1]["observed_property"],
            "aion:SoilTemperature"
        );
    }

    #[tokio::test]
    async fn rejects_ultralight_payload_without_mapping_or_payload_profile() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "ultralight",
                    "protocol": "http",
                    "content_type": "text/plain",
                    "observed_at": "2026-04-27T13:00:00Z",
                    "payload": "m|18.5|t|24.1"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error = to_json(response).await;
        assert!(error["error"]
            .as_str()
            .unwrap()
            .contains("request mapping or producer PayloadProfile attribute_mapping"));
    }

    #[tokio::test]
    async fn ingests_canonical_json_payload() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "canonical-json",
                    "protocol": "http",
                    "content_type": "application/json",
                    "payload": {
                        "observations": [
                            {
                                "observed_property": "aion:SoilMoisture",
                                "value": {"type": "number", "value": 18.5},
                                "unit": "%",
                                "observed_at": "2026-04-27T13:00:00Z"
                            }
                        ]
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let ingest = to_json(response).await;
        assert_eq!(ingest["observations"].as_array().unwrap().len(), 1);
        assert_eq!(
            ingest["observations"][0]["raw_message_id"],
            ingest["raw_message_id"]
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/observations?feature_of_interest_id={plot_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let observations = to_json(response).await;
        assert_eq!(observations.as_array().unwrap().len(), 1);
        assert_eq!(observations[0]["observed_property"], "aion:SoilMoisture");
    }

    #[tokio::test]
    async fn rejects_invalid_ingest_payload_after_raw_storage() {
        let app = app();
        let sensor_id = create_test_entity(&app, "soil-sensor-01", "aion:Sensor").await;
        let plot_id = create_test_entity(&app, "plot-01", "aion:Plot").await;

        let response = app
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "senml-json",
                    "protocol": "http",
                    "content_type": "application/senml+json",
                    "payload": "not json"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error = to_json(response).await;
        assert!(error["error"]
            .as_str()
            .unwrap()
            .contains("invalid SenML JSON payload"));
    }

    #[tokio::test]
    async fn flow_routes_support_crud_validation_and_lifecycle_events_in_dev_mode() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);

        let created = create_test_flow(&app, "flow-dev-01", "Dev Flow").await;
        let flow_id = created["id"].as_str().unwrap();
        assert_eq!(created["flow_key"], "flow-dev-01");
        assert_eq!(created["enabled"], false);

        let list = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/flows")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(list.as_array().unwrap().len(), 1);

        let detail = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/flows/{flow_id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(detail["id"], created["id"]);

        let duplicate_node_response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/flows",
                sample_flow_body_with_nodes(
                    "flow-invalid-dup",
                    "Duplicate Nodes",
                    json!([
                        {"node_id": "dup", "node_type": "source", "config": {"kind": "mqtt_subscribe"}},
                        {"node_id": "dup", "node_type": "sink", "config": {"kind": "internal_observation_store"}}
                    ]),
                    json!([]),
                ),
            ))
            .await
            .unwrap();
        assert_eq!(duplicate_node_response.status(), StatusCode::BAD_REQUEST);

        let unknown_edge_response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/flows",
                sample_flow_body_with_nodes(
                    "flow-invalid-edge",
                    "Unknown Edge",
                    json!([
                        {"node_id": "source-1", "node_type": "source", "config": {"kind": "mqtt_subscribe"}}
                    ]),
                    json!([
                        {"edge_id": "edge-1", "source_node_id": "source-1", "target_node_id": "missing-1"}
                    ]),
                ),
            ))
            .await
            .unwrap();
        assert_eq!(unknown_edge_response.status(), StatusCode::BAD_REQUEST);

        let invalid_type_response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/flows",
                sample_flow_body_with_nodes(
                    "flow-invalid-type",
                    "Invalid Type",
                    json!([
                        {"node_id": "source-1", "node_type": "banana", "config": {"kind": "mqtt_subscribe"}}
                    ]),
                    json!([]),
                ),
            ))
            .await
            .unwrap();
        assert_eq!(invalid_type_response.status(), StatusCode::BAD_REQUEST);

        let updated = to_json(
            app.clone()
                .oneshot(json_request(
                    "PATCH",
                    &format!("/flows/{flow_id}"),
                    json!({
                        "name": "Updated Dev Flow",
                        "description": "updated",
                        "metadata": {"updated": true}
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(updated["name"], "Updated Dev Flow");
        assert_eq!(updated["metadata"]["updated"], true);

        let enabled = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/flows/{flow_id}/enable"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(enabled["enabled"], true);

        let disabled = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/flows/{flow_id}/disable"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(disabled["enabled"], false);

        let overview = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/dashboard/overview")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(overview["flows_count"], 1);
        assert_eq!(overview["enabled_flows_count"], 0);

        let flow_events = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/events?event_type=aion:FlowCreated")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert!(!flow_events.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn token_mode_protects_flow_reads_and_writes_with_scopes() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        let token_app = token_mode_app_with_storage(storage.clone());
        create_test_flow(&dev_app, "flow-token-01", "Token Flow").await;

        let no_token = token_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/flows")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_token.status(), StatusCode::UNAUTHORIZED);

        let wrong_scope_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("flow-wrong-scope"),
            &["entities:read"],
        );
        let wrong_scope = token_app
            .clone()
            .oneshot(auth_request("GET", "/flows", &wrong_scope_token))
            .await
            .unwrap();
        assert_eq!(wrong_scope.status(), StatusCode::FORBIDDEN);

        let read_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("flow-reader"),
            &["flows:read", "dashboard:read"],
        );
        let read_list = to_json(
            token_app
                .clone()
                .oneshot(auth_request("GET", "/flows", &read_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(read_list.as_array().unwrap().len(), 1);

        let write_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("flow-writer"),
            &["flows:write"],
        );
        let created = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/flows",
                sample_flow_body("flow-token-write", "Token Write Flow"),
                &write_token,
            ))
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn token_mode_filters_flows_by_tenant_and_denies_cross_tenant_writes() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let token_app = token_mode_app_with_storage(storage.clone());

        let flow_a = create_test_flow(&tenant_a_app, "tenant-a-flow", "Tenant A Flow").await;
        let flow_b = create_test_flow(&tenant_b_app, "tenant-b-flow", "Tenant B Flow").await;
        let flow_b_id = flow_b["id"].as_str().unwrap();

        let tenant_a_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-flow-user"),
            &["flows:read", "flows:write", "dashboard:read"],
        );
        let tenant_b_token = store_api_token_for_tenant(
            &storage,
            tenant_b,
            ApiTokenPrincipalType::Service,
            Some("tenant-b-flow-user"),
            &["flows:read", "flows:write", "dashboard:read"],
        );

        let tenant_a_list = to_json(
            token_app
                .clone()
                .oneshot(auth_request("GET", "/flows", &tenant_a_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(tenant_a_list.as_array().unwrap().len(), 1);
        assert_eq!(tenant_a_list[0]["id"], flow_a["id"]);

        let cross_tenant_get = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/flows/{flow_b_id}"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_get.status(), StatusCode::FORBIDDEN);

        let cross_tenant_update = token_app
            .clone()
            .oneshot(auth_json_request(
                "PATCH",
                &format!("/flows/{flow_b_id}"),
                json!({"name": "hijack"}),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_update.status(), StatusCode::FORBIDDEN);

        let tenant_b_overview = to_json(
            token_app
                .clone()
                .oneshot(auth_request("GET", "/dashboard/overview", &tenant_b_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(tenant_b_overview["flows_count"], 1);
        assert_eq!(tenant_b_overview["enabled_flows_count"], 0);
    }

    #[tokio::test]
    async fn admin_all_can_read_and_manage_flows_across_tenants() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let token_app = token_mode_app_with_storage(storage.clone());
        let flow_b = create_test_flow(&tenant_b_app, "tenant-b-admin-flow", "Tenant B Flow").await;
        let flow_b_id = flow_b["id"].as_str().unwrap();

        let admin_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Admin,
            Some("flow-admin"),
            &["admin:all"],
        );

        let list = to_json(
            token_app
                .clone()
                .oneshot(auth_request("GET", "/flows", &admin_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(list.as_array().unwrap().len(), 1);

        let enabled = to_json(
            token_app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/flows/{flow_b_id}/enable"))
                        .header("authorization", format!("Bearer {admin_token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(enabled["enabled"], true);
    }

    #[tokio::test]
    async fn dashboard_flow_inventory_and_detail_work_in_dev_mode() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);

        let valid_flow = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows",
                    sample_flow_body_with_nodes(
                        "dashboard-flow-valid",
                        "Dashboard Valid Flow",
                        json!([
                            {
                                "node_id": "source-1",
                                "node_type": "source",
                                "name": "Source",
                                "config": {
                                    "kind": "mqtt_subscribe",
                                    "api_key": "top-secret"
                                }
                            },
                            {
                                "node_id": "decoder-1",
                                "node_type": "decoder",
                                "name": "Decoder",
                                "config": { "kind": "senml_decode" }
                            },
                            {
                                "node_id": "transform-1",
                                "node_type": "transform",
                                "name": "Transform",
                                "config": { "kind": "canonical_json" }
                            },
                            {
                                "node_id": "filter-1",
                                "node_type": "filter",
                                "name": "Filter",
                                "config": { "kind": "filter_condition" }
                            },
                            {
                                "node_id": "rule-1",
                                "node_type": "rule",
                                "name": "Rule",
                                "config": { "kind": "threshold_rule" }
                            },
                            {
                                "node_id": "sink-1",
                                "node_type": "sink",
                                "name": "Store",
                                "config": {
                                    "kind": "internal_observation_store",
                                    "password": "also-secret"
                                }
                            },
                            {
                                "node_id": "dlq-1",
                                "node_type": "dlq",
                                "name": "DLQ",
                                "config": { "kind": "dlq" }
                            }
                        ]),
                        json!([
                            { "edge_id": "edge-1", "source_node_id": "source-1", "target_node_id": "decoder-1" },
                            { "edge_id": "edge-2", "source_node_id": "decoder-1", "target_node_id": "transform-1" },
                            { "edge_id": "edge-3", "source_node_id": "transform-1", "target_node_id": "filter-1" },
                            { "edge_id": "edge-4", "source_node_id": "filter-1", "target_node_id": "rule-1" },
                            { "edge_id": "edge-5", "source_node_id": "rule-1", "target_node_id": "sink-1" },
                            { "edge_id": "edge-6", "source_node_id": "rule-1", "target_node_id": "dlq-1" }
                        ]),
                    ),
                ))
                .await
                .unwrap(),
        )
        .await;
        let valid_flow_id = valid_flow["id"].as_str().unwrap();

        to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows",
                    sample_flow_body_with_nodes(
                        "dashboard-flow-warning",
                        "Dashboard Warning Flow",
                        json!([
                            {
                                "node_id": "source-1",
                                "node_type": "source",
                                "config": { "kind": "mqtt_subscribe" }
                            },
                            {
                                "node_id": "sink-1",
                                "node_type": "sink",
                                "config": { "kind": "internal_observation_store" }
                            }
                        ]),
                        json!([]),
                    ),
                ))
                .await
                .unwrap(),
        )
        .await;

        to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows",
                    sample_flow_body_with_nodes(
                        "dashboard-flow-invalid",
                        "Dashboard Invalid Flow",
                        json!([
                            {
                                "node_id": "sink-1",
                                "node_type": "sink",
                                "config": { "kind": "internal_observation_store" }
                            }
                        ]),
                        json!([]),
                    ),
                ))
                .await
                .unwrap(),
        )
        .await;

        let inventory = get_json(&app, "/dashboard/flows").await;
        let flows = inventory["flows"].as_array().unwrap();
        assert_eq!(flows.len(), 3);

        let valid_item = flows
            .iter()
            .find(|entry| entry["flow_key"] == "dashboard-flow-valid")
            .unwrap();
        assert_eq!(valid_item["node_count"], 7);
        assert_eq!(valid_item["edge_count"], 6);
        assert_eq!(valid_item["source_count"], 1);
        assert_eq!(valid_item["decoder_count"], 1);
        assert_eq!(valid_item["transform_count"], 1);
        assert_eq!(valid_item["filter_count"], 1);
        assert_eq!(valid_item["rule_count"], 1);
        assert_eq!(valid_item["sink_count"], 1);
        assert_eq!(valid_item["dlq_count"], 1);
        assert_eq!(valid_item["validation_status"], "valid");
        assert_eq!(valid_item["validation_error_count"], 0);
        assert_eq!(valid_item["validation_warning_count"], 0);

        let invalid_item = flows
            .iter()
            .find(|entry| entry["flow_key"] == "dashboard-flow-invalid")
            .unwrap();
        assert_eq!(invalid_item["validation_status"], "invalid");
        assert!(invalid_item["validation_error_count"].as_u64().unwrap() >= 1);

        let warning_item = flows
            .iter()
            .find(|entry| entry["flow_key"] == "dashboard-flow-warning")
            .unwrap();
        assert_eq!(warning_item["validation_status"], "warning");
        assert_eq!(warning_item["validation_warning_count"], 2);

        let detail = get_json(&app, &format!("/dashboard/flows/{valid_flow_id}")).await;
        assert_eq!(detail["flow"]["flow_key"], "dashboard-flow-valid");
        assert_eq!(detail["nodes"].as_array().unwrap().len(), 7);
        assert_eq!(detail["edges"].as_array().unwrap().len(), 6);
        assert_eq!(detail["graph_summary"]["rule_count"], 1);
        assert_eq!(detail["validation_summary"]["status"], "valid");
        assert_eq!(detail["validation_summary"]["error_count"], 0);
        assert_eq!(detail["execution_supported"], false);
        assert_eq!(detail["execution_status"], "not_implemented");
        assert_eq!(detail["side_effects_performed"], false);
        assert_eq!(detail["planned_path"].as_array().unwrap().len(), 7);
        assert_eq!(detail["nodes"][0]["config"]["api_key"], "***REDACTED***");
        assert_eq!(detail["nodes"][5]["config"]["password"], "***REDACTED***");

        let overview = get_json(&app, "/dashboard/overview").await;
        assert_eq!(overview["flows_count"], 3);
        assert_eq!(overview["invalid_flows_count"], 1);
        assert_eq!(overview["flow_validation_warning_count"], 3);
    }

    #[tokio::test]
    async fn token_mode_protects_dashboard_flow_endpoints_with_dashboard_read_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let dev_app = dev_mode_app_with_storage(storage.clone());
        create_test_flow(&dev_app, "dashboard-token-flow", "Dashboard Token Flow").await;
        let app = token_mode_app_with_storage(storage.clone());

        let no_token = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/dashboard/flows")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_token.status(), StatusCode::UNAUTHORIZED);

        let wrong_scope_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("dashboard-flow-wrong-scope"),
            &["flows:read"],
        );
        let wrong_scope = app
            .clone()
            .oneshot(auth_request("GET", "/dashboard/flows", &wrong_scope_token))
            .await
            .unwrap();
        assert_eq!(wrong_scope.status(), StatusCode::FORBIDDEN);

        let dashboard_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Service,
            Some("dashboard-flow-reader"),
            &["dashboard:read"],
        );
        let inventory = app
            .clone()
            .oneshot(auth_request("GET", "/dashboard/flows", &dashboard_token))
            .await
            .unwrap();
        assert_eq!(inventory.status(), StatusCode::OK);

        let admin_token = store_api_token(
            &storage,
            ApiTokenPrincipalType::Admin,
            Some("dashboard-flow-admin"),
            &["admin:all"],
        );
        let admin_inventory = app
            .clone()
            .oneshot(auth_request("GET", "/dashboard/flows", &admin_token))
            .await
            .unwrap();
        assert_eq!(admin_inventory.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_filters_dashboard_flows_by_tenant_and_denies_cross_tenant_detail() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let token_app = token_mode_app_with_storage(storage.clone());

        create_test_flow(
            &tenant_a_app,
            "tenant-a-dashboard-flow",
            "Tenant A Dashboard Flow",
        )
        .await;
        let flow_b = create_test_flow(
            &tenant_b_app,
            "tenant-b-dashboard-flow",
            "Tenant B Dashboard Flow",
        )
        .await;
        let flow_b_id = flow_b["id"].as_str().unwrap();

        let tenant_a_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-dashboard-reader"),
            &["dashboard:read"],
        );

        let tenant_a_inventory = to_json(
            token_app
                .clone()
                .oneshot(auth_request("GET", "/dashboard/flows", &tenant_a_token))
                .await
                .unwrap(),
        )
        .await;
        let tenant_a_flows = tenant_a_inventory["flows"].as_array().unwrap();
        assert_eq!(tenant_a_flows.len(), 1);
        assert_eq!(tenant_a_flows[0]["flow_key"], "tenant-a-dashboard-flow");

        let cross_tenant_detail = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/dashboard/flows/{flow_b_id}"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_detail.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn flow_validation_endpoint_reports_structured_issues_for_proposed_flows() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage);

        let valid = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows/validate",
                    sample_flow_body("flow-validate-01", "Validate Flow"),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(valid["valid"], true);
        assert_eq!(valid["validation_issues"].as_array().unwrap().len(), 1);
        assert_eq!(
            valid["validation_issues"][0]["code"],
            "flow_connector_reference_unverified"
        );

        let duplicate = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows/validate",
                    sample_flow_body_with_nodes(
                        "flow-validate-dup",
                        "Duplicate Nodes",
                        json!([
                            {"node_id": "dup", "node_type": "source", "config": {"kind": "mqtt_subscribe"}},
                            {"node_id": "dup", "node_type": "sink", "config": {"kind": "internal_observation_store"}}
                        ]),
                        json!([]),
                    ),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(duplicate["valid"], false);
        assert!(validation_issue_codes(&duplicate, "validation_issues")
            .contains(&"flow_duplicate_node_id"));

        let unknown_edge = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows/validate",
                    sample_flow_body_with_nodes(
                        "flow-validate-edge",
                        "Unknown Edge",
                        json!([
                            {"node_id": "source-1", "node_type": "source", "config": {"kind": "mqtt_subscribe"}},
                            {"node_id": "sink-1", "node_type": "sink", "config": {"kind": "internal_observation_store"}}
                        ]),
                        json!([
                            {"edge_id": "edge-1", "source_node_id": "missing-source", "target_node_id": "sink-1"},
                            {"edge_id": "edge-2", "source_node_id": "source-1", "target_node_id": "missing-target"}
                        ]),
                    ),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert!(validation_issue_codes(&unknown_edge, "validation_issues")
            .contains(&"flow_unknown_edge_source"));
        assert!(validation_issue_codes(&unknown_edge, "validation_issues")
            .contains(&"flow_unknown_edge_target"));

        let missing_source = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows/validate",
                    sample_flow_body_with_nodes(
                        "flow-validate-no-source",
                        "No Source",
                        json!([
                            {"node_id": "decoder-1", "node_type": "decoder", "config": {"kind": "senml_decode"}},
                            {"node_id": "sink-1", "node_type": "sink", "config": {"kind": "internal_observation_store"}}
                        ]),
                        json!([
                            {"edge_id": "edge-1", "source_node_id": "decoder-1", "target_node_id": "sink-1"}
                        ]),
                    ),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert!(validation_issue_codes(&missing_source, "validation_issues")
            .contains(&"flow_source_missing"));

        let missing_sink = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows/validate",
                    sample_flow_body_with_nodes(
                        "flow-validate-no-sink",
                        "No Sink",
                        json!([
                            {"node_id": "source-1", "node_type": "source", "config": {"kind": "mqtt_subscribe"}},
                            {"node_id": "filter-1", "node_type": "filter", "config": {"kind": "filter_condition"}}
                        ]),
                        json!([
                            {"edge_id": "edge-1", "source_node_id": "source-1", "target_node_id": "filter-1"}
                        ]),
                    ),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert!(validation_issue_codes(&missing_sink, "validation_issues")
            .contains(&"flow_sink_or_dlq_missing"));
    }

    #[tokio::test]
    async fn flow_validation_and_dry_run_support_stored_flows_and_redaction() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage.clone());
        let connector = create_mqtt_connector(
            &app,
            "flow-validator-connector",
            "generic-mqtt",
            true,
            Some("mqtt://broker.example:1883"),
            Some("devices/+/up"),
            Some("senml-json"),
        )
        .await;
        let connector_id = connector["id"].as_str().unwrap();

        let created = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows",
                    json!({
                        "flow_key": "stored-flow-validate",
                        "name": "Stored Flow Validate",
                        "enabled": false,
                        "nodes": [
                            {
                                "node_id": "source-1",
                                "node_type": "source",
                                "config": {
                                    "kind": "mqtt_subscribe",
                                    "connector_id": connector_id,
                                    "access_token": "super-secret-token"
                                }
                            },
                            {
                                "node_id": "sink-1",
                                "node_type": "sink",
                                "config": {
                                    "kind": "mqtt_publish",
                                    "topic": "alerts/out",
                                    "password": "secret-password"
                                }
                            },
                            {
                                "node_id": "dlq-1",
                                "node_type": "dlq",
                                "config": {
                                    "kind": "dlq",
                                    "credential_ref": "hidden-credential"
                                }
                            }
                        ],
                        "edges": [
                            {"edge_id": "edge-1", "source_node_id": "source-1", "target_node_id": "sink-1"},
                            {"edge_id": "edge-2", "source_node_id": "source-1", "target_node_id": "dlq-1"}
                        ]
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        let flow_id = created["id"].as_str().unwrap();

        let validation = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/flows/{flow_id}/validation"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(validation["valid"], true);
        assert_eq!(validation["flow_id"], created["id"]);
        assert_eq!(validation["referenced_connectors"][0]["exists"], true);

        let dry_run = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    &format!("/flows/{flow_id}/dry-run"),
                    json!({
                        "sample_payload": {"temperature": 21.4},
                        "payload_format": "senml-json"
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(dry_run["simulated"], true);
        assert_eq!(dry_run["side_effects_performed"], false);
        assert_eq!(dry_run["would_publish_mqtt"], true);
        assert_eq!(dry_run["would_use_dlq"], true);
        assert_eq!(dry_run["planned_sinks"].as_array().unwrap().len(), 2);
        assert_eq!(
            dry_run["node_plan"][0]["config"]["access_token"],
            "***REDACTED***"
        );
        assert_eq!(
            dry_run["planned_sinks"][0]["config"]["password"],
            "***REDACTED***"
        );
        assert_eq!(
            dry_run["planned_sinks"][1]["config"]["credential_ref"],
            "***REDACTED***"
        );

        let proposed_dry_run = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows/dry-run",
                    json!({
                        "flow_key": "proposed-dry-run",
                        "nodes": [
                            {
                                "node_id": "source-1",
                                "node_type": "source",
                                "config": {
                                    "kind": "mqtt_subscribe",
                                    "connector_id": connector_id,
                                    "api_key": "hidden"
                                }
                            },
                            {
                                "node_id": "sink-1",
                                "node_type": "sink",
                                "config": {
                                    "kind": "internal_observation_store"
                                }
                            }
                        ],
                        "edges": [
                            {"source_node_id": "source-1", "target_node_id": "sink-1"}
                        ],
                        "sample_payload": {"n": "temperature", "v": 21.4},
                        "payload_format": "senml-json"
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(proposed_dry_run["side_effects_performed"], false);
        assert_eq!(proposed_dry_run["would_store_observation"], true);
        assert_eq!(
            proposed_dry_run["node_plan"][0]["config"]["api_key"],
            "***REDACTED***"
        );
    }

    #[tokio::test]
    async fn token_mode_protects_flow_validation_and_dry_run_and_enforces_tenants() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let token_app = token_mode_app_with_storage(storage.clone());
        let flow_b =
            create_test_flow(&tenant_b_app, "tenant-b-validate", "Tenant B Validate").await;
        let flow_b_id = flow_b["id"].as_str().unwrap();

        let no_token = token_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/flows/{flow_b_id}/validation"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_token.status(), StatusCode::UNAUTHORIZED);

        let wrong_scope_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("flow-validate-wrong-scope"),
            &["dashboard:read"],
        );
        let wrong_scope = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/flows/{flow_b_id}/validation"),
                &wrong_scope_token,
            ))
            .await
            .unwrap();
        assert_eq!(wrong_scope.status(), StatusCode::FORBIDDEN);

        let tenant_b_token = store_api_token_for_tenant(
            &storage,
            tenant_b,
            ApiTokenPrincipalType::Service,
            Some("flow-validate-reader"),
            &["flows:read"],
        );
        let tenant_b_validation = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/flows/{flow_b_id}/validation"),
                &tenant_b_token,
            ))
            .await
            .unwrap();
        assert_eq!(tenant_b_validation.status(), StatusCode::OK);

        let tenant_a_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("flow-validate-tenant-a"),
            &["flows:read"],
        );
        let cross_tenant_validation = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/flows/{flow_b_id}/validation"),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_validation.status(), StatusCode::FORBIDDEN);

        let cross_tenant_dry_run = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                &format!("/flows/{flow_b_id}/dry-run"),
                json!({"sample_payload": {"x": 1}}),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_dry_run.status(), StatusCode::FORBIDDEN);

        let proposed_validation = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/flows/validate",
                sample_flow_body("token-validate", "Token Validate"),
                &tenant_b_token,
            ))
            .await
            .unwrap();
        assert_eq!(proposed_validation.status(), StatusCode::OK);

        let admin_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Admin,
            Some("flow-admin-validation"),
            &["admin:all"],
        );
        let admin_validation = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/flows/{flow_b_id}/validation"),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(admin_validation.status(), StatusCode::OK);

        let admin_dry_run = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                &format!("/flows/{flow_b_id}/dry-run"),
                json!({"sample_payload": {"x": 1}}),
                &admin_token,
            ))
            .await
            .unwrap();
        assert_eq!(admin_dry_run.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn flow_execute_supports_proposed_and_stored_simulation_without_side_effects() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage.clone());

        let proposed = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows/execute",
                    json!({
                        "flow_key": "execute-proposed",
                        "name": "Execute Proposed",
                        "nodes": [
                            {
                                "node_id": "source-1",
                                "node_type": "source",
                                "config": { "kind": "http_input" }
                            },
                            {
                                "node_id": "decoder-1",
                                "node_type": "decoder",
                                "config": { "kind": "senml_decode" }
                            },
                            {
                                "node_id": "sink-1",
                                "node_type": "sink",
                                "config": { "kind": "internal_observation_store" }
                            }
                        ],
                        "edges": [
                            { "source_node_id": "source-1", "target_node_id": "decoder-1" },
                            { "source_node_id": "decoder-1", "target_node_id": "sink-1" }
                        ],
                        "sample_payload": [
                            { "n": "temperature", "v": 21.4, "u": "Cel" }
                        ],
                        "payload_format": "senml-json"
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(proposed["simulated"], true);
        assert_eq!(proposed["side_effects_performed"], false);
        assert_eq!(proposed["valid"], true);
        assert_eq!(
            proposed["observations_preview"].as_array().unwrap().len(),
            1
        );
        assert_eq!(storage.list_all_observations().unwrap().len(), 0);

        let stored = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows",
                    json!(sample_flow_body_with_nodes(
                        "execute-stored",
                        "Execute Stored",
                        json!([
                            {
                                "node_id": "source-1",
                                "node_type": "source",
                                "config": { "kind": "http_input" }
                            },
                            {
                                "node_id": "event-1",
                                "node_type": "sink",
                                "config": { "kind": "event_create", "event_type": "aion:FlowAlert", "severity": "warning" }
                            },
                            {
                                "node_id": "command-1",
                                "node_type": "sink",
                                "config": { "kind": "command_create", "command_type": "StartPump", "target_entity_id": "target-01" }
                            },
                            {
                                "node_id": "mqtt-1",
                                "node_type": "sink",
                                "config": { "kind": "mqtt_publish", "topic_template": "alerts/{device_id}" }
                            },
                            {
                                "node_id": "http-1",
                                "node_type": "sink",
                                "config": { "kind": "http_forward", "endpoint_url": "https://example.invalid/hook", "method": "POST" }
                            },
                            {
                                "node_id": "dlq-1",
                                "node_type": "dlq",
                                "config": { "kind": "dlq", "failure_stage": "flow_processing", "failure_reason": "preview only" }
                            }
                        ]),
                        json!([
                            { "edge_id": "edge-1", "source_node_id": "source-1", "target_node_id": "event-1" },
                            { "edge_id": "edge-2", "source_node_id": "source-1", "target_node_id": "command-1" },
                            { "edge_id": "edge-3", "source_node_id": "source-1", "target_node_id": "mqtt-1" },
                            { "edge_id": "edge-4", "source_node_id": "source-1", "target_node_id": "http-1" },
                            { "edge_id": "edge-5", "source_node_id": "source-1", "target_node_id": "dlq-1" }
                        ])
                    )),
                ))
                .await
                .unwrap(),
        )
        .await;
        let flow_id = stored["id"].as_str().unwrap();
        let events_before_execute = storage.list_all_events().unwrap().len();

        let executed = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    &format!("/flows/{flow_id}/execute"),
                    json!({
                        "sample_payload": { "temperature": 29.1, "device_id": "sensor-01" },
                        "payload_format": "application/json"
                    }),
                ))
                .await
                .unwrap(),
        )
        .await;
        let actions = executed["sink_results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["action"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(actions.contains(&"would_create_event"));
        assert!(actions.contains(&"would_create_command"));
        assert!(actions.contains(&"would_publish_mqtt"));
        assert!(actions.contains(&"would_forward_http"));
        assert!(actions.contains(&"would_write_dlq"));
        assert_eq!(executed["events_preview"].as_array().unwrap().len(), 1);
        assert_eq!(executed["commands_preview"].as_array().unwrap().len(), 1);
        assert_eq!(executed["dlq_preview"].as_array().unwrap().len(), 1);
        assert_eq!(
            storage.list_all_events().unwrap().len(),
            events_before_execute
        );
        assert_eq!(storage.list_all_commands().unwrap().len(), 0);
        assert_eq!(
            storage
                .list_all_dlq_records(aion_storage::DlqRecordFilter::default())
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn flow_execute_filter_controls_downstream_execution() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = dev_mode_app_with_storage(storage.clone());
        let created = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    "/flows",
                    json!(sample_flow_body_with_nodes(
                        "execute-filter",
                        "Execute Filter",
                        json!([
                            {
                                "node_id": "source-1",
                                "node_type": "source",
                                "config": { "kind": "http_input" }
                            },
                            {
                                "node_id": "filter-1",
                                "node_type": "filter",
                                "config": { "kind": "filter_condition", "field": "temperature", "operator": "gt", "value": 30 }
                            },
                            {
                                "node_id": "sink-1",
                                "node_type": "sink",
                                "config": { "kind": "internal_observation_store", "observed_property": "temperature", "unit": "Cel" }
                            }
                        ]),
                        json!([
                            { "edge_id": "edge-1", "source_node_id": "source-1", "target_node_id": "filter-1" },
                            { "edge_id": "edge-2", "source_node_id": "filter-1", "target_node_id": "sink-1" }
                        ])
                    )),
                ))
                .await
                .unwrap(),
        )
        .await;
        let flow_id = created["id"].as_str().unwrap();

        let passes = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    &format!("/flows/{flow_id}/execute"),
                    json!({ "sample_payload": { "temperature": 31 } }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            passes["sink_results"][0]["action"],
            "would_store_observation"
        );
        assert_eq!(passes["observations_preview"].as_array().unwrap().len(), 1);

        let filtered = to_json(
            app.clone()
                .oneshot(json_request(
                    "POST",
                    &format!("/flows/{flow_id}/execute"),
                    json!({ "sample_payload": { "temperature": 12 } }),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(filtered["sink_results"][0]["action"], "no_op");
        assert_eq!(
            filtered["sink_results"][0]["preview"]["reason"],
            "upstream execution did not continue"
        );
        assert_eq!(
            filtered["observations_preview"].as_array().unwrap().len(),
            0
        );
        assert_eq!(storage.list_all_observations().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn token_mode_protects_flow_execute_and_enforces_tenants() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let token_app = token_mode_app_with_storage(storage.clone());
        let flow_b = create_test_flow(&tenant_b_app, "tenant-b-execute", "Tenant B Execute").await;
        let flow_b_id = flow_b["id"].as_str().unwrap();

        let raw_message = RawMessage::new(
            tenant_b,
            RawMessageSource::Http,
            Some("/ingest/http".to_string()),
            Some("device-01".to_string()),
            Some("senml-json".to_string()),
            Some("application/json".to_string()),
            None,
            None,
            Some("senml-json".to_string()),
            json!({"connector_key": "test"}),
            br#"[{"n":"temperature","v":22.6,"u":"Cel"}]"#.to_vec(),
            Utc::now(),
        )
        .unwrap();
        let raw_message_id = raw_message.id;
        storage.store_raw_message(raw_message).unwrap();

        let no_token = token_app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/flows/{flow_b_id}/execute"),
                json!({"sample_payload": {"x": 1}}),
            ))
            .await
            .unwrap();
        assert_eq!(no_token.status(), StatusCode::UNAUTHORIZED);

        let wrong_scope_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("flow-execute-wrong-scope"),
            &["dashboard:read"],
        );
        let wrong_scope = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                &format!("/flows/{flow_b_id}/execute"),
                json!({"sample_payload": {"x": 1}}),
                &wrong_scope_token,
            ))
            .await
            .unwrap();
        assert_eq!(wrong_scope.status(), StatusCode::FORBIDDEN);

        let tenant_b_token = store_api_token_for_tenant(
            &storage,
            tenant_b,
            ApiTokenPrincipalType::Service,
            Some("flow-execute-reader"),
            &["flows:read"],
        );
        let ok = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                &format!("/flows/{flow_b_id}/execute"),
                json!({ "raw_message_id": raw_message_id }),
                &tenant_b_token,
            ))
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        let ok_body = to_json(ok).await;
        assert_eq!(ok_body["simulated"], true);
        assert_eq!(ok_body["side_effects_performed"], false);
        assert_eq!(ok_body["flow_id"], flow_b["id"]);

        let tenant_a_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("flow-execute-tenant-a"),
            &["flows:read"],
        );
        let cross_tenant = token_app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                &format!("/flows/{flow_b_id}/execute"),
                json!({ "sample_payload": { "x": 1 } }),
                &tenant_a_token,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant.status(), StatusCode::FORBIDDEN);
    }

    async fn create_test_entity(app: &Router, key: &str, entity_type: &str) -> String {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/entities",
                json!({
                    "entity_key": key,
                    "entity_type": entity_type,
                    "jsonld": {
                        "@context": {"aion": "https://aioncore.org/ns#"},
                        "@id": format!("urn:aion:test:{key}"),
                        "@type": entity_type
                    }
                }),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await["id"].as_str().unwrap().to_string()
    }

    async fn create_test_command(
        app: &Router,
        target_entity_id: &str,
        command_type: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/commands",
                json!({
                    "target_entity_id": target_entity_id,
                    "command_type": command_type,
                    "payload": {
                        "target_state": "running"
                    },
                    "requested_by": "test",
                    "reason": "test command"
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_low_water_command_rule(
        app: &Router,
        tank_id: &str,
        pump_id: &str,
        enabled: bool,
        threshold: f64,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/rules",
                json!({
                    "name": "Start pump when level is low",
                    "description": "Generic observation threshold rule",
                    "enabled": enabled,
                    "trigger_type": "observation_created",
                    "target_entity_id": tank_id,
                    "observed_property": "WaterTankLevel",
                    "condition": {
                        "comparison": "less_than",
                        "value": threshold
                    },
                    "action": {
                        "type": "create_command",
                        "target_entity_id": pump_id,
                        "command_type": "StartPump",
                        "payload": {
                            "target_state": "running"
                        },
                        "requested_by": "aion-rule-engine",
                        "reason": "Water tank level is below threshold"
                    },
                    "metadata": {
                        "test": true
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_low_water_event_rule(
        app: &Router,
        tank_id: &str,
        enabled: bool,
        threshold: f64,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/rules",
                json!({
                    "name": "Create low-water event",
                    "enabled": enabled,
                    "trigger_type": "observation_created",
                    "target_entity_id": tank_id,
                    "observed_property": "WaterTankLevel",
                    "condition": {
                        "comparison": "less_than",
                        "value": threshold
                    },
                    "action": {
                        "type": "create_event",
                        "event_type": "aion:LowWaterLevel",
                        "severity": "warning",
                        "target_entity_id": tank_id,
                        "message": "Water tank level is below threshold"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_event_command_rule(app: &Router, tank_id: &str, pump_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/rules",
                json!({
                    "name": "Start pump after low-water event",
                    "enabled": true,
                    "trigger_type": "event_created",
                    "target_entity_id": tank_id,
                    "event_type": "aion:LowWaterLevel",
                    "condition": {
                        "comparison": "equals",
                        "value": "aion:LowWaterLevel"
                    },
                    "action": {
                        "type": "create_command",
                        "target_entity_id": pump_id,
                        "command_type": "StartPump",
                        "payload": {
                            "target_state": "running"
                        },
                        "requested_by": "aion-rule-engine",
                        "reason": "Low-water event detected"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_loop_event_rule(app: &Router, tank_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/rules",
                json!({
                    "name": "Loop event rule",
                    "enabled": true,
                    "trigger_type": "event_created",
                    "target_entity_id": tank_id,
                    "event_type": "aion:Loop",
                    "condition": {
                        "comparison": "equals",
                        "value": "aion:Loop"
                    },
                    "action": {
                        "type": "create_event",
                        "event_type": "aion:Loop",
                        "severity": "warning",
                        "target_entity_id": tank_id,
                        "message": "Loop event generated by rule"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_water_level_observation(app: &Router, tank_id: &str, value: f64) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/observations",
                json!({
                    "producer_entity_id": tank_id,
                    "feature_of_interest_id": tank_id,
                    "observed_property": "WaterTankLevel",
                    "value": {
                        "type": "number",
                        "value": value
                    },
                    "unit": "%",
                    "observed_at": "2026-04-28T12:00:00Z",
                    "received_at": "2026-04-28T12:00:01Z",
                    "protocol": "http",
                    "payload_format": "json_mapping",
                    "quality": {},
                    "metadata": {}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_test_event(
        app: &Router,
        event_type: &str,
        target_entity_id: Option<&str>,
        metadata: Value,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/events",
                json!({
                    "event_type": event_type,
                    "severity": "warning",
                    "target_entity_id": target_entity_id,
                    "message": "test event",
                    "occurred_at": "2026-04-28T12:00:00Z",
                    "metadata": metadata
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn query_pending_commands(app: &Router, target_entity_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/commands?target_entity_id={target_entity_id}&status=pending"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn query_events_by_type(app: &Router, event_type: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/events?event_type={event_type}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn query_events_by_raw_message(app: &Router, raw_message_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/events?raw_message_id={raw_message_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn query_raw_messages(app: &Router) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/raw-messages")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn create_test_observation_at(
        app: &Router,
        producer_entity_id: &str,
        feature_of_interest_id: &str,
        observed_property: &str,
        value: Value,
        unit: Option<&str>,
        observed_at: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/observations",
                json!({
                    "producer_entity_id": producer_entity_id,
                    "feature_of_interest_id": feature_of_interest_id,
                    "observed_property": observed_property,
                    "value": value,
                    "unit": unit,
                    "observed_at": observed_at,
                    "received_at": observed_at,
                    "protocol": "http",
                    "payload_format": "json_mapping",
                    "raw_message_id": null,
                    "quality": {},
                    "metadata": {"suite": "timeseries"}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn query_observations_by_feature(app: &Router, feature_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/observations?feature_of_interest_id={feature_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn put_start_pump_policy(app: &Router, pump_id: &str, requires_approval: bool) -> Value {
        put_command_policy(app, pump_id, "StartPump", requires_approval).await
    }

    async fn put_command_policy(
        app: &Router,
        target_entity_id: &str,
        command_type: &str,
        requires_approval: bool,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                "/policies",
                json!([
                    {
                        "target_entity_id": target_entity_id,
                        "command_type": command_type,
                        "requires_approval": requires_approval,
                        "auto_execute_allowed": false,
                        "metadata": {
                            "source": "test"
                        }
                    }
                ]),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn smartsentinel_service_entity(app: &Router) -> String {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/snapshots",
                smartsentinel_sample_snapshot(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let entities = get_json(app, "/entities").await;
        entity_id_by_key(&entities, "smartsentinel:fog-01:service:mosquitto")
    }

    async fn register_smartsentinel_executor(
        app: &Router,
        agent_key: &str,
        target_entity_id: &str,
        command_types: &[&str],
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/integrations/smartsentinel/executors/register",
                json!({
                    "agent_key": agent_key,
                    "display_name": agent_key,
                    "capabilities": command_types,
                    "scopes": [
                        {
                            "target_entity_id": target_entity_id,
                            "metadata": {"source": "test"}
                        }
                    ],
                    "metadata": {
                        "test": true
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn poll_smartsentinel_commands(app: &Router, executor_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/integrations/smartsentinel/executors/{executor_id}/commands"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn claim_smartsentinel_command(
        app: &Router,
        executor_id: &str,
        command_id: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!(
                    "/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/claim"
                ),
                json!({
                    "lease_duration_seconds": 60,
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn report_smartsentinel_command(
        app: &Router,
        executor_id: &str,
        command_id: &str,
        status: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!(
                    "/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/report"
                ),
                json!({
                    "action_type": if status == "executed" {
                        "sentinel:RunDiagnostic"
                    } else {
                        "sentinel:RestartService"
                    },
                    "status": status,
                    "verified": status == "executed",
                    "result_payload": {
                        "dry_run": true,
                        "detail": "reported by test executor"
                    },
                    "evidence_refs": ["ev-log-1"],
                    "incident_id": "inc-001",
                    "alert_id": "alert-001",
                    "workflow_id": "wf-remediate",
                    "run_id": "run-42",
                    "trace_id": "trace-abc",
                    "correlation_id": "corr-123",
                    "message": if status == "failed" {
                        "SmartSentinel dry-run execution failed"
                    } else {
                        "SmartSentinel dry-run execution reported"
                    },
                    "metadata": {
                        "operator": "test"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn create_test_executor(app: &Router, agent_key: &str) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/executors",
                json!({
                    "agent_key": agent_key,
                    "agent_type": "edge",
                    "display_name": agent_key,
                    "status": "online",
                    "metadata": {
                        "test": true
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_compatible_executor(app: &Router, agent_key: &str, pump_id: &str) -> Value {
        let executor = create_test_executor(app, agent_key).await;
        let executor_id = executor["id"].as_str().unwrap();
        put_executor_capabilities(app, executor_id, &["StartPump"]).await;
        put_executor_scope_for_target(app, executor_id, pump_id).await;
        executor
    }

    async fn put_executor_capabilities(
        app: &Router,
        executor_id: &str,
        command_types: &[&str],
    ) -> Value {
        let capabilities = command_types
            .iter()
            .map(|command_type| {
                json!({
                    "command_type": command_type,
                    "protocol": "local",
                    "metadata": {
                        "test": true
                    }
                })
            })
            .collect::<Vec<_>>();
        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/executors/{executor_id}/capabilities"),
                json!(capabilities),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn put_executor_scope_for_target(
        app: &Router,
        executor_id: &str,
        target_entity_id: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "PUT",
                &format!("/executors/{executor_id}/scopes"),
                json!([
                    {
                        "target_entity_id": target_entity_id,
                        "metadata": {
                            "test": true
                        }
                    }
                ]),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn poll_executor_commands(app: &Router, executor_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/executors/{executor_id}/commands/pending"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn approve_test_command(app: &Router, command_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/commands/{command_id}/approve"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn claim_executor_test_command(
        app: &Router,
        executor_id: &str,
        command_id: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!(
                        "/executors/{executor_id}/commands/{command_id}/claim"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn claim_executor_test_command_with_lease(
        app: &Router,
        executor_id: &str,
        command_id: &str,
        lease_duration_seconds: i64,
        max_retries: Option<u32>,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/executors/{executor_id}/commands/{command_id}/claim"),
                json!({
                    "lease_duration_seconds": lease_duration_seconds,
                    "max_retries": max_retries,
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn get_command_lease(app: &Router, command_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/commands/{command_id}/lease"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn refresh_command_lease(
        app: &Router,
        command_id: &str,
        executor_id: &str,
        lease_duration_seconds: i64,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/lease/refresh"),
                json!({
                    "executor_id": executor_id,
                    "lease_duration_seconds": lease_duration_seconds
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn release_command_lease(app: &Router, command_id: &str, executor_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/lease/release"),
                json!({
                    "executor_id": executor_id
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn recover_expired_leases(app: &Router) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/commands/recover-expired-leases")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    fn smartsentinel_sample_snapshot() -> Value {
        json!({
            "snapshot_id": "snap-001",
            "node_id": "fog-01",
            "observed_at": "2026-04-29T12:00:00Z",
            "entities": [
                {
                    "id": "host:fog-01",
                    "type": "sentinel:Host",
                    "name": "fog-01",
                    "properties": {}
                },
                {
                    "id": "service:mosquitto",
                    "type": "sentinel:Service",
                    "name": "mosquitto",
                    "status": "healthy",
                    "properties": {}
                }
            ],
            "relationships": [
                {
                    "source": "host:fog-01",
                    "type": "sentinel:runs",
                    "target": "service:mosquitto"
                }
            ],
            "observations": [
                {
                    "entity_id": "service:mosquitto",
                    "observed_property": "sentinel:ServiceStatus",
                    "value": "healthy",
                    "observed_at": "2026-04-29T12:00:01Z"
                }
            ],
            "events": [
                {
                    "event_type": "sentinel:ServiceDegraded",
                    "target_entity_id": "service:mosquitto",
                    "severity": "warning",
                    "message": "API service degraded"
                }
            ]
        })
    }

    fn smartsentinel_snapshot_with_provenance() -> Value {
        json!({
            "snapshot_id": "snap-prov-001",
            "node_id": "fog-02",
            "observed_at": "2026-04-29T13:00:00Z",
            "source": {
                "agent_id": "agent-fog-02",
                "agent_version": "0.4.2",
                "host_id": "fog-02",
                "environment": "fog",
                "collector": "smartsentinel-snapshot"
            },
            "provenance": {
                "run_id": "run-42",
                "cycle_id": "cycle-7",
                "trace_id": "trace-abc",
                "correlation_id": "corr-123",
                "workflow_id": "wf-remediate",
                "external_refs": [
                    {"system": "incident-platform", "external_id": "inc-001"}
                ]
            },
            "evidence": [
                {
                    "evidence_id": "ev-log-1",
                    "evidence_type": "log",
                    "title": "API error log",
                    "uri": "https://evidence.example.invalid/logs/api",
                    "external_id": "log-001",
                    "collected_at": "2026-04-29T13:00:02Z",
                    "related_entity_id": "service:api",
                    "metadata": {"line_count": 20}
                },
                {
                    "evidence_id": "ev-metric-1",
                    "evidence_type": "metric",
                    "title": "Latency p95",
                    "external_id": "metric-001",
                    "related_entity_id": "service:api"
                }
            ],
            "entities": [
                {
                    "id": "host:fog-02",
                    "type": "sentinel:Host",
                    "name": "fog-02",
                    "properties": {}
                },
                {
                    "id": "service:api",
                    "type": "sentinel:Service",
                    "name": "api",
                    "status": "degraded",
                    "properties": {}
                }
            ],
            "relationships": [
                {
                    "source": "host:fog-02",
                    "type": "sentinel:runs",
                    "target": "service:api"
                }
            ],
            "observations": [
                {
                    "entity_id": "service:api",
                    "observed_property": "sentinel:LatencyP95",
                    "value": 832.0,
                    "unit": "ms",
                    "observed_at": "2026-04-29T13:00:03Z",
                    "evidence_refs": ["ev-metric-1"],
                    "source": {"collector": "metrics-summary"}
                }
            ],
            "events": [
                {
                    "event_type": "sentinel:IncidentOpened",
                    "target_entity_id": "service:api",
                    "severity": "warning",
                    "message": "API latency degraded",
                    "incident_id": "inc-001",
                    "alert_id": "alert-001",
                    "workflow_id": "wf-remediate",
                    "run_id": "run-42",
                    "trace_id": "trace-abc",
                    "evidence_refs": ["ev-log-1"]
                }
            ]
        })
    }

    async fn get_json(app: &Router, uri: &str) -> Value {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn create_native_entity(app: &Router, entity: Value) -> Value {
        let response = app
            .clone()
            .oneshot(json_request("POST", "/entities", entity))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    #[tokio::test]
    async fn dev_mode_dlq_create_list_get_update_and_dashboard_counts() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_id = Uuid::new_v4();
        let app = dev_mode_app_with_storage_for_tenant(storage, tenant_id);

        let first = create_test_dlq_record(
            &app,
            sample_dlq_body("decode-01", "pending", "decoding", "minifi"),
        )
        .await;
        let second = create_test_dlq_record(
            &app,
            sample_dlq_body("validation-01", "resolved", "validation", "nifi"),
        )
        .await;

        let listed = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/dlq/records")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(listed.as_array().unwrap().len(), 2);

        for (uri, expected_id) in [
            ("/dlq/records?status=pending", first["id"].as_str().unwrap()),
            (
                "/dlq/records?failure_stage=decoding",
                first["id"].as_str().unwrap(),
            ),
            (
                "/dlq/records?source_system=minifi",
                first["id"].as_str().unwrap(),
            ),
            (
                "/dlq/records?idempotency_key=tenant-a:decode-01",
                first["id"].as_str().unwrap(),
            ),
            (
                "/dlq/records?external_flowfile_uuid=flowfile-decode-01",
                first["id"].as_str().unwrap(),
            ),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_json(response).await;
            assert_eq!(body.as_array().unwrap().len(), 1, "filter uri {uri}");
            assert_eq!(body[0]["id"], expected_id, "filter uri {uri}");
        }

        let detail = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/dlq/records/{}", first["id"].as_str().unwrap()))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(detail["dlq_key"], "decode-01");

        let replay_requested = to_json(
            app.clone()
                .oneshot(json_request(
                    "PATCH",
                    &format!("/dlq/records/{}/status", first["id"].as_str().unwrap()),
                    json!({"status": "replay_requested"}),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(replay_requested["status"], "replay_requested");

        let ignored = to_json(
            app.clone()
                .oneshot(json_request(
                    "PATCH",
                    &format!("/dlq/records/{}/status", first["id"].as_str().unwrap()),
                    json!({"status": "ignored"}),
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(ignored["status"], "ignored");
        assert!(ignored["resolved_at"].is_string());

        let overview = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/dashboard/overview")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(overview["dlq_pending_count"], 0);
        assert_eq!(overview["dlq_total_count"], 2);
        assert_eq!(second["status"], "resolved");

        let events = to_json(
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri("/events")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        let event_types = events
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["event_type"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(event_types.contains(&"aion:DlqRecordCreated"));
        assert!(event_types.contains(&"aion:DlqReplayRequested"));
        assert!(event_types.contains(&"aion:DlqRecordIgnored"));
    }

    #[tokio::test]
    async fn token_mode_dlq_read_requires_auth_and_scope() {
        let storage = Arc::new(InMemoryStorage::new());
        let app = token_mode_app_with_storage(storage.clone());
        let wrong_scope = store_api_token_for_tenant(
            &storage,
            Uuid::new_v4(),
            ApiTokenPrincipalType::Service,
            Some("svc-no-dlq"),
            &["dashboard:read"],
        );
        let read_scope = store_api_token_for_tenant(
            &storage,
            Uuid::new_v4(),
            ApiTokenPrincipalType::Service,
            Some("svc-dlq-read"),
            &["dlq:read"],
        );

        let no_token = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/dlq/records")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_token.status(), StatusCode::UNAUTHORIZED);

        let wrong_scope_response = app
            .clone()
            .oneshot(auth_request("GET", "/dlq/records", &wrong_scope))
            .await
            .unwrap();
        assert_eq!(wrong_scope_response.status(), StatusCode::FORBIDDEN);

        let read_scope_response = app
            .clone()
            .oneshot(auth_request("GET", "/dlq/records", &read_scope))
            .await
            .unwrap();
        assert_eq!(read_scope_response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_mode_dlq_write_scope_can_create_record() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_id = Uuid::new_v4();
        let app = token_mode_app_with_storage(storage.clone());
        let token = store_api_token_for_tenant(
            &storage,
            tenant_id,
            ApiTokenPrincipalType::Service,
            Some("svc-dlq-write"),
            &["dlq:write"],
        );

        let response = app
            .clone()
            .oneshot(auth_json_request(
                "POST",
                "/dlq/records",
                sample_dlq_body("token-create-01", "pending", "decoding", "minifi"),
                &token,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_json(response).await;
        assert_eq!(body["tenant_id"], tenant_id.to_string());
    }

    #[tokio::test]
    async fn token_mode_dlq_tenant_filtering_and_cross_tenant_update_denial() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let token_app = token_mode_app_with_storage(storage.clone());

        let record_a = create_test_dlq_record(
            &tenant_a_app,
            sample_dlq_body("tenant-a-record", "pending", "decoding", "minifi"),
        )
        .await;
        let record_b = create_test_dlq_record(
            &tenant_b_app,
            sample_dlq_body("tenant-b-record", "pending", "validation", "nifi"),
        )
        .await;
        let read_token_a = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-read"),
            &["dlq:read"],
        );
        let write_token_a = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Service,
            Some("tenant-a-write"),
            &["dlq:write"],
        );

        let list = to_json(
            token_app
                .clone()
                .oneshot(auth_request("GET", "/dlq/records", &read_token_a))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(list.as_array().unwrap().len(), 1);
        assert_eq!(list[0]["id"], record_a["id"]);

        let cross_tenant_get = token_app
            .clone()
            .oneshot(auth_request(
                "GET",
                &format!("/dlq/records/{}", record_b["id"].as_str().unwrap()),
                &read_token_a,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_get.status(), StatusCode::FORBIDDEN);

        let cross_tenant_patch = token_app
            .clone()
            .oneshot(auth_json_request(
                "PATCH",
                &format!("/dlq/records/{}/status", record_b["id"].as_str().unwrap()),
                json!({"status": "resolved"}),
                &write_token_a,
            ))
            .await
            .unwrap();
        assert_eq!(cross_tenant_patch.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn token_mode_admin_can_read_and_manage_dlq_across_tenants() {
        let storage = Arc::new(InMemoryStorage::new());
        let tenant_a = Uuid::new_v4();
        let tenant_b = Uuid::new_v4();
        let tenant_a_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_a);
        let tenant_b_app = dev_mode_app_with_storage_for_tenant(storage.clone(), tenant_b);
        let token_app = token_mode_app_with_storage(storage.clone());

        create_test_dlq_record(
            &tenant_a_app,
            sample_dlq_body("tenant-a-admin", "pending", "decoding", "minifi"),
        )
        .await;
        let record_b = create_test_dlq_record(
            &tenant_b_app,
            sample_dlq_body("tenant-b-admin", "pending", "validation", "nifi"),
        )
        .await;
        let admin_token = store_api_token_for_tenant(
            &storage,
            tenant_a,
            ApiTokenPrincipalType::Admin,
            Some("admin-all"),
            &["admin:all"],
        );

        let list = to_json(
            token_app
                .clone()
                .oneshot(auth_request("GET", "/dlq/records", &admin_token))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(list.as_array().unwrap().len(), 2);

        let updated = to_json(
            token_app
                .clone()
                .oneshot(auth_json_request(
                    "PATCH",
                    &format!("/dlq/records/{}/status", record_b["id"].as_str().unwrap()),
                    json!({"status": "resolved"}),
                    &admin_token,
                ))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(updated["status"], "resolved");
    }

    fn entity_id_by_key(entities: &Value, entity_key: &str) -> String {
        entity_by_key(entities, entity_key)["id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn entity_by_key<'a>(entities: &'a Value, entity_key: &str) -> &'a Value {
        entities
            .as_array()
            .unwrap()
            .iter()
            .find(|entity| entity["entity_key"] == entity_key)
            .unwrap()
    }

    async fn complete_executor_test_command(
        app: &Router,
        executor_id: &str,
        command_id: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/executors/{executor_id}/commands/{command_id}/complete"),
                json!({
                    "result_payload": {
                        "pump_state": "running"
                    },
                    "verified": true,
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn fail_executor_test_command(
        app: &Router,
        executor_id: &str,
        command_id: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/executors/{executor_id}/commands/{command_id}/fail"),
                json!({
                    "failure_reason": "executor timeout",
                    "result_payload": {
                        "error": "timeout"
                    },
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn claim_test_command(app: &Router, command_id: &str, claimed_by: &str) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/commands/{command_id}/claim"),
                json!({
                    "claimed_by": claimed_by
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn ingest_test_senml(app: &Router, sensor_id: &str, plot_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingest/http",
                json!({
                    "producer_entity_id": sensor_id,
                    "feature_of_interest_id": plot_id,
                    "payload_format": "senml-json",
                    "protocol": "http",
                    "content_type": "application/senml+json",
                    "payload": [
                        {
                            "bn": "urn:aion:farm:01:soil-sensor:01:",
                            "bt": 1777294800,
                            "n": "soil_moisture",
                            "u": "%",
                            "v": 18.5
                        },
                        {
                            "n": "soil_temperature",
                            "u": "Cel",
                            "v": 24.1
                        }
                    ]
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_http_connector(
        app: &Router,
        connector_key: &str,
        producer_entity_id: Option<&str>,
        feature_of_interest_id: Option<&str>,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": connector_key,
                    "connector_type": "http",
                    "connector_profile": "custom",
                    "enabled": true,
                    "protocol": "http",
                    "endpoint": "/ingestion/connectors/{connector_id}/ingest",
                    "http_path": "/ingestion/connectors/{connector_id}/ingest",
                    "payload_format": "senml-json",
                    "content_type": "application/senml+json",
                    "default_producer_entity_id": producer_entity_id,
                    "default_feature_of_interest_id": feature_of_interest_id
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_test_flow(app: &Router, flow_key: &str, name: &str) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/flows",
                sample_flow_body(flow_key, name),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_test_dlq_record(app: &Router, body: Value) -> Value {
        let response = app
            .clone()
            .oneshot(json_request("POST", "/dlq/records", body))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    fn sample_dlq_body(
        dlq_key: &str,
        status: &str,
        failure_stage: &str,
        source_system: &str,
    ) -> Value {
        json!({
            "dlq_key": dlq_key,
            "source_system": source_system,
            "source_id": format!("{source_system}-source"),
            "idempotency_key": format!("tenant-a:{dlq_key}"),
            "external_flow_id": "flow-edge-sync",
            "external_flow_name": "Edge Sync",
            "external_flowfile_uuid": format!("flowfile-{dlq_key}"),
            "external_process_group_id": "pg-01",
            "external_processor_id": "proc-01",
            "external_provenance_uri": "nifi://provenance/events/123",
            "sync_session_id": format!("sync-{dlq_key}"),
            "payload_format": "senml-json",
            "payload": [{"n": "temperature", "v": 21.4}],
            "payload_hash": "sha256:test",
            "failure_stage": failure_stage,
            "failure_reason": "decoder rejected payload",
            "failure_detail": "invalid field shape",
            "retry_count": 2,
            "replay_count": 1,
            "status": status,
            "metadata": {
                "external.source_system": source_system,
                "note": "test"
            }
        })
    }

    fn sample_flow_body(flow_key: &str, name: &str) -> Value {
        sample_flow_body_with_nodes(
            flow_key,
            name,
            json!([
                {
                    "node_id": "source-1",
                    "node_type": "source",
                    "name": "MQTT Source",
                    "config": {
                        "kind": "mqtt_subscribe",
                        "connector_id": "connector-01",
                        "topic_filter": "devices/+/up"
                    },
                    "position": { "x": 10.0, "y": 20.0 }
                },
                {
                    "node_id": "decoder-1",
                    "node_type": "decoder",
                    "name": "SenML Decoder",
                    "config": { "kind": "senml_decode" },
                    "position": { "x": 120.0, "y": 20.0 }
                },
                {
                    "node_id": "sink-1",
                    "node_type": "sink",
                    "name": "Store",
                    "config": { "kind": "internal_observation_store" },
                    "position": { "x": 240.0, "y": 20.0 }
                }
            ]),
            json!([
                {
                    "edge_id": "edge-1",
                    "source_node_id": "source-1",
                    "target_node_id": "decoder-1"
                },
                {
                    "edge_id": "edge-2",
                    "source_node_id": "decoder-1",
                    "target_node_id": "sink-1"
                }
            ]),
        )
    }

    fn sample_flow_body_with_nodes(
        flow_key: &str,
        name: &str,
        nodes: Value,
        edges: Value,
    ) -> Value {
        json!({
            "flow_key": flow_key,
            "name": name,
            "description": "flow test",
            "enabled": false,
            "nodes": nodes,
            "edges": edges,
            "metadata": {
                "source_convention": "mqtt_subscribe",
                "sink_convention": "internal_observation_store"
            }
        })
    }

    async fn create_ttn_connector(
        app: &Router,
        connector_key: &str,
        producer_entity_id: &str,
        feature_of_interest_id: &str,
    ) -> Value {
        create_ttn_connector_with_defaults(
            app,
            connector_key,
            Some(producer_entity_id),
            Some(feature_of_interest_id),
        )
        .await
    }

    async fn create_ttn_connector_with_defaults(
        app: &Router,
        connector_key: &str,
        producer_entity_id: Option<&str>,
        feature_of_interest_id: Option<&str>,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": connector_key,
                    "connector_type": "mqtt",
                    "connector_profile": "ttn-v3",
                    "enabled": true,
                    "broker_url": "mqtt://eu1.cloud.thethings.network:1883",
                    "client_id": format!("{connector_key}-client"),
                    "topic_filter": "v3/demo-app/devices/+/up",
                    "payload_format": "ttn-uplink-json",
                    "content_type": "application/json",
                    "default_producer_entity_id": producer_entity_id,
                    "default_feature_of_interest_id": feature_of_interest_id,
                    "metadata": {
                        "unit_mapping": {
                            "temperature": "Cel",
                            "soil_moisture": "%"
                        }
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_ttn_connector_with_secret(
        app: &Router,
        connector_key: &str,
        secret_ref_id: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": connector_key,
                    "connector_type": "mqtt",
                    "connector_profile": "ttn-v3",
                    "enabled": true,
                    "broker_url": "mqtt://eu1.cloud.thethings.network:1883",
                    "client_id": format!("{connector_key}-client"),
                    "topic_filter": "v3/demo-app/devices/+/up",
                    "payload_format": "ttn-uplink-json",
                    "secret_ref_id": secret_ref_id
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_ttn_device_mapping(
        app: &Router,
        connector_id: &str,
        ttn_application_id: Option<&str>,
        ttn_device_id: &str,
        producer_entity_id: &str,
        feature_of_interest_id: Option<&str>,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ttn-device-mappings"),
                json!({
                    "ttn_application_id": ttn_application_id,
                    "ttn_device_id": ttn_device_id,
                    "producer_entity_id": producer_entity_id,
                    "feature_of_interest_id": feature_of_interest_id,
                    "metadata": {
                        "source": "test"
                    }
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    fn ttn_uplink_payload() -> Value {
        json!({
            "end_device_ids": {
                "device_id": "soil-node-01",
                "application_ids": {
                    "application_id": "farm-app"
                }
            },
            "received_at": "2026-04-29T12:00:00Z",
            "uplink_message": {
                "received_at": "2026-04-29T12:01:02Z",
                "f_port": 1,
                "f_cnt": 42,
                "frm_payload": "AQID",
                "decoded_payload": {
                    "temperature": 21.5,
                    "state": "ok",
                    "battery_low": false,
                    "location": {
                        "lat": -23.5,
                        "lon": -46.6
                    }
                },
                "rx_metadata": [
                    {
                        "gateway_ids": {
                            "gateway_id": "gw-1"
                        },
                        "rssi": -71
                    }
                ],
                "settings": {
                    "data_rate": {
                        "lora": {
                            "spreading_factor": 7
                        }
                    }
                }
            }
        })
    }

    async fn create_mqtt_connector(
        app: &Router,
        connector_key: &str,
        connector_profile: &str,
        enabled: bool,
        broker_url: Option<&str>,
        topic_filter: Option<&str>,
        payload_format: Option<&str>,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": connector_key,
                    "connector_type": "mqtt",
                    "connector_profile": connector_profile,
                    "enabled": enabled,
                    "broker_url": broker_url,
                    "client_id": format!("{connector_key}-client"),
                    "topic_filter": topic_filter,
                    "payload_format": payload_format
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_mqtt_connector_with_secret(
        app: &Router,
        connector_key: &str,
        enabled: bool,
        broker_url: Option<&str>,
        topic_filter: Option<&str>,
        payload_format: Option<&str>,
        secret_ref_id: Option<&str>,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/ingestion/connectors",
                json!({
                    "connector_key": connector_key,
                    "connector_type": "mqtt",
                    "connector_profile": "generic-mqtt",
                    "enabled": enabled,
                    "broker_url": broker_url,
                    "client_id": format!("{connector_key}-client"),
                    "topic_filter": topic_filter,
                    "payload_format": payload_format,
                    "secret_ref_id": secret_ref_id
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    async fn create_connector_secret(
        app: &Router,
        secret_key: &str,
        username: &str,
        secret_value: &str,
    ) -> Value {
        create_connector_secret_with_type(
            app,
            secret_key,
            "mqtt_basic_auth",
            Some(username),
            secret_value,
        )
        .await
    }

    async fn create_connector_secret_with_type(
        app: &Router,
        secret_key: &str,
        secret_type: &str,
        username: Option<&str>,
        secret_value: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                "/secrets/connectors",
                json!({
                    "secret_key": secret_key,
                    "secret_type": secret_type,
                    "username": username,
                    "secret_value": secret_value,
                    "metadata": {"suite": "api"}
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        to_json(response).await
    }

    fn token_mode_app_with_storage(storage: Arc<InMemoryStorage>) -> Router {
        app_with_state(AppState::with_backend_storage_and_auth(
            storage,
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Token,
                bootstrap_admin_token_hash: None,
            },
            Uuid::nil(),
        ))
    }

    fn dev_mode_app_with_storage_for_tenant(
        storage: Arc<InMemoryStorage>,
        tenant_id: Uuid,
    ) -> Router {
        app_with_state(AppState::with_backend_storage_and_auth(
            storage,
            StorageBackendName::Memory,
            AuthConfig::default(),
            tenant_id,
        ))
    }

    fn dev_mode_app_with_storage(storage: Arc<InMemoryStorage>) -> Router {
        app_with_state(AppState::with_backend_storage_and_auth(
            storage,
            StorageBackendName::Memory,
            AuthConfig::default(),
            Uuid::nil(),
        ))
    }

    fn token_mode_app_with_bootstrap(
        storage: Arc<InMemoryStorage>,
        bootstrap_token: &str,
    ) -> Router {
        app_with_state(AppState::with_backend_storage_and_auth(
            storage,
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Token,
                bootstrap_admin_token_hash: Some(hash_token_value(bootstrap_token)),
            },
            Uuid::nil(),
        ))
    }

    fn disabled_mode_app_with_storage(storage: Arc<InMemoryStorage>) -> Router {
        app_with_state(AppState::with_backend_storage_and_auth(
            storage,
            StorageBackendName::Memory,
            AuthConfig {
                mode: AuthMode::Disabled,
                bootstrap_admin_token_hash: None,
            },
            Uuid::nil(),
        ))
    }

    fn store_api_token(
        storage: &Arc<InMemoryStorage>,
        principal_type: ApiTokenPrincipalType,
        principal_id: Option<&str>,
        scopes: &[&str],
    ) -> String {
        store_api_token_for_tenant(storage, Uuid::nil(), principal_type, principal_id, scopes)
    }

    fn store_api_token_for_tenant(
        storage: &Arc<InMemoryStorage>,
        tenant_id: Uuid,
        principal_type: ApiTokenPrincipalType,
        principal_id: Option<&str>,
        scopes: &[&str],
    ) -> String {
        let issued = issue_api_token();
        let token = ApiToken::new(
            tenant_id,
            "test-token",
            issued.token_prefix.clone(),
            issued.token_hash,
            principal_type,
            principal_id.map(ToOwned::to_owned),
            scopes.iter().map(|scope| (*scope).to_string()).collect(),
            None,
            Some(json!({"suite": "auth"})),
            Utc::now(),
        )
        .unwrap();
        storage.create_api_token(token).unwrap();
        issued.raw_token
    }

    async fn get_worker_plan(app: &Router) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ingestion/workers/plan")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn validate_connector(app: &Router, connector_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/ingestion/connectors/{connector_id}/validate"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn get_ttn_live_readiness_plan(app: &Router, connector_id: &str) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/ingestion/connectors/{connector_id}/ttn-live-readiness-plan"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn post_ttn_live_validate(app: &Router, connector_id: &str, body: Value) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "POST",
                &format!("/ingestion/connectors/{connector_id}/ttn-live-validate"),
                body,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    async fn patch_connector_secret_ref(
        app: &Router,
        connector_id: &str,
        secret_id: &str,
    ) -> Value {
        let response = app
            .clone()
            .oneshot(json_request(
                "PATCH",
                &format!("/ingestion/connectors/{connector_id}"),
                json!({
                    "secret_ref_id": secret_id
                }),
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    fn validation_issue_codes<'a>(validation: &'a Value, field: &str) -> Vec<&'a str> {
        validation[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|issue| issue["code"].as_str().unwrap())
            .collect()
    }

    fn plan_has_check(plan: &Value, check_key: &str, status: &str) -> bool {
        plan["checks"].as_array().unwrap().iter().any(|check| {
            check["check_key"] == check_key
                && check["status"] == status
                && check["future_live_check"].is_boolean()
        })
    }

    async fn get_worker_status(app: &Router) -> Value {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ingestion/workers/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        to_json(response).await
    }

    fn json_request(method: &str, uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn auth_request(method: &str, uri: &str, token: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    }

    fn auth_json_request(method: &str, uri: &str, body: Value, token: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn json_ld_request(method: &str, uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/ld+json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn to_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn response_text(response: axum::response::Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn create_temp_dashboard_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = env::temp_dir().join(format!("aioncore-dashboard-static-{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
