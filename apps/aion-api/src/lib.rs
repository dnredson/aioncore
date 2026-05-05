use aion_action::{
    Action, ActionResult, ApprovalStatus, Capability, Command, CommandLease, CommandStatus,
    ExecutorAgent, ExecutorAgentStatus, ExecutorCapability, ExecutorScope, Policy,
};
use aion_entity::Entity;
use aion_event::{Event, EventSeverity};
use aion_mcp::{ToolDefinition, ToolRequest, ToolResponse};
use aion_observation::{Observation, ObservationValue};
use aion_payload::{
    CanonicalJsonDecoder, DecodeInput, DecodedMeasurement, PayloadDecoder, PayloadFormat,
    SenMlJsonDecoder, TtnUplinkJsonDecoder, UltraLightDecoder,
};
use aion_raw_message::{NormalizationStatus, RawMessage, RawMessageSource};
use aion_relationship::Relationship;
use aion_rule::{Rule, RuleAction, RuleCondition, RuleEvaluationResult, RuleTriggerType};
#[cfg(test)]
use aion_storage::ApiTokenPrincipalType;
use aion_storage::{
    ApiToken, ConnectorProfile, ConnectorSecret, ConnectorSecretType, EventFilter, InMemoryStorage,
    IngestionConnector, IngestionConnectorType, PayloadProfile, PostgresStorage,
    PostgresStorageConfig, StorageBackend, StorageError, TtnDeviceMapping,
};
use axum::{
    body::Bytes,
    extract::{Extension, Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::Response,
    routing::{get, post, put},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    env,
    str::FromStr,
    sync::{Arc, RwLock},
    time::Instant,
};
use tokio::time;
use uuid::Uuid;

mod auth;
mod error;
mod mqtt_ingest;
mod routes;

#[cfg(test)]
use auth::hash_token_value;
#[cfg(test)]
use auth::issue_api_token;
use auth::{
    deny_cross_tenant_write, is_admin_all, map_principal_type_from_storage, principal_tenant_id,
    principal_tenant_or_default, require_any_scope, require_scope, require_scope_for_write,
    resolve_auth_context, tenant_for_created_resource, AuthContext, TokenRejectionReason,
};
pub use auth::{AuthConfig, AuthEnforcementLevel, AuthMode, PrincipalType};
use error::ApiError;

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

#[derive(Debug)]
struct ConnectorWorkerHandle {
    signature: ConnectorWorkerSignature,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConnectorWorkerSignature {
    broker_url: Option<String>,
    client_id: Option<String>,
    topic_filter: Option<String>,
    payload_format: Option<String>,
    content_type: Option<String>,
    secret_ref_id: Option<Uuid>,
    connector_profile: ConnectorProfile,
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

fn state_for_tenant(state: &AppState, tenant_id: Uuid) -> AppState {
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
struct ReadyWorkerPlanSummary {
    planned_workers: usize,
    invalid_workers: usize,
    unsupported_workers: usize,
}

#[derive(Debug, Deserialize)]
pub struct CreateEntityRequest {
    pub entity_key: String,
    pub entity_type: String,
    pub jsonld: Value,
}

#[derive(Debug)]
struct EntityInput {
    entity_key: String,
    entity_type: String,
    jsonld: Value,
}

#[derive(Debug, Deserialize)]
pub struct CreateRelationshipRequest {
    pub source_entity_id: Uuid,
    pub relationship_type: String,
    pub target_entity_id: Uuid,
    #[serde(default = "empty_object")]
    pub jsonld: Value,
}

#[derive(Debug, Deserialize)]
pub struct CreateObservationRequest {
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Uuid,
    pub observed_property: String,
    pub value: ObservationValue,
    pub unit: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub protocol: String,
    pub payload_format: String,
    pub raw_message_id: Option<Uuid>,
    #[serde(default = "empty_object")]
    pub quality: Value,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

#[derive(Debug, Deserialize)]
pub struct HttpIngestRequest {
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Uuid,
    pub payload_format: String,
    pub protocol: String,
    pub content_type: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
    pub payload: Value,
    pub mapping: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ConnectorHttpIngestRequest {
    pub producer_entity_id: Option<Uuid>,
    pub feature_of_interest_id: Option<Uuid>,
    pub payload_format: Option<String>,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
    pub payload: Value,
    pub mapping: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct HttpIngestResponse {
    pub raw_message_id: Uuid,
    pub observations: Vec<Observation>,
}

const SMARTSENTINEL_PAYLOAD_FORMAT: &str = "smartsentinel-snapshot-json";

#[derive(Debug, Deserialize)]
pub struct SmartSentinelSnapshot {
    pub snapshot_id: String,
    pub node_id: String,
    pub observed_at: Option<DateTime<Utc>>,
    pub source: Option<Value>,
    pub provenance: Option<Value>,
    #[serde(default)]
    pub evidence: Vec<Value>,
    #[serde(default)]
    pub entities: Vec<SmartSentinelSnapshotEntity>,
    #[serde(default)]
    pub relationships: Vec<SmartSentinelSnapshotRelationship>,
    #[serde(default)]
    pub observations: Vec<SmartSentinelSnapshotObservation>,
    #[serde(default)]
    pub events: Vec<SmartSentinelSnapshotEvent>,
}

#[derive(Debug, Deserialize)]
pub struct SmartSentinelSnapshotEntity {
    pub id: String,
    #[serde(rename = "type")]
    pub entity_type: String,
    pub name: Option<String>,
    pub status: Option<String>,
    #[serde(default = "empty_object")]
    pub properties: Value,
}

#[derive(Debug, Deserialize)]
pub struct SmartSentinelSnapshotRelationship {
    pub source: String,
    #[serde(rename = "type")]
    pub relationship_type: String,
    pub target: String,
}

#[derive(Debug, Deserialize)]
pub struct SmartSentinelSnapshotObservation {
    pub entity_id: String,
    pub observed_property: String,
    pub value: Value,
    pub unit: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
    pub evidence_refs: Option<Value>,
    pub source: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct SmartSentinelSnapshotEvent {
    pub event_type: String,
    pub target_entity_id: Option<String>,
    pub source_entity_id: Option<String>,
    pub severity: Option<EventSeverity>,
    pub message: Option<String>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub incident_id: Option<String>,
    pub alert_id: Option<String>,
    pub workflow_id: Option<String>,
    pub run_id: Option<String>,
    pub trace_id: Option<String>,
    pub evidence_refs: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct SmartSentinelSnapshotResponse {
    pub raw_message_id: Uuid,
    pub snapshot_id: String,
    pub node_id: String,
    pub entities_created: usize,
    pub entities_updated: usize,
    pub entities_reused: usize,
    pub entities_skipped: usize,
    pub relationships_created: usize,
    pub relationships_reused: usize,
    pub relationships_skipped: usize,
    pub observations_created: usize,
    pub events_created: usize,
    pub validation_warnings: Vec<SmartSentinelValidationIssue>,
    pub validation_errors: Vec<SmartSentinelValidationIssue>,
    pub skipped_items: Vec<SmartSentinelSkippedItem>,
    pub provenance_present: bool,
    pub evidence_count: usize,
    pub external_ref_count: usize,
    pub correlation_id: Option<String>,
    pub trace_id: Option<String>,
    pub run_id: Option<String>,
    pub cycle_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SmartSentinelValidationIssue {
    pub path: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SmartSentinelSkippedItem {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SmartSentinelValidationReport {
    warnings: Vec<SmartSentinelValidationIssue>,
    errors: Vec<SmartSentinelValidationIssue>,
    skipped_items: Vec<SmartSentinelSkippedItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SmartSentinelEntityMappingStatus {
    Created,
    Updated,
    Reused,
}

#[derive(Debug, Clone)]
struct SmartSentinelProvenanceSummary {
    provenance_present: bool,
    evidence_count: usize,
    external_ref_count: usize,
    correlation_id: Option<String>,
    trace_id: Option<String>,
    run_id: Option<String>,
    cycle_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedTtnDeviceMapping {
    mapping_id: Uuid,
    ttn_device_id: String,
    ttn_application_id: Option<String>,
    mapping_resolution: &'static str,
}

#[derive(Debug, Clone)]
struct TtnUplinkIds {
    device_id: String,
    application_id: Option<String>,
}

enum TtnDeviceMappingResolution {
    Resolved {
        mapping: TtnDeviceMapping,
        resolution: &'static str,
    },
    Missing,
    Ambiguous(String),
}

#[derive(Debug, Deserialize)]
pub struct CreateIngestionConnectorRequest {
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: ConnectorProfile,
    #[serde(default)]
    pub enabled: bool,
    pub display_name: Option<String>,
    pub protocol: Option<String>,
    pub endpoint: Option<String>,
    pub broker_url: Option<String>,
    pub client_id: Option<String>,
    pub topic_filter: Option<String>,
    pub http_path: Option<String>,
    pub payload_format: Option<String>,
    pub content_type: Option<String>,
    pub secret_ref_id: Option<Uuid>,
    pub default_producer_entity_id: Option<Uuid>,
    pub default_feature_of_interest_id: Option<Uuid>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateIngestionConnectorRequest {
    pub display_name: Option<String>,
    pub enabled: Option<bool>,
    pub protocol: Option<String>,
    pub endpoint: Option<String>,
    pub broker_url: Option<String>,
    pub client_id: Option<String>,
    pub topic_filter: Option<String>,
    pub http_path: Option<String>,
    pub payload_format: Option<String>,
    pub content_type: Option<String>,
    pub secret_ref_id: Option<Uuid>,
    pub default_producer_entity_id: Option<Uuid>,
    pub default_feature_of_interest_id: Option<Uuid>,
    pub metadata: Option<Value>,
}

#[derive(Deserialize)]
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
pub struct CreateTtnDeviceMappingRequest {
    pub ttn_application_id: Option<String>,
    pub ttn_device_id: String,
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Option<Uuid>,
    pub enabled: Option<bool>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateTtnDeviceMappingRequest {
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub ttn_application_id: Option<Option<String>>,
    pub ttn_device_id: Option<String>,
    pub producer_entity_id: Option<Uuid>,
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub feature_of_interest_id: Option<Option<Uuid>>,
    pub enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub metadata: Option<Option<Value>>,
}

#[derive(Debug, Serialize)]
pub struct TtnDeviceMappingResponse {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub connector_id: Uuid,
    pub ttn_application_id: Option<String>,
    pub ttn_device_id: String,
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Option<Uuid>,
    pub enabled: bool,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn deserialize_optional_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Serialize)]
pub struct IngestionConnectorStatusResponse {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: ConnectorProfile,
    pub enabled: bool,
    pub status: &'static str,
    pub last_error: Option<String>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_successful_ingest_at: Option<DateTime<Utc>>,
    pub last_failed_ingest_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TtnConnectorReadiness {
    Ready,
    Degraded,
    Invalid,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TtnConnectorValidationIssue {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TtnConnectorValidation {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub valid: bool,
    pub readiness: TtnConnectorReadiness,
    pub issues: Vec<TtnConnectorValidationIssue>,
    pub warnings: Vec<TtnConnectorValidationIssue>,
    pub detected_profile: ConnectorProfile,
    pub expected_topic_shape: &'static str,
    pub mapping_count: usize,
    pub enabled_mapping_count: usize,
    pub has_secret_ref: bool,
    pub secret_configured: bool,
    pub secret_type: Option<ConnectorSecretType>,
    pub payload_format_supported: bool,
    pub operator_hints: Vec<String>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TtnLiveReadinessCheckStatus {
    Pass,
    Warn,
    Fail,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub struct TtnLiveReadinessCheck {
    pub check_key: &'static str,
    pub description: &'static str,
    pub status: TtnLiveReadinessCheckStatus,
    pub reason: Option<String>,
    pub future_live_check: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TtnLiveReadinessPlan {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub dry_run: bool,
    pub can_attempt_live_validation: bool,
    pub readiness: TtnConnectorReadiness,
    pub checks: Vec<TtnLiveReadinessCheck>,
    pub blockers: Vec<TtnConnectorValidationIssue>,
    pub warnings: Vec<TtnConnectorValidationIssue>,
    pub required_operator_steps: Vec<String>,
    pub safe_to_connect: bool,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct TtnLiveValidationRequest {
    pub timeout_seconds: Option<u64>,
    pub expect_message: Option<bool>,
    pub client_id_suffix: Option<String>,
    pub dry_run_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TtnLiveValidationResultStatus {
    Success,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub struct TtnLiveValidationPlanSummary {
    pub safe_to_connect: bool,
    pub can_attempt_live_validation: bool,
    pub readiness: TtnConnectorReadiness,
    pub blocker_count: usize,
    pub warning_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TtnLiveValidationResponse {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub attempted_live_connection: bool,
    pub dry_run_passed: bool,
    pub connected: bool,
    pub subscribed: bool,
    pub message_received: bool,
    pub broker_url_redacted_or_safe: Option<String>,
    pub topic_filter: Option<String>,
    pub duration_ms: u128,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub result: TtnLiveValidationResultStatus,
    pub errors: Vec<TtnConnectorValidationIssue>,
    pub warnings: Vec<TtnConnectorValidationIssue>,
    pub dry_run_plan_summary: TtnLiveValidationPlanSummary,
    pub secret_exposed: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionWorkerKind {
    HttpListener,
    MqttSubscriber,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionWorkerSpecStatus {
    Planned,
    Skipped,
    Invalid,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestionWorkerValidationIssue {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct IngestionWorkerSpec {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: ConnectorProfile,
    pub enabled: bool,
    pub worker_kind: IngestionWorkerKind,
    pub broker_url: Option<String>,
    pub client_id: Option<String>,
    pub topic_filter: Option<String>,
    pub http_path: Option<String>,
    pub payload_format: Option<String>,
    pub content_type: Option<String>,
    pub secret_ref_id: Option<Uuid>,
    pub status: IngestionWorkerSpecStatus,
    pub validation_issues: Vec<IngestionWorkerValidationIssue>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct IngestionWorkerPlan {
    pub specs: Vec<IngestionWorkerSpec>,
    pub planned_workers: usize,
    pub skipped_workers: usize,
    pub invalid_workers: usize,
    pub unsupported_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorWorkerConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorWorkerEnvValues {
    pub enabled: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorWorkerRuntimeState {
    Planned,
    Starting,
    Running,
    Reconnecting,
    Degraded,
    Stopped,
    Skipped,
    Invalid,
    Error,
    Unsupported,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorWorkerRuntimeStatus {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: ConnectorProfile,
    pub enabled: bool,
    pub worker_kind: IngestionWorkerKind,
    pub status: ConnectorWorkerRuntimeState,
    pub connected: bool,
    pub subscribed: bool,
    pub broker_url: Option<String>,
    pub client_id: Option<String>,
    pub topic_filter: Option<String>,
    pub http_path: Option<String>,
    pub payload_format: Option<String>,
    pub content_type: Option<String>,
    pub secret_ref_id: Option<Uuid>,
    pub last_error: Option<String>,
    pub last_message_at: Option<DateTime<Utc>>,
    pub last_successful_ingest_at: Option<DateTime<Utc>>,
    pub last_failed_ingest_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub restart_count: u32,
    pub reconnect_attempts: u32,
    pub last_disconnect_at: Option<DateTime<Utc>>,
    pub last_reconnect_at: Option<DateTime<Utc>>,
    pub next_reconnect_at: Option<DateTime<Utc>>,
    pub last_reconciled_at: Option<DateTime<Utc>>,
    pub validation_issues: Vec<IngestionWorkerValidationIssue>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorWorkersReadiness {
    pub enabled: bool,
    pub total: usize,
    pub running: usize,
    pub degraded: usize,
    pub stopped: usize,
    pub skipped: usize,
    pub invalid: usize,
    pub errors: usize,
}

#[derive(Debug, Serialize)]
pub struct IngestionWorkersStatusResponse {
    pub connector_workers: ConnectorWorkersReadiness,
    pub workers: Vec<ConnectorWorkerRuntimeStatus>,
}

#[derive(Debug, Serialize)]
pub struct ReconcileConnectorWorkersResponse {
    pub connector_workers: ConnectorWorkersReadiness,
    pub actions: Vec<ConnectorWorkerReconcileAction>,
    pub workers: Vec<ConnectorWorkerRuntimeStatus>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConnectorWorkerReconcileAction {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub action: String,
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PutPayloadProfileRequest {
    pub payload_format: String,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub attribute_mapping: Option<Value>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct PutCapabilityRequest {
    pub capability_name: String,
    pub command_type: String,
    pub protocol: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct CreateExecutorRequest {
    pub agent_key: String,
    pub agent_type: String,
    pub display_name: Option<String>,
    pub status: Option<ExecutorAgentStatus>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ExecutorHeartbeatRequest {
    pub status: ExecutorAgentStatus,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ExecutorClaimCommandRequest {
    pub lease_duration_seconds: Option<i64>,
    pub max_retries: Option<u32>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct PutExecutorCapabilityRequest {
    pub command_type: String,
    pub protocol: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct PutExecutorScopeRequest {
    pub target_entity_id: Option<Uuid>,
    pub entity_type: Option<String>,
    pub relationship_type: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ExecutorCompleteCommandRequest {
    pub result_payload: Value,
    pub verified: Option<bool>,
    pub status: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ExecutorFailCommandRequest {
    pub failure_reason: String,
    pub result_payload: Option<Value>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct ExecutorCommandCompletionResponse {
    pub command: Command,
    pub action: Action,
    pub action_result: ActionResult,
}

#[derive(Debug, Deserialize)]
pub struct RegisterSmartSentinelExecutorRequest {
    pub agent_key: String,
    pub display_name: Option<String>,
    pub metadata: Option<Value>,
    #[serde(default)]
    pub capabilities: Vec<SmartSentinelExecutorCapabilityRequest>,
    #[serde(default)]
    pub scopes: Vec<PutExecutorScopeRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum SmartSentinelExecutorCapabilityRequest {
    CommandType(String),
    Detailed {
        command_type: String,
        protocol: Option<String>,
        metadata: Option<Value>,
    },
}

#[derive(Debug, Serialize)]
pub struct RegisterSmartSentinelExecutorResponse {
    pub executor: ExecutorAgent,
    pub reused: bool,
    pub capabilities: Vec<ExecutorCapability>,
    pub scopes: Vec<ExecutorScope>,
}

#[derive(Debug, Serialize)]
pub struct SmartSentinelCommandEnvelope {
    pub command: Command,
    pub latest_lease: Option<CommandLease>,
    pub target_entity: Option<Entity>,
    pub recent_provenance: Vec<Value>,
}

#[derive(Debug, Deserialize)]
pub struct SmartSentinelCommandReportRequest {
    pub action_type: String,
    pub status: String,
    pub verified: bool,
    pub result_payload: Value,
    pub evidence_refs: Option<Value>,
    pub incident_id: Option<String>,
    pub alert_id: Option<String>,
    pub workflow_id: Option<String>,
    pub run_id: Option<String>,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub message: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct SmartSentinelCommandReportResponse {
    pub command: Command,
    pub action: Action,
    pub action_result: ActionResult,
    pub event: Event,
}

#[derive(Debug, Deserialize)]
pub struct RefreshCommandLeaseRequest {
    pub executor_id: Uuid,
    pub lease_duration_seconds: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseCommandLeaseRequest {
    pub executor_id: Uuid,
}

#[derive(Debug, Serialize)]
pub struct RecoverExpiredLeasesResponse {
    pub expired_lease_ids: Vec<Uuid>,
    pub retried_command_ids: Vec<Uuid>,
    pub failed_command_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct CreateCommandRequest {
    pub target_entity_id: Uuid,
    pub command_type: String,
    pub payload: Value,
    pub requested_by: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClaimCommandRequest {
    pub claimed_by: String,
}

#[derive(Debug, Deserialize)]
pub struct MarkFailedCommandRequest {
    pub failure_reason: String,
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
pub struct CommandQuery {
    pub target_entity_id: Option<Uuid>,
    pub status: Option<CommandStatus>,
}

#[derive(Debug, Deserialize)]
pub struct CreateActionRequest {
    pub command_id: Uuid,
    pub executor_entity_id: Option<Uuid>,
    pub action_type: String,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ActionQuery {
    pub command_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct CreateActionResultRequest {
    pub command_id: Uuid,
    pub action_id: Uuid,
    pub status: String,
    pub verified: bool,
    pub result_payload: Value,
    pub observed_at: DateTime<Utc>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ActionResultQuery {
    pub action_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct CreateEventRequest {
    pub event_type: String,
    pub severity: EventSeverity,
    pub source_entity_id: Option<Uuid>,
    pub target_entity_id: Option<Uuid>,
    pub message: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub observed_at: Option<DateTime<Utc>>,
    pub correlation_id: Option<String>,
    pub raw_message_id: Option<Uuid>,
    pub observation_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
    pub action_id: Option<Uuid>,
    pub action_result_id: Option<Uuid>,
    pub metadata: Option<Value>,
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

#[derive(Debug, Deserialize)]
pub struct EventQuery {
    pub source_entity_id: Option<Uuid>,
    pub target_entity_id: Option<Uuid>,
    pub event_type: Option<String>,
    pub severity: Option<EventSeverity>,
    pub command_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub incident_id: Option<String>,
    pub alert_id: Option<String>,
    pub trace_id: Option<String>,
    pub run_id: Option<String>,
    pub workflow_id: Option<String>,
    pub cycle_id: Option<String>,
    pub evidence_id: Option<String>,
    pub external_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ObservationQuery {
    pub feature_of_interest_id: Option<Uuid>,
    pub observed_property: Option<String>,
    pub raw_message_id: Option<Uuid>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct RawMessageQuery {
    pub producer_entity_id: Option<Uuid>,
    pub feature_of_interest_id: Option<Uuid>,
    pub payload_format: Option<String>,
    pub trace_id: Option<String>,
    pub run_id: Option<String>,
    pub workflow_id: Option<String>,
    pub cycle_id: Option<String>,
    pub correlation_id: Option<String>,
    pub snapshot_id: Option<String>,
    pub node_id: Option<String>,
    pub connector_id: Option<Uuid>,
    pub connector_key: Option<String>,
    pub connector_profile: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ProvenanceSearchQuery {
    pub incident_id: Option<String>,
    pub alert_id: Option<String>,
    pub trace_id: Option<String>,
    pub run_id: Option<String>,
    pub workflow_id: Option<String>,
    pub cycle_id: Option<String>,
    pub correlation_id: Option<String>,
    pub snapshot_id: Option<String>,
    pub node_id: Option<String>,
    pub evidence_id: Option<String>,
    pub external_id: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct AiContextQuery {
    pub include_observations: Option<bool>,
    pub include_events: Option<bool>,
    pub include_commands: Option<bool>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct McpRecentObservationsArgs {
    pub feature_of_interest_id: Option<Uuid>,
    pub producer_entity_id: Option<Uuid>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct McpEventsArgs {
    pub entity_id: Option<Uuid>,
    pub event_type: Option<String>,
    pub severity: Option<EventSeverity>,
    pub command_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct RawMessageResponse {
    pub id: Uuid,
    pub raw_message_id: Uuid,
    pub source_type: RawMessageSource,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub payload_format: Option<String>,
    pub connector_id: Option<Uuid>,
    pub connector_key: Option<String>,
    pub connector_profile: Option<String>,
    pub source_endpoint: Option<String>,
    pub topic_or_path: Option<String>,
    pub producer_entity_id: Option<Uuid>,
    pub feature_of_interest_id: Option<Uuid>,
    pub received_at: DateTime<Utc>,
    pub normalization_status: NormalizationStatus,
    pub normalization_error: Option<String>,
    pub decoder_metadata: Value,
    pub payload: Value,
}

#[derive(Debug, Serialize)]
pub struct EntityContextResponse {
    pub entity: Entity,
    pub outgoing_relationships: Vec<Relationship>,
    pub incoming_relationships: Vec<Relationship>,
}

#[derive(Debug, Serialize)]
pub struct AiEntityContextResponse {
    pub target_entity: Entity,
    pub outgoing_relationships: Vec<Relationship>,
    pub incoming_relationships: Vec<Relationship>,
    pub recent_observations: Vec<Observation>,
    pub recent_events: Vec<Event>,
    pub related_commands: Vec<Command>,
    pub related_actions: Vec<Action>,
    pub related_action_results: Vec<ActionResult>,
    pub raw_message_refs: Vec<Uuid>,
    pub generated_at: DateTime<Utc>,
    pub metadata: Value,
}

#[derive(Debug, Serialize)]
pub struct ProvenanceSearchResponse {
    pub matching_events: Vec<Event>,
    pub matching_raw_messages: Vec<RawMessageResponse>,
    pub matching_observations: Vec<Observation>,
    pub counts: ProvenanceSearchCounts,
    pub query: Value,
}

#[derive(Debug, Serialize)]
pub struct ProvenanceSearchCounts {
    pub matching_events: usize,
    pub matching_raw_messages: usize,
    pub matching_observations: usize,
}

const DEFAULT_COMMAND_LEASE_SECONDS: i64 = 60;

pub fn app() -> Router {
    app_with_state(AppState::local())
}

pub fn app_from_env() -> Result<Router, StartupError> {
    Ok(app_with_state(AppState::from_env()?))
}

pub fn app_from_env_with_diagnostics() -> Result<(Router, StartupDiagnostics), StartupError> {
    let (state, diagnostics) = AppState::from_env_with_diagnostics()?;
    Ok((app_with_state(state), diagnostics))
}

impl ConnectorWorkerConfig {
    pub fn from_env() -> Result<Self, StartupError> {
        Self::from_env_values(ConnectorWorkerEnvValues {
            enabled: env::var("AIONCORE_CONNECTOR_WORKERS_ENABLED").ok(),
        })
    }

    pub fn from_env_values(values: ConnectorWorkerEnvValues) -> Result<Self, StartupError> {
        Ok(Self {
            enabled: parse_bool_env_value(
                values.enabled.as_deref(),
                false,
                "AIONCORE_CONNECTOR_WORKERS_ENABLED",
            )?,
        })
    }
}

pub async fn start_mqtt_ingest_if_enabled(state: AppState) -> Result<(), StartupError> {
    mqtt_ingest::start_if_enabled(state).await
}

pub async fn start_connector_workers_if_enabled(state: AppState) -> Result<(), StartupError> {
    let config = ConnectorWorkerConfig::from_env()?;
    start_connector_workers(state, config).await
}

pub fn app_with_state(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .merge(routes::auth::router())
        .route("/entities", post(create_entity).get(list_entities))
        .route("/entities/:entity_id", get(get_entity))
        .route("/entities/:entity_id/context", get(get_entity_context))
        .route(
            "/entities/:entity_id/capabilities",
            put(put_capabilities).get(get_capabilities),
        )
        .route(
            "/entities/:entity_id/payload-profile",
            put(put_payload_profile).get(get_payload_profile),
        )
        .route("/relationships", post(create_relationship))
        .route("/policies", put(put_policies).get(query_policies))
        .route("/rules", post(create_rule).get(list_rules))
        .route("/rules/evaluate", post(evaluate_rules_manually))
        .route("/rules/:rule_id", get(get_rule))
        .route("/rules/:rule_id/enable", put(enable_rule))
        .route("/rules/:rule_id/disable", put(disable_rule))
        .route("/executors", post(create_executor).get(list_executors))
        .route("/executors/:executor_id", get(get_executor))
        .route("/executors/:executor_id/heartbeat", put(heartbeat_executor))
        .route(
            "/executors/:executor_id/capabilities",
            put(put_executor_capabilities).get(get_executor_capabilities),
        )
        .route(
            "/executors/:executor_id/scopes",
            put(put_executor_scopes).get(get_executor_scopes),
        )
        .route(
            "/executors/:executor_id/commands/pending",
            get(poll_executor_pending_commands),
        )
        .route(
            "/executors/:executor_id/commands/:command_id/claim",
            post(claim_executor_command),
        )
        .route(
            "/executors/:executor_id/commands/:command_id/complete",
            post(complete_executor_command),
        )
        .route(
            "/executors/:executor_id/commands/:command_id/fail",
            post(fail_executor_command),
        )
        .merge(routes::adapters::router())
        .route("/commands", post(create_command).get(query_commands))
        .route(
            "/commands/recover-expired-leases",
            post(recover_expired_leases),
        )
        .route("/commands/:command_id/lease", get(get_command_lease))
        .route(
            "/commands/:command_id/lease/refresh",
            post(refresh_command_lease),
        )
        .route(
            "/commands/:command_id/lease/release",
            post(release_command_lease),
        )
        .route("/commands/:command_id/claim", post(claim_command))
        .route("/commands/:command_id/release", post(release_command))
        .route(
            "/commands/:command_id/mark-executed",
            post(mark_command_executed),
        )
        .route(
            "/commands/:command_id/mark-failed",
            post(mark_command_failed),
        )
        .route("/commands/:command_id/cancel", post(cancel_command))
        .route("/commands/:command_id/approve", post(approve_command))
        .route("/commands/:command_id/reject", post(reject_command))
        .route("/commands/:command_id", get(get_command))
        .route("/actions", post(create_action).get(query_actions))
        .route("/actions/:action_id", get(get_action))
        .route(
            "/action-results",
            post(create_action_result).get(query_action_results),
        )
        .route("/events", post(create_event).get(query_events))
        .route("/events/:event_id", get(get_event))
        .route(
            "/secrets/connectors",
            post(create_connector_secret).get(list_connector_secrets),
        )
        .route(
            "/secrets/connectors/:secret_id",
            get(get_connector_secret).delete(delete_connector_secret),
        )
        .route("/ai/context/entity/:entity_id", get(get_ai_entity_context))
        .route("/mcp", post(handle_mcp_json_rpc))
        .route("/mcp/tools", get(list_mcp_tools))
        .route("/mcp/tools/:tool_name", post(invoke_mcp_tool))
        .route("/provenance/search", get(search_provenance))
        .route(
            "/ingestion/connectors",
            post(create_ingestion_connector).get(list_ingestion_connectors),
        )
        .route(
            "/ingestion/connectors/:connector_id",
            get(get_ingestion_connector).patch(update_ingestion_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/enable",
            put(enable_ingestion_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/disable",
            put(disable_ingestion_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/status",
            get(get_ingestion_connector_status),
        )
        .route(
            "/ingestion/connectors/:connector_id/validate",
            get(validate_ingestion_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-live-readiness-plan",
            get(get_ttn_live_readiness_plan),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-live-validate",
            post(ttn_live_validate_connector),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-device-mappings",
            post(create_ttn_device_mapping).get(list_ttn_device_mappings),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id",
            get(get_ttn_device_mapping)
                .patch(update_ttn_device_mapping)
                .delete(delete_ttn_device_mapping),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id/enable",
            put(enable_ttn_device_mapping),
        )
        .route(
            "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id/disable",
            put(disable_ttn_device_mapping),
        )
        .route(
            "/ingestion/connectors/:connector_id/ingest",
            post(ingest_http_for_connector),
        )
        .route("/ingestion/workers/plan", get(get_ingestion_worker_plan))
        .route(
            "/ingestion/workers/status",
            get(get_ingestion_workers_status),
        )
        .route(
            "/ingestion/workers/reconcile",
            post(reconcile_ingestion_workers),
        )
        .route("/ingest/http", post(ingest_http))
        .route(
            "/integrations/smartsentinel/snapshots",
            post(ingest_smartsentinel_snapshot),
        )
        .route(
            "/integrations/smartsentinel/executors/register",
            post(register_smartsentinel_executor),
        )
        .route(
            "/integrations/smartsentinel/executors/:executor_id/commands",
            get(poll_smartsentinel_executor_commands),
        )
        .route(
            "/integrations/smartsentinel/executors/:executor_id/commands/:command_id/claim",
            post(claim_smartsentinel_executor_command),
        )
        .route(
            "/integrations/smartsentinel/executors/:executor_id/commands/:command_id/report",
            post(report_smartsentinel_executor_command),
        )
        .route("/raw-messages", get(query_raw_messages))
        .route("/raw-messages/:raw_message_id", get(get_raw_message))
        .route(
            "/observations",
            post(create_observation).get(query_observations),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_context_middleware,
        ))
        .with_state(state)
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

async fn create_entity(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<Value>,
) -> Result<(StatusCode, Json<Entity>), ApiError> {
    require_scope_for_write(&state, &auth, "/entities", "entities:write")?;
    let request = parse_entity_input(request)?;
    let tenant_id = tenant_for_created_resource(&state, &auth)?;
    let entity = Entity::new(
        tenant_id,
        request.entity_key,
        request.entity_type,
        request.jsonld,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let entity = state.storage.create_entity(entity)?;
    Ok((StatusCode::CREATED, Json(entity)))
}

fn parse_entity_input(value: Value) -> Result<EntityInput, ApiError> {
    if value.get("jsonld").is_some() {
        let request: CreateEntityRequest =
            serde_json::from_value(value).map_err(|err| ApiError::bad_request(err.to_string()))?;
        return Ok(EntityInput {
            entity_key: request.entity_key,
            entity_type: request.entity_type,
            jsonld: request.jsonld,
        });
    }

    let object = value
        .as_object()
        .ok_or_else(|| ApiError::bad_request("entity request must be a JSON object"))?;

    if !object.contains_key("@context") {
        return Err(ApiError::bad_request(
            "native JSON-LD entity must include @context",
        ));
    }

    let jsonld_id = object
        .get("@id")
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::bad_request("native JSON-LD entity must include string @id"))?;
    let entity_type = extract_jsonld_type(object.get("@type"))
        .ok_or_else(|| ApiError::bad_request("native JSON-LD entity must include string @type"))?;
    let entity_key = extract_jsonld_entity_key(object)
        .or_else(|| derive_entity_key(jsonld_id))
        .ok_or_else(|| {
            ApiError::bad_request("could not derive entity_key from native JSON-LD @id")
        })?;

    Ok(EntityInput {
        entity_key,
        entity_type,
        jsonld: value,
    })
}

fn extract_jsonld_type(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        Some(Value::Array(values)) => values
            .iter()
            .find_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        _ => None,
    }
}

fn extract_jsonld_entity_key(object: &serde_json::Map<String, Value>) -> Option<String> {
    object
        .get("entity_key")
        .or_else(|| object.get("aion:entityKey"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn derive_entity_key(jsonld_id: &str) -> Option<String> {
    let trimmed = jsonld_id.trim();
    if trimmed.is_empty() {
        return None;
    }

    let segments = trimmed
        .split(['/', '#', ':'])
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    let last = segments.last()?;

    if is_generic_numeric_suffix(last) {
        return segments
            .iter()
            .rev()
            .skip(1)
            .find(|segment| !is_generic_numeric_suffix(segment))
            .map(|prefix| format!("{prefix}-{last}"));
    }

    Some((*last).to_string())
}

fn is_generic_numeric_suffix(segment: &str) -> bool {
    segment.chars().all(|character| character.is_ascii_digit())
}

async fn get_entity(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<Entity>, ApiError> {
    require_scope(&state, &auth, "/entities/:entity_id", "entities:read")?;
    let entity = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_entity(state.tenant_id, entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_entity_any_tenant(entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_entity(tenant_id, entity_id)? {
            Some(entity) => entity,
            None => {
                if state.storage.get_entity_any_tenant(entity_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /entities/:entity_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(entity))
}

async fn list_entities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<Entity>>, ApiError> {
    require_scope(&state, &auth, "/entities", "entities:read")?;
    let entities = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_entities(state.tenant_id)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_entities()?
    } else {
        state.storage.list_entities(principal_tenant_id(&auth)?)?
    };
    Ok(Json(entities))
}

async fn create_relationship(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateRelationshipRequest>,
) -> Result<(StatusCode, Json<Relationship>), ApiError> {
    require_scope_for_write(&state, &auth, "/relationships", "relationships:write")?;
    require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/relationships",
        request.source_entity_id,
    )?;
    require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/relationships",
        request.target_entity_id,
    )?;
    let tenant_id = tenant_for_created_resource(&state, &auth)?;

    let relationship = Relationship::new(
        tenant_id,
        request.source_entity_id,
        request.relationship_type,
        request.target_entity_id,
        request.jsonld,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let relationship = state.storage.create_relationship(relationship)?;
    Ok((StatusCode::CREATED, Json(relationship)))
}

async fn put_payload_profile(
    State(state): State<AppState>,
    Path(entity_id): Path<Uuid>,
    Json(request): Json<PutPayloadProfileRequest>,
) -> Result<(StatusCode, Json<PayloadProfile>), ApiError> {
    ensure_entity_exists(&state, entity_id)?;
    let profile = PayloadProfile::new(
        entity_id,
        request.payload_format,
        request.protocol,
        request.content_type,
        request.attribute_mapping,
        request.metadata,
    )?;
    let profile = state
        .storage
        .put_payload_profile(state.tenant_id, profile)?;

    Ok((StatusCode::OK, Json(profile)))
}

async fn get_payload_profile(
    State(state): State<AppState>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<PayloadProfile>, ApiError> {
    ensure_entity_exists(&state, entity_id)?;
    let profile = state
        .storage
        .get_payload_profile(state.tenant_id, entity_id)?
        .ok_or_else(ApiError::not_found)?;

    Ok(Json(profile))
}

async fn get_entity_context(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<EntityContextResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/entities/:entity_id/context",
        "entities:read",
    )?;
    let entity = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_entity(state.tenant_id, entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_entity_any_tenant(entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_entity(tenant_id, entity_id)? {
            Some(entity) => entity,
            None => {
                if state.storage.get_entity_any_tenant(entity_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /entities/:entity_id/context",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    let outgoing_relationships = state
        .storage
        .list_relationships(entity.tenant_id, Some(entity_id), None)?
        .into_iter()
        .filter(|relationship| {
            state
                .storage
                .get_entity(relationship.tenant_id, relationship.source_entity_id)
                .ok()
                .flatten()
                .is_some()
                && state
                    .storage
                    .get_entity(relationship.tenant_id, relationship.target_entity_id)
                    .ok()
                    .flatten()
                    .is_some()
        })
        .collect::<Vec<_>>();
    let incoming_relationships = state
        .storage
        .list_relationships(entity.tenant_id, None, Some(entity_id))?
        .into_iter()
        .filter(|relationship| {
            state
                .storage
                .get_entity(relationship.tenant_id, relationship.source_entity_id)
                .ok()
                .flatten()
                .is_some()
                && state
                    .storage
                    .get_entity(relationship.tenant_id, relationship.target_entity_id)
                    .ok()
                    .flatten()
                    .is_some()
        })
        .collect::<Vec<_>>();

    Ok(Json(EntityContextResponse {
        entity,
        outgoing_relationships,
        incoming_relationships,
    }))
}

async fn get_ai_entity_context(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
    Query(query): Query<AiContextQuery>,
) -> Result<Json<AiEntityContextResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ai/context/entity/:entity_id",
        "ai:context:read",
    )?;
    Ok(Json(build_ai_entity_context(&state, entity_id, query)?))
}

fn build_ai_entity_context(
    state: &AppState,
    entity_id: Uuid,
    query: AiContextQuery,
) -> Result<AiEntityContextResponse, ApiError> {
    let target_entity = state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .ok_or_else(ApiError::not_found)?;

    let limit = query.limit.unwrap_or(10);
    let include_observations = query.include_observations.unwrap_or(true);
    let include_events = query.include_events.unwrap_or(true);
    let include_commands = query.include_commands.unwrap_or(true);

    let outgoing_relationships =
        state
            .storage
            .list_relationships(state.tenant_id, Some(entity_id), None)?;
    let incoming_relationships =
        state
            .storage
            .list_relationships(state.tenant_id, None, Some(entity_id))?;

    let recent_observations = if include_observations {
        state.storage.query_observations(
            state.tenant_id,
            Some(entity_id),
            None,
            None,
            None,
            limit,
        )?
    } else {
        Vec::new()
    };

    let recent_events = if include_events {
        query_events_for_entity(&state, entity_id, limit)?
    } else {
        Vec::new()
    };

    let related_commands = if include_commands {
        state
            .storage
            .query_commands(state.tenant_id, Some(entity_id), None)?
            .into_iter()
            .take(limit as usize)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let mut related_actions = Vec::new();
    let mut related_action_results = Vec::new();
    if include_commands {
        for command in &related_commands {
            related_actions.extend(
                state
                    .storage
                    .query_actions(state.tenant_id, Some(command.id))?,
            );
            related_action_results.extend(state.storage.query_action_results(
                state.tenant_id,
                None,
                Some(command.id),
            )?);
        }
        related_actions.sort_by(|left, right| {
            right
                .started_at
                .cmp(&left.started_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        related_action_results.sort_by(|left, right| {
            right
                .observed_at
                .cmp(&left.observed_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        related_actions.truncate(limit as usize);
        related_action_results.truncate(limit as usize);
    }

    let mut raw_message_refs = Vec::new();
    for raw_message_id in recent_observations
        .iter()
        .filter_map(|observation| observation.raw_message_id)
        .chain(
            recent_events
                .iter()
                .filter_map(|event| event.raw_message_id),
        )
    {
        if !raw_message_refs.contains(&raw_message_id) {
            raw_message_refs.push(raw_message_id);
        }
    }

    Ok(AiEntityContextResponse {
        target_entity,
        outgoing_relationships,
        incoming_relationships,
        recent_observations,
        recent_events,
        related_commands,
        related_actions,
        related_action_results,
        raw_message_refs,
        generated_at: Utc::now(),
        metadata: json!({
            "builder": "aion:AiContextBuilder",
            "domain_agnostic": true,
            "llm_invoked": false,
            "include_observations": include_observations,
            "include_events": include_events,
            "include_commands": include_commands,
            "limit": limit
        }),
    })
}

fn query_events_for_entity(
    state: &AppState,
    entity_id: Uuid,
    limit: u32,
) -> Result<Vec<Event>, ApiError> {
    let mut events = state.storage.query_events(
        state.tenant_id,
        EventFilter {
            target_entity_id: Some(entity_id),
            ..Default::default()
        },
    )?;

    for event in state.storage.query_events(
        state.tenant_id,
        EventFilter {
            source_entity_id: Some(entity_id),
            ..Default::default()
        },
    )? {
        if !events.iter().any(|existing| existing.id == event.id) {
            events.push(event);
        }
    }

    events.sort_by(|left, right| {
        right
            .occurred_at
            .cmp(&left.occurred_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    events.truncate(limit as usize);
    Ok(events)
}

async fn list_mcp_tools(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<ToolDefinition>>, ApiError> {
    require_scope(&state, &auth, "/mcp/tools", "mcp:tools")?;
    Ok(Json(mcp_tool_definitions()))
}

async fn handle_mcp_json_rpc(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_scope(&state, &auth, "/mcp", "mcp:tools")?;
    let request = match serde_json::from_slice::<Value>(&body) {
        Ok(request) => request,
        Err(error) => {
            return Ok((
                StatusCode::OK,
                Json(json_rpc_error(
                    Value::Null,
                    -32700,
                    format!("parse error: {error}"),
                    None,
                )),
            ));
        }
    };

    let object = match request.as_object() {
        Some(object) => object,
        None => {
            return Ok((
                StatusCode::OK,
                Json(json_rpc_error(
                    Value::Null,
                    -32600,
                    "invalid JSON-RPC request",
                    None,
                )),
            ));
        }
    };

    let id = object.get("id").cloned().unwrap_or(Value::Null);
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Ok((
            StatusCode::OK,
            Json(json_rpc_error(id, -32600, "jsonrpc must be \"2.0\"", None)),
        ));
    }

    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return Ok((
            StatusCode::OK,
            Json(json_rpc_error(id, -32600, "method is required", None)),
        ));
    };

    let response = match method {
        "tools/list" => json_rpc_success(
            id,
            json!({
                "tools": mcp_tool_definitions()
                    .into_iter()
                    .map(mcp_compatible_tool_definition)
                    .collect::<Vec<_>>()
            }),
        ),
        "tools/call" => match parse_mcp_tools_call_params(object.get("params")) {
            Ok((tool_name, arguments)) => {
                match invoke_local_mcp_tool(&state, &tool_name, arguments) {
                    Ok(content) => json_rpc_success(id, mcp_compatible_tool_result(content)),
                    Err(error) => json_rpc_error(
                        id,
                        json_rpc_code_for_tool_failure(&error),
                        error.message,
                        Some(json!({
                            "code": error.code,
                            "isError": true
                        })),
                    ),
                }
            }
            Err(error) => json_rpc_error(
                id,
                -32602,
                error.message,
                Some(json!({
                    "code": error.code,
                    "isError": true
                })),
            ),
        },
        _ => json_rpc_error(
            id,
            -32601,
            format!("unknown JSON-RPC method '{method}'"),
            None,
        ),
    };

    Ok((StatusCode::OK, Json(response)))
}

async fn invoke_mcp_tool(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(tool_name): Path<String>,
    Json(request): Json<ToolRequest>,
) -> Result<(StatusCode, Json<ToolResponse>), ApiError> {
    require_scope(&state, &auth, "/mcp/tools/:tool_name", "mcp:tools")?;
    match invoke_local_mcp_tool(&state, &tool_name, request.arguments) {
        Ok(content) => Ok((
            StatusCode::OK,
            Json(ToolResponse::success(tool_name, content)),
        )),
        Err(error) => Ok((
            error.status,
            Json(ToolResponse::error(tool_name, error.code, error.message)),
        )),
    }
}

fn mcp_tool_definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "list_entities".to_string(),
            description: "List known entities with compact identity metadata.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolDefinition {
            name: "get_entity".to_string(),
            description: "Get one entity by entity_id.".to_string(),
            input_schema: json!({
                "type": "object",
                "required": ["entity_id"],
                "properties": {
                    "entity_id": {"type": "string", "format": "uuid"}
                }
            }),
        },
        ToolDefinition {
            name: "get_entity_context".to_string(),
            description: "Get one entity with incoming and outgoing relationships.".to_string(),
            input_schema: json!({
                "type": "object",
                "required": ["entity_id"],
                "properties": {
                    "entity_id": {"type": "string", "format": "uuid"}
                }
            }),
        },
        ToolDefinition {
            name: "get_recent_observations".to_string(),
            description: "Get recent observations by feature_of_interest_id or producer_entity_id."
                .to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "feature_of_interest_id": {"type": "string", "format": "uuid"},
                    "producer_entity_id": {"type": "string", "format": "uuid"},
                    "limit": {"type": "integer", "minimum": 1}
                }
            }),
        },
        ToolDefinition {
            name: "get_events".to_string(),
            description: "Get events by entity or optional event filters.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "entity_id": {"type": "string", "format": "uuid"},
                    "event_type": {"type": "string"},
                    "severity": {"type": "string"},
                    "command_id": {"type": "string", "format": "uuid"},
                    "raw_message_id": {"type": "string", "format": "uuid"},
                    "correlation_id": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1}
                }
            }),
        },
        ToolDefinition {
            name: "get_pending_commands".to_string(),
            description: "Get pending commands, optionally scoped to a target entity.".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "target_entity_id": {"type": "string", "format": "uuid"}
                }
            }),
        },
        ToolDefinition {
            name: "build_ai_context".to_string(),
            description: "Build the AI context package for an entity.".to_string(),
            input_schema: json!({
                "type": "object",
                "required": ["entity_id"],
                "properties": {
                    "entity_id": {"type": "string", "format": "uuid"},
                    "include_observations": {"type": "boolean"},
                    "include_events": {"type": "boolean"},
                    "include_commands": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1}
                }
            }),
        },
    ]
}

fn mcp_compatible_tool_definition(tool: ToolDefinition) -> Value {
    json!({
        "name": tool.name,
        "description": tool.description,
        "inputSchema": mcp_compatible_input_schema(tool.input_schema)
    })
}

fn mcp_compatible_input_schema(input_schema: Value) -> Value {
    let has_parameters = input_schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| !properties.is_empty())
        .unwrap_or(false)
        || input_schema
            .get("required")
            .and_then(Value::as_array)
            .map(|required| !required.is_empty())
            .unwrap_or(false);

    if has_parameters {
        input_schema
    } else {
        json!({
            "type": "object",
            "additionalProperties": false
        })
    }
}

fn parse_mcp_tools_call_params(params: Option<&Value>) -> Result<(String, Value), McpToolFailure> {
    let params = params.ok_or_else(|| {
        McpToolFailure::bad_request("missing_params", "params is required for tools/call")
    })?;
    let object = params.as_object().ok_or_else(|| {
        McpToolFailure::bad_request("invalid_params", "params must be a JSON object")
    })?;
    let tool_name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| McpToolFailure::bad_request("missing_argument", "params.name is required"))?
        .to_string();
    let arguments = object
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !arguments.is_object() {
        return Err(McpToolFailure::bad_request(
            "invalid_arguments",
            "params.arguments must be a JSON object",
        ));
    }

    Ok((tool_name, arguments))
}

fn mcp_compatible_tool_result(content: Value) -> Value {
    let text = serde_json::to_string(&content)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize tool result\"}".to_string());

    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": content,
        "isError": false
    })
}

fn json_rpc_success(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
}

fn json_rpc_error(id: Value, code: i64, message: impl Into<String>, data: Option<Value>) -> Value {
    let mut error = json!({
        "code": code,
        "message": message.into()
    });
    if let Some(data) = data {
        if let Some(object) = error.as_object_mut() {
            object.insert("data".to_string(), data);
        }
    }

    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": error
    })
}

fn json_rpc_code_for_tool_failure(error: &McpToolFailure) -> i64 {
    match error.status {
        StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST => -32602,
        _ => -32000,
    }
}

fn invoke_local_mcp_tool(
    state: &AppState,
    tool_name: &str,
    arguments: Value,
) -> Result<Value, McpToolFailure> {
    match tool_name {
        "list_entities" => mcp_list_entities(state),
        "get_entity" => mcp_get_entity(state, &arguments),
        "get_entity_context" => mcp_get_entity_context(state, &arguments),
        "get_recent_observations" => mcp_get_recent_observations(state, arguments),
        "get_events" => mcp_get_events(state, arguments),
        "get_pending_commands" => mcp_get_pending_commands(state, &arguments),
        "build_ai_context" => mcp_build_ai_context(state, arguments),
        _ => Err(McpToolFailure::not_found(format!(
            "unknown MCP tool '{tool_name}'"
        ))),
    }
}

fn mcp_list_entities(state: &AppState) -> Result<Value, McpToolFailure> {
    let entities = state
        .storage
        .list_entities(state.tenant_id)
        .map_err(McpToolFailure::from_storage)?
        .into_iter()
        .map(|entity| {
            json!({
                "id": entity.id,
                "entity_key": entity.entity_key,
                "entity_type": entity.entity_type,
                "jsonld_id": entity.jsonld.get("@id").and_then(Value::as_str)
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({ "entities": entities }))
}

fn mcp_get_entity(state: &AppState, arguments: &Value) -> Result<Value, McpToolFailure> {
    let entity_id = required_uuid(arguments, "entity_id")?;
    let entity = state
        .storage
        .get_entity(state.tenant_id, entity_id)
        .map_err(McpToolFailure::from_storage)?
        .ok_or_else(|| McpToolFailure::not_found("entity was not found"))?;

    Ok(json!({ "entity": entity }))
}

fn mcp_get_entity_context(state: &AppState, arguments: &Value) -> Result<Value, McpToolFailure> {
    let entity_id = required_uuid(arguments, "entity_id")?;
    let entity = state
        .storage
        .get_entity(state.tenant_id, entity_id)
        .map_err(McpToolFailure::from_storage)?
        .ok_or_else(|| McpToolFailure::not_found("entity was not found"))?;
    let outgoing_relationships = state
        .storage
        .list_relationships(state.tenant_id, Some(entity_id), None)
        .map_err(McpToolFailure::from_storage)?;
    let incoming_relationships = state
        .storage
        .list_relationships(state.tenant_id, None, Some(entity_id))
        .map_err(McpToolFailure::from_storage)?;

    Ok(json!({
        "entity": entity,
        "outgoing_relationships": outgoing_relationships,
        "incoming_relationships": incoming_relationships
    }))
}

fn mcp_get_recent_observations(
    state: &AppState,
    arguments: Value,
) -> Result<Value, McpToolFailure> {
    let args: McpRecentObservationsArgs = parse_tool_args(arguments)?;
    let limit = args.limit.unwrap_or(10);
    if args.feature_of_interest_id.is_none() && args.producer_entity_id.is_none() {
        return Err(McpToolFailure::bad_request(
            "missing_argument",
            "feature_of_interest_id or producer_entity_id is required",
        ));
    }

    let query_limit = if args.producer_entity_id.is_some() {
        u32::MAX
    } else {
        limit
    };
    let mut observations = state
        .storage
        .query_observations(
            state.tenant_id,
            args.feature_of_interest_id,
            None,
            None,
            None,
            query_limit,
        )
        .map_err(McpToolFailure::from_storage)?;

    if let Some(producer_entity_id) = args.producer_entity_id {
        observations.retain(|observation| observation.producer_entity_id == producer_entity_id);
        observations.truncate(limit as usize);
    }

    Ok(json!({ "observations": observations }))
}

fn mcp_get_events(state: &AppState, arguments: Value) -> Result<Value, McpToolFailure> {
    let args: McpEventsArgs = parse_tool_args(arguments)?;
    let limit = args.limit.unwrap_or(10);
    let filter = EventFilter {
        event_type: args.event_type,
        severity: args.severity,
        command_id: args.command_id,
        raw_message_id: args.raw_message_id,
        correlation_id: args.correlation_id,
        ..Default::default()
    };

    let mut events = if let Some(entity_id) = args.entity_id {
        let mut target_filter = filter.clone();
        target_filter.target_entity_id = Some(entity_id);
        let mut events = state
            .storage
            .query_events(state.tenant_id, target_filter)
            .map_err(McpToolFailure::from_storage)?;

        let mut source_filter = filter;
        source_filter.source_entity_id = Some(entity_id);
        for event in state
            .storage
            .query_events(state.tenant_id, source_filter)
            .map_err(McpToolFailure::from_storage)?
        {
            if !events.iter().any(|existing| existing.id == event.id) {
                events.push(event);
            }
        }
        events.sort_by(|left, right| {
            right
                .occurred_at
                .cmp(&left.occurred_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        events
    } else {
        state
            .storage
            .query_events(state.tenant_id, filter)
            .map_err(McpToolFailure::from_storage)?
    };

    events.truncate(limit as usize);
    Ok(json!({ "events": events }))
}

fn mcp_get_pending_commands(state: &AppState, arguments: &Value) -> Result<Value, McpToolFailure> {
    let target_entity_id = optional_uuid(arguments, "target_entity_id")?;
    let commands = state
        .storage
        .query_commands(
            state.tenant_id,
            target_entity_id,
            Some(CommandStatus::Pending),
        )
        .map_err(McpToolFailure::from_storage)?;

    Ok(json!({ "commands": commands }))
}

fn mcp_build_ai_context(state: &AppState, arguments: Value) -> Result<Value, McpToolFailure> {
    let entity_id = required_uuid(&arguments, "entity_id")?;
    let query = AiContextQuery {
        include_observations: optional_bool(&arguments, "include_observations")?,
        include_events: optional_bool(&arguments, "include_events")?,
        include_commands: optional_bool(&arguments, "include_commands")?,
        limit: optional_u32(&arguments, "limit")?,
    };
    let context =
        build_ai_entity_context(state, entity_id, query).map_err(McpToolFailure::from_api)?;

    Ok(json!({ "context": context }))
}

fn parse_tool_args<T>(arguments: Value) -> Result<T, McpToolFailure>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(arguments).map_err(|err| {
        McpToolFailure::bad_request("invalid_arguments", format!("invalid arguments: {err}"))
    })
}

fn required_uuid(arguments: &Value, field: &str) -> Result<Uuid, McpToolFailure> {
    optional_uuid(arguments, field)?.ok_or_else(|| {
        McpToolFailure::bad_request("missing_argument", format!("{field} is required"))
    })
}

fn optional_uuid(arguments: &Value, field: &str) -> Result<Option<Uuid>, McpToolFailure> {
    match arguments.get(field) {
        Some(Value::String(value)) => Uuid::parse_str(value).map(Some).map_err(|err| {
            McpToolFailure::bad_request(
                "invalid_argument",
                format!("{field} must be a UUID: {err}"),
            )
        }),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(McpToolFailure::bad_request(
            "invalid_argument",
            format!("{field} must be a UUID string"),
        )),
    }
}

fn optional_bool(arguments: &Value, field: &str) -> Result<Option<bool>, McpToolFailure> {
    match arguments.get(field) {
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(McpToolFailure::bad_request(
            "invalid_argument",
            format!("{field} must be a boolean"),
        )),
    }
}

fn optional_u32(arguments: &Value, field: &str) -> Result<Option<u32>, McpToolFailure> {
    match arguments.get(field) {
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| {
                McpToolFailure::bad_request(
                    "invalid_argument",
                    format!("{field} must be a non-negative integer within u32 range"),
                )
            }),
        Some(Value::Null) | None => Ok(None),
        Some(_) => Err(McpToolFailure::bad_request(
            "invalid_argument",
            format!("{field} must be an integer"),
        )),
    }
}

#[derive(Debug)]
struct McpToolFailure {
    status: StatusCode,
    code: String,
    message: String,
}

impl McpToolFailure {
    fn bad_request(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: code.into(),
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found".to_string(),
            message: message.into(),
        }
    }

    fn from_storage(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => Self::not_found("record was not found"),
            StorageError::InvalidInput(message) => Self::bad_request("invalid_input", message),
            StorageError::Conflict => Self {
                status: StatusCode::CONFLICT,
                code: "conflict".to_string(),
                message: "record conflicts with existing data".to_string(),
            },
            StorageError::ConflictWithMessage(message) => Self {
                status: StatusCode::CONFLICT,
                code: "conflict".to_string(),
                message,
            },
            StorageError::Backend(message) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "backend_error".to_string(),
                message,
            },
        }
    }

    fn from_api(error: ApiError) -> Self {
        Self {
            status: error.status,
            code: match error.status {
                StatusCode::NOT_FOUND => "not_found",
                StatusCode::BAD_REQUEST => "invalid_arguments",
                _ => "tool_error",
            }
            .to_string(),
            message: error.message,
        }
    }
}

async fn put_capabilities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
    Json(requests): Json<Vec<PutCapabilityRequest>>,
) -> Result<(StatusCode, Json<Vec<Capability>>), ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/entities/:entity_id/capabilities",
        "capabilities:write",
    )?;
    let entity = require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/entities/:entity_id/capabilities",
        entity_id,
    )?;
    let scoped_state = state_for_tenant(&state, entity.tenant_id);
    let capabilities = requests
        .into_iter()
        .map(|request| {
            Capability::new(
                entity_id,
                request.capability_name,
                request.command_type,
                request.protocol,
                request.metadata,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let capabilities =
        scoped_state
            .storage
            .put_capabilities(scoped_state.tenant_id, entity_id, capabilities)?;
    Ok((StatusCode::OK, Json(capabilities)))
}

async fn get_capabilities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(entity_id): Path<Uuid>,
) -> Result<Json<Vec<Capability>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/entities/:entity_id/capabilities",
        "capabilities:read",
    )?;
    let entity = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_entity(state.tenant_id, entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_entity_any_tenant(entity_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_entity(tenant_id, entity_id)? {
            Some(entity) => entity,
            None => {
                if state.storage.get_entity_any_tenant(entity_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /entities/:entity_id/capabilities",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };
    Ok(Json(
        state
            .storage
            .list_capabilities(entity.tenant_id, entity_id)?,
    ))
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

async fn create_executor(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateExecutorRequest>,
) -> Result<(StatusCode, Json<ExecutorAgent>), ApiError> {
    require_scope(&state, &auth, "/executors", "executors:register")?;
    let now = Utc::now();
    let executor = ExecutorAgent::new(
        state.tenant_id,
        request.agent_key,
        request.agent_type,
        request.display_name,
        request.status.unwrap_or(ExecutorAgentStatus::Online),
        request.metadata,
        now,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let executor = state.storage.create_executor(executor)?;
    record_executor_event(
        &state,
        "aion:ExecutorRegistered",
        &executor,
        None,
        Some(json!({"agent_type": executor.agent_type})),
    )?;

    Ok((StatusCode::CREATED, Json(executor)))
}

async fn list_executors(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<ExecutorAgent>>, ApiError> {
    require_scope(&state, &auth, "/executors", "executors:read")?;
    let executors = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_executors(state.tenant_id)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_executors()?
    } else {
        state.storage.list_executors(principal_tenant_id(&auth)?)?
    };
    Ok(Json(executors))
}

async fn get_executor(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<ExecutorAgent>, ApiError> {
    require_scope(&state, &auth, "/executors/:executor_id", "executors:read")?;
    let executor = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_executor(state.tenant_id, executor_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_executor_any_tenant(executor_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_executor(tenant_id, executor_id)? {
            Some(executor) => executor,
            None => {
                if state
                    .storage
                    .get_executor_any_tenant(executor_id)?
                    .is_some()
                {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /executors/:executor_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };
    Ok(Json(executor))
}

async fn heartbeat_executor(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
    Json(request): Json<ExecutorHeartbeatRequest>,
) -> Result<Json<ExecutorAgent>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/heartbeat",
        "executors:heartbeat",
    )?;
    let mut executor = get_executor_agent(&state, executor_id)?;
    executor.heartbeat(request.status, Utc::now());
    if request.metadata.is_some() {
        executor.metadata = request.metadata;
    }
    let executor = state.storage.update_executor(executor)?;
    record_executor_event(
        &state,
        "aion:ExecutorHeartbeat",
        &executor,
        None,
        Some(json!({"status": executor.status})),
    )?;

    Ok(Json(executor))
}

async fn put_executor_capabilities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
    Json(requests): Json<Vec<PutExecutorCapabilityRequest>>,
) -> Result<(StatusCode, Json<Vec<ExecutorCapability>>), ApiError> {
    require_any_scope(
        &state,
        &auth,
        "/executors/:executor_id/capabilities",
        &["executors:admin", "executors:write"],
    )?;
    let executor = require_same_tenant_for_target_executor(
        &state,
        &auth,
        "/executors/:executor_id/capabilities",
        executor_id,
    )?;
    let scoped_state = state_for_tenant(&state, executor.tenant_id);
    let capabilities = requests
        .into_iter()
        .map(|request| {
            ExecutorCapability::new(
                executor_id,
                request.command_type,
                request.protocol,
                request.metadata,
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok((
        StatusCode::OK,
        Json(scoped_state.storage.put_executor_capabilities(
            scoped_state.tenant_id,
            executor_id,
            capabilities,
        )?),
    ))
}

async fn get_executor_capabilities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<Vec<ExecutorCapability>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/capabilities",
        "executors:read",
    )?;
    let executor = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_executor(state.tenant_id, executor_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_executor_any_tenant(executor_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_executor(tenant_id, executor_id)? {
            Some(executor) => executor,
            None => {
                if state
                    .storage
                    .get_executor_any_tenant(executor_id)?
                    .is_some()
                {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /executors/:executor_id/capabilities",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };
    Ok(Json(state.storage.list_executor_capabilities(
        executor.tenant_id,
        executor_id,
    )?))
}

async fn put_executor_scopes(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
    Json(requests): Json<Vec<PutExecutorScopeRequest>>,
) -> Result<(StatusCode, Json<Vec<ExecutorScope>>), ApiError> {
    require_any_scope(
        &state,
        &auth,
        "/executors/:executor_id/scopes",
        &["executors:admin", "executors:write"],
    )?;
    let executor = require_same_tenant_for_target_executor(
        &state,
        &auth,
        "/executors/:executor_id/scopes",
        executor_id,
    )?;
    let scoped_state = state_for_tenant(&state, executor.tenant_id);
    let mut scopes = Vec::with_capacity(requests.len());
    for request in requests {
        if let Some(target_entity_id) = request.target_entity_id {
            require_same_tenant_for_target_entity(
                &state,
                &auth,
                "/executors/:executor_id/scopes",
                target_entity_id,
            )?;
        }
        scopes.push(ExecutorScope::new(
            executor_id,
            request.target_entity_id,
            request.entity_type,
            request.relationship_type,
            request.metadata,
        ));
    }

    Ok((
        StatusCode::OK,
        Json(scoped_state.storage.put_executor_scopes(
            scoped_state.tenant_id,
            executor_id,
            scopes,
        )?),
    ))
}

async fn get_executor_scopes(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<Vec<ExecutorScope>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/scopes",
        "executors:read",
    )?;
    let executor = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_executor(state.tenant_id, executor_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_executor_any_tenant(executor_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_executor(tenant_id, executor_id)? {
            Some(executor) => executor,
            None => {
                if state
                    .storage
                    .get_executor_any_tenant(executor_id)?
                    .is_some()
                {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /executors/:executor_id/scopes",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };
    Ok(Json(
        state
            .storage
            .list_executor_scopes(executor.tenant_id, executor_id)?,
    ))
}

async fn poll_executor_pending_commands(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<Vec<Command>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/commands/pending",
        "executors:poll",
    )?;
    ensure_executor_exists(&state, executor_id)?;
    let commands = state
        .storage
        .query_commands(state.tenant_id, None, Some(CommandStatus::Pending))?
        .into_iter()
        .filter(|command| executor_can_run_command(&state, executor_id, command).unwrap_or(false))
        .collect::<Vec<_>>();

    Ok(Json(commands))
}

async fn claim_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    request: Option<Json<ExecutorClaimCommandRequest>>,
) -> Result<Json<Command>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/commands/:command_id/claim",
        "executors:claim",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    ensure_executor_can_run_command(&state, executor_id, command_id)?;
    let request = request.map(|Json(request)| request);
    let command = claim_command_for_executor(
        &state,
        command_id,
        &executor,
        request
            .as_ref()
            .and_then(|request| request.lease_duration_seconds),
        request.as_ref().and_then(|request| request.max_retries),
        request.and_then(|request| request.metadata),
    )?;
    record_executor_event(
        &state,
        "aion:ExecutorClaimedCommand",
        &executor,
        Some(&command),
        None,
    )?;

    Ok(Json(command))
}

async fn complete_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<ExecutorCompleteCommandRequest>,
) -> Result<Json<ExecutorCommandCompletionResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/commands/:command_id/complete",
        "executors:report",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    let command = get_command_for_executor_mutation(&state, command_id, &executor.agent_key)?;
    let now = Utc::now();
    let action = Action::new(
        state.tenant_id,
        command.id,
        None,
        command.command_type.clone(),
        "completed",
        command.claimed_at,
        Some(now),
        Some(json!({
            "executor_id": executor.id,
            "agent_key": executor.agent_key,
            "source": "executor_api"
        })),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action = state.storage.store_action(action)?;
    let action_result = ActionResult::new(
        state.tenant_id,
        command.id,
        action.id,
        request.status.unwrap_or_else(|| "succeeded".to_string()),
        request.verified.unwrap_or(true),
        request.result_payload,
        now,
        Some(enrich_executor_result_metadata(&executor, request.metadata)),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action_result = state.storage.store_action_result(action_result)?;
    let command = mutate_command_raw(&state, command_id, |command, now| {
        command.mark_executed(now)
    })?;
    mark_active_lease_completed(&state, command_id, executor_id)?;
    record_command_event(
        &state,
        "aion:CommandExecuted",
        EventSeverity::Info,
        &command,
        None,
    )?;
    record_executor_event(
        &state,
        "aion:ExecutorCompletedCommand",
        &executor,
        Some(&command),
        Some(json!({"action_id": action.id, "action_result_id": action_result.id})),
    )?;

    Ok(Json(ExecutorCommandCompletionResponse {
        command,
        action,
        action_result,
    }))
}

async fn fail_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<ExecutorFailCommandRequest>,
) -> Result<Json<ExecutorCommandCompletionResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/executors/:executor_id/commands/:command_id/fail",
        "executors:report",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    let command = get_command_for_executor_mutation(&state, command_id, &executor.agent_key)?;
    let now = Utc::now();
    let action = Action::new(
        state.tenant_id,
        command.id,
        None,
        command.command_type.clone(),
        "failed",
        command.claimed_at,
        Some(now),
        Some(json!({
            "executor_id": executor.id,
            "agent_key": executor.agent_key,
            "source": "executor_api"
        })),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action = state.storage.store_action(action)?;
    let action_result = ActionResult::new(
        state.tenant_id,
        command.id,
        action.id,
        "failed",
        false,
        request
            .result_payload
            .unwrap_or_else(|| json!({"failure_reason": request.failure_reason})),
        now,
        Some(enrich_executor_result_metadata(&executor, request.metadata)),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action_result = state.storage.store_action_result(action_result)?;
    let command = mutate_command_raw(&state, command_id, |command, now| {
        command.mark_failed(request.failure_reason, now)
    })?;
    mark_active_lease_failed(&state, command_id, executor_id)?;
    record_command_event(
        &state,
        "aion:CommandFailed",
        EventSeverity::Error,
        &command,
        None,
    )?;
    record_executor_event(
        &state,
        "aion:ExecutorFailedCommand",
        &executor,
        Some(&command),
        Some(json!({"action_id": action.id, "action_result_id": action_result.id})),
    )?;

    Ok(Json(ExecutorCommandCompletionResponse {
        command,
        action,
        action_result,
    }))
}

async fn register_smartsentinel_executor(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<RegisterSmartSentinelExecutorRequest>,
) -> Result<(StatusCode, Json<RegisterSmartSentinelExecutorResponse>), ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/executors/register",
        "smartsentinel:executor_register",
    )?;
    let now = Utc::now();
    let existing = state
        .storage
        .list_executors(state.tenant_id)?
        .into_iter()
        .find(|executor| executor.agent_key == request.agent_key);

    let (executor, reused) = if let Some(mut executor) = existing {
        if executor.agent_type != "smartsentinel" {
            return Err(ApiError::bad_request(
                "agent_key is already registered for a non-SmartSentinel executor",
            ));
        }
        executor.display_name = request.display_name;
        executor.metadata = request.metadata;
        executor.heartbeat(ExecutorAgentStatus::Online, now);
        (state.storage.update_executor(executor)?, true)
    } else {
        let executor = ExecutorAgent::new(
            state.tenant_id,
            request.agent_key,
            "smartsentinel",
            request.display_name,
            ExecutorAgentStatus::Online,
            request.metadata,
            now,
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        (state.storage.create_executor(executor)?, false)
    };

    let capabilities = smart_sentinel_executor_capabilities(executor.id, request.capabilities)?;
    let capabilities =
        state
            .storage
            .put_executor_capabilities(state.tenant_id, executor.id, capabilities)?;
    let scopes = smart_sentinel_executor_scopes(&state, executor.id, request.scopes)?;
    let scopes = state
        .storage
        .put_executor_scopes(state.tenant_id, executor.id, scopes)?;

    record_executor_event(
        &state,
        if reused {
            "aion:SmartSentinelExecutorUpdated"
        } else {
            "aion:SmartSentinelExecutorRegistered"
        },
        &executor,
        None,
        Some(json!({
            "capability_count": capabilities.len(),
            "scope_count": scopes.len(),
            "source": "smartsentinel_bridge"
        })),
    )?;

    Ok((
        if reused {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(RegisterSmartSentinelExecutorResponse {
            executor,
            reused,
            capabilities,
            scopes,
        }),
    ))
}

async fn poll_smartsentinel_executor_commands(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(executor_id): Path<Uuid>,
) -> Result<Json<Vec<SmartSentinelCommandEnvelope>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/executors/:executor_id/commands",
        "smartsentinel:executor_poll",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    ensure_smartsentinel_executor(&executor)?;
    let commands = state
        .storage
        .query_commands(state.tenant_id, None, Some(CommandStatus::Pending))?
        .into_iter()
        .filter(|command| executor_can_run_command(&state, executor_id, command).unwrap_or(false))
        .map(|command| smartsentinel_command_envelope(&state, command))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(commands))
}

async fn claim_smartsentinel_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    request: Option<Json<ExecutorClaimCommandRequest>>,
) -> Result<Json<SmartSentinelCommandEnvelope>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/executors/:executor_id/commands/:command_id/claim",
        "smartsentinel:executor_claim",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    ensure_smartsentinel_executor(&executor)?;
    ensure_executor_can_run_command(&state, executor_id, command_id)?;
    let request = request.map(|Json(request)| request);
    let command = claim_command_for_executor(
        &state,
        command_id,
        &executor,
        request
            .as_ref()
            .and_then(|request| request.lease_duration_seconds),
        request.as_ref().and_then(|request| request.max_retries),
        request
            .and_then(|request| request.metadata)
            .map(|metadata| json!({"source": "smartsentinel_bridge", "metadata": metadata})),
    )?;
    record_executor_event(
        &state,
        "aion:SmartSentinelCommandClaimed",
        &executor,
        Some(&command),
        Some(json!({"source": "smartsentinel_bridge"})),
    )?;

    Ok(Json(smartsentinel_command_envelope(&state, command)?))
}

async fn report_smartsentinel_executor_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((executor_id, command_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<SmartSentinelCommandReportRequest>,
) -> Result<Json<SmartSentinelCommandReportResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/executors/:executor_id/commands/:command_id/report",
        "smartsentinel:executor_report",
    )?;
    let executor = get_executor_agent(&state, executor_id)?;
    ensure_smartsentinel_executor(&executor)?;
    let command = get_command_for_executor_mutation(&state, command_id, &executor.agent_key)?;
    let report_status = request.status.trim();
    if !matches!(report_status, "executed" | "failed") {
        return Err(ApiError::bad_request(
            "status must be either executed or failed",
        ));
    }
    if request.action_type.trim().is_empty() {
        return Err(ApiError::bad_request("action_type must not be empty"));
    }

    let now = Utc::now();
    let metadata = smartsentinel_report_metadata(&executor, &request);
    let action = Action::new(
        state.tenant_id,
        command.id,
        None,
        request.action_type.clone(),
        report_status.to_string(),
        command.claimed_at,
        Some(now),
        Some(metadata.clone()),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action = state.storage.store_action(action)?;
    let action_result = ActionResult::new(
        state.tenant_id,
        command.id,
        action.id,
        report_status.to_string(),
        request.verified,
        request.result_payload.clone(),
        now,
        Some(metadata.clone()),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let action_result = state.storage.store_action_result(action_result)?;

    let command = if report_status == "executed" {
        let command = mutate_command_raw(&state, command_id, |command, now| {
            command.mark_executed(now)
        })?;
        mark_active_lease_completed(&state, command_id, executor_id)?;
        record_command_event(
            &state,
            "aion:CommandExecuted",
            EventSeverity::Info,
            &command,
            request.message.clone(),
        )?;
        command
    } else {
        let failure_reason = request
            .message
            .clone()
            .unwrap_or_else(|| "SmartSentinel executor reported failure".to_string());
        let command = mutate_command_raw(&state, command_id, |command, now| {
            command.mark_failed(failure_reason, now)
        })?;
        mark_active_lease_failed(&state, command_id, executor_id)?;
        record_command_event(
            &state,
            "aion:CommandFailed",
            EventSeverity::Error,
            &command,
            request.message.clone(),
        )?;
        command
    };

    let event = record_event(
        &state,
        EventDraft {
            event_type: "aion:SmartSentinelCommandReported".to_string(),
            severity: if report_status == "executed" {
                EventSeverity::Info
            } else {
                EventSeverity::Error
            },
            source_entity_id: None,
            target_entity_id: Some(command.target_entity_id),
            message: request.message,
            occurred_at: now,
            observed_at: None,
            correlation_id: request.correlation_id,
            raw_message_id: None,
            observation_id: None,
            command_id: Some(command.id),
            action_id: Some(action.id),
            action_result_id: Some(action_result.id),
            metadata: Some(metadata),
        },
    )?;

    Ok(Json(SmartSentinelCommandReportResponse {
        command,
        action,
        action_result,
        event,
    }))
}

async fn get_command_lease(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<CommandLease>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/commands/:command_id/lease",
        "commands:read",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/lease",
        command_id,
    )?;
    let scoped_state = state_for_tenant(&state, command.tenant_id);
    Ok(Json(
        scoped_state
            .storage
            .get_latest_command_lease(scoped_state.tenant_id, command_id)?
            .ok_or_else(ApiError::not_found)?,
    ))
}

async fn refresh_command_lease(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<RefreshCommandLeaseRequest>,
) -> Result<Json<CommandLease>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/lease/refresh",
        "commands:lease",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/lease/refresh",
        command_id,
    )?;
    let scoped_state = state_for_tenant(&state, command.tenant_id);
    let mut lease = active_lease_for_executor(&scoped_state, command_id, request.executor_id)?;
    let now = Utc::now();
    let expires_at = lease_expiry(now, request.lease_duration_seconds)?;
    lease
        .refresh(expires_at, now)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let lease = scoped_state.storage.update_command_lease(lease)?;
    let mut command = scoped_state
        .storage
        .get_command(scoped_state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;
    command.set_lease_expires_at(Some(expires_at), now);
    let command = scoped_state.storage.update_command(command)?;
    record_lease_event(
        &scoped_state,
        "aion:CommandLeaseRefreshed",
        &lease,
        Some(&command),
        None,
    )?;
    Ok(Json(lease))
}

async fn release_command_lease(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<ReleaseCommandLeaseRequest>,
) -> Result<Json<CommandLease>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/lease/release",
        "commands:lease",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/lease/release",
        command_id,
    )?;
    release_active_lease(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        request.executor_id,
    )
    .map(Json)
}

async fn recover_expired_leases(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<RecoverExpiredLeasesResponse>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/recover-expired-leases",
        "commands:lease",
    )?;
    let scoped_state = state_for_tenant(&state, principal_tenant_or_default(&state, &auth)?);
    let now = Utc::now();
    let mut response = RecoverExpiredLeasesResponse {
        expired_lease_ids: Vec::new(),
        retried_command_ids: Vec::new(),
        failed_command_ids: Vec::new(),
    };

    for mut lease in scoped_state
        .storage
        .list_active_command_leases(scoped_state.tenant_id)?
    {
        if lease.expires_at > now {
            continue;
        }
        lease.mark_expired(now);
        let lease = scoped_state.storage.update_command_lease(lease)?;
        response.expired_lease_ids.push(lease.id);

        let mut command = scoped_state
            .storage
            .get_command(scoped_state.tenant_id, lease.command_id)?
            .ok_or_else(ApiError::not_found)?;
        record_lease_event(
            &scoped_state,
            "aion:CommandLeaseExpired",
            &lease,
            Some(&command),
            None,
        )?;

        if command.retry_limit_exceeded() {
            command.mark_failed_due_to_retry_limit("command retry limit exceeded", now);
            let command = scoped_state.storage.update_command(command)?;
            response.failed_command_ids.push(command.id);
            record_lease_event(
                &scoped_state,
                "aion:CommandRetryLimitExceeded",
                &lease,
                Some(&command),
                Some(
                    json!({"retry_count": command.retry_count, "max_retries": command.max_retries}),
                ),
            )?;
            record_command_event(
                &scoped_state,
                "aion:CommandFailed",
                EventSeverity::Error,
                &command,
                Some("command retry limit exceeded".to_string()),
            )?;
        } else {
            command.schedule_retry(now);
            let command = scoped_state.storage.update_command(command)?;
            response.retried_command_ids.push(command.id);
            record_lease_event(
                &scoped_state,
                "aion:CommandRetryScheduled",
                &lease,
                Some(&command),
                Some(
                    json!({"retry_count": command.retry_count, "max_retries": command.max_retries}),
                ),
            )?;
        }
    }

    Ok(Json(response))
}

async fn create_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateCommandRequest>,
) -> Result<(StatusCode, Json<Command>), ApiError> {
    require_scope_for_write(&state, &auth, "/commands", "commands:create")?;
    require_same_tenant_for_target_entity(&state, &auth, "/commands", request.target_entity_id)?;
    let scoped_state = state_for_tenant(&state, tenant_for_created_resource(&state, &auth)?);
    let (approval_status, policy_decision) = command_policy_decision(
        &scoped_state,
        request.target_entity_id,
        &request.command_type,
    )?;
    let command = Command::new(
        scoped_state.tenant_id,
        request.target_entity_id,
        request.command_type,
        request.payload,
        request.requested_by,
        request.reason,
        Some(approval_status),
        Some(policy_decision),
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let command = scoped_state.storage.store_command(command)?;
    record_command_event(
        &scoped_state,
        "aion:CommandCreated",
        EventSeverity::Info,
        &command,
        None,
    )?;
    Ok((StatusCode::CREATED, Json(command)))
}

async fn claim_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<ClaimCommandRequest>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/claim",
        "commands:claim",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/claim",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandClaimed",
        EventSeverity::Info,
        |command, now| command.claim(request.claimed_by, now),
    )
}

async fn release_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/release",
        "commands:write",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/release",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandReleased",
        EventSeverity::Info,
        |command, now| command.release(now),
    )
}

async fn mark_command_executed(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/mark-executed",
        "commands:write",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/mark-executed",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandExecuted",
        EventSeverity::Info,
        |command, now| command.mark_executed(now),
    )
}

async fn mark_command_failed(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
    Json(request): Json<MarkFailedCommandRequest>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/mark-failed",
        "commands:write",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/mark-failed",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandFailed",
        EventSeverity::Error,
        |command, now| command.mark_failed(request.failure_reason, now),
    )
}

async fn cancel_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/cancel",
        "commands:write",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/cancel",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandCancelled",
        EventSeverity::Warning,
        |command, now| command.cancel(now),
    )
}

async fn approve_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/approve",
        "commands:approve",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/approve",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandApproved",
        EventSeverity::Info,
        |command, now| command.approve(now),
    )
}

async fn reject_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope_for_write(
        &state,
        &auth,
        "/commands/:command_id/reject",
        "commands:approve",
    )?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/commands/:command_id/reject",
        command_id,
    )?;
    mutate_command(
        &state_for_tenant(&state, command.tenant_id),
        command_id,
        "aion:CommandRejected",
        EventSeverity::Warning,
        |command, now| command.reject(now),
    )
}

async fn get_command(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(command_id): Path<Uuid>,
) -> Result<Json<Command>, ApiError> {
    require_scope(&state, &auth, "/commands/:command_id", "commands:read")?;
    let command = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_command(state.tenant_id, command_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_command_any_tenant(command_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_command(tenant_id, command_id)? {
            Some(command) => command,
            None => {
                if state.storage.get_command_any_tenant(command_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /commands/:command_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(command))
}

async fn query_commands(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<CommandQuery>,
) -> Result<Json<Vec<Command>>, ApiError> {
    require_scope(&state, &auth, "/commands", "commands:read")?;
    let commands = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .query_commands(state.tenant_id, query.target_entity_id, query.status)?
    } else if is_admin_all(&auth) {
        let status = query.status.clone();
        state
            .storage
            .list_all_commands()?
            .into_iter()
            .filter(|command| {
                query
                    .target_entity_id
                    .map(|id| command.target_entity_id == id)
                    .unwrap_or(true)
            })
            .filter(|command| {
                status
                    .as_ref()
                    .map(|value| command.status == *value)
                    .unwrap_or(true)
            })
            .collect()
    } else {
        state.storage.query_commands(
            principal_tenant_id(&auth)?,
            query.target_entity_id,
            query.status,
        )?
    };
    Ok(Json(commands))
}

async fn create_action(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateActionRequest>,
) -> Result<(StatusCode, Json<Action>), ApiError> {
    require_scope_for_write(&state, &auth, "/actions", "actions:write")?;
    let command =
        require_same_tenant_for_target_command(&state, &auth, "/actions", request.command_id)?;
    if let Some(executor_entity_id) = request.executor_entity_id {
        require_same_tenant_for_target_entity(&state, &auth, "/actions", executor_entity_id)?;
    }
    let scoped_state = state_for_tenant(&state, command.tenant_id);

    let action = Action::new(
        scoped_state.tenant_id,
        request.command_id,
        request.executor_entity_id,
        request.action_type,
        request.status,
        request.started_at,
        request.finished_at,
        request.metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let action = scoped_state.storage.store_action(action)?;
    Ok((StatusCode::CREATED, Json(action)))
}

async fn get_action(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(action_id): Path<Uuid>,
) -> Result<Json<Action>, ApiError> {
    require_scope(&state, &auth, "/actions/:action_id", "actions:read")?;
    let action = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_action(state.tenant_id, action_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_action_any_tenant(action_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_action(tenant_id, action_id)? {
            Some(action) => action,
            None => {
                if state.storage.get_action_any_tenant(action_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /actions/:action_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(action))
}

async fn query_actions(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ActionQuery>,
) -> Result<Json<Vec<Action>>, ApiError> {
    require_scope(&state, &auth, "/actions", "actions:read")?;
    let actions = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .query_actions(state.tenant_id, query.command_id)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .list_all_actions()?
            .into_iter()
            .filter(|action| {
                query
                    .command_id
                    .map(|id| action.command_id == id)
                    .unwrap_or(true)
            })
            .collect()
    } else {
        state
            .storage
            .query_actions(principal_tenant_id(&auth)?, query.command_id)?
    };
    Ok(Json(actions))
}

async fn create_action_result(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateActionResultRequest>,
) -> Result<(StatusCode, Json<ActionResult>), ApiError> {
    require_scope_for_write(&state, &auth, "/action-results", "actions:write")?;
    let command = require_same_tenant_for_target_command(
        &state,
        &auth,
        "/action-results",
        request.command_id,
    )?;
    let scoped_state = state_for_tenant(&state, command.tenant_id);
    let action = require_same_tenant_for_target_action(
        &scoped_state,
        &auth,
        "/action-results",
        request.action_id,
    )?;
    if action.command_id != request.command_id {
        return Err(ApiError::bad_request(
            "action_id does not belong to command_id",
        ));
    }

    let result = ActionResult::new(
        scoped_state.tenant_id,
        request.command_id,
        request.action_id,
        request.status,
        request.verified,
        request.result_payload,
        request.observed_at,
        request.metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let result = scoped_state.storage.store_action_result(result)?;
    Ok((StatusCode::CREATED, Json(result)))
}

async fn query_action_results(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ActionResultQuery>,
) -> Result<Json<Vec<ActionResult>>, ApiError> {
    require_scope(&state, &auth, "/action-results", "actions:read")?;
    let results = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .query_action_results(state.tenant_id, query.action_id, query.command_id)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .list_all_action_results()?
            .into_iter()
            .filter(|result| {
                query
                    .action_id
                    .map(|id| result.action_id == id)
                    .unwrap_or(true)
            })
            .filter(|result| {
                query
                    .command_id
                    .map(|id| result.command_id == id)
                    .unwrap_or(true)
            })
            .collect()
    } else {
        state.storage.query_action_results(
            principal_tenant_id(&auth)?,
            query.action_id,
            query.command_id,
        )?
    };
    Ok(Json(results))
}

async fn create_event(
    State(state): State<AppState>,
    Json(request): Json<CreateEventRequest>,
) -> Result<(StatusCode, Json<Event>), ApiError> {
    if let Some(source_entity_id) = request.source_entity_id {
        ensure_entity_exists(&state, source_entity_id)?;
    }
    if let Some(target_entity_id) = request.target_entity_id {
        ensure_entity_exists(&state, target_entity_id)?;
    }
    if let Some(command_id) = request.command_id {
        ensure_command_exists(&state, command_id)?;
    }
    if let Some(action_id) = request.action_id {
        ensure_action_exists(&state, action_id)?;
    }
    if let Some(action_result_id) = request.action_result_id {
        ensure_action_result_exists(&state, action_result_id)?;
    }
    if let Some(raw_message_id) = request.raw_message_id {
        ensure_raw_message_exists(&state, raw_message_id)?;
    }

    let event = Event::new(
        state.tenant_id,
        request.event_type,
        request.severity,
        request.source_entity_id,
        request.target_entity_id,
        request.message,
        request.occurred_at,
        request.observed_at,
        request.correlation_id,
        request.raw_message_id,
        request.observation_id,
        request.command_id,
        request.action_id,
        request.action_result_id,
        request.metadata,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let event = state.storage.store_event(event)?;
    evaluate_rules_for_event(&state, &event, true)?;
    Ok((StatusCode::CREATED, Json(event)))
}

async fn get_event(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(event_id): Path<Uuid>,
) -> Result<Json<Event>, ApiError> {
    require_scope(&state, &auth, "/events/:event_id", "events:read")?;
    let event = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_event(state.tenant_id, event_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_event_any_tenant(event_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_event(tenant_id, event_id)? {
            Some(event) => event,
            None => {
                if state.storage.get_event_any_tenant(event_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /events/:event_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(event))
}

async fn query_events(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<Event>>, ApiError> {
    require_scope(&state, &auth, "/events", "events:read")?;
    let filter = EventFilter {
        source_entity_id: query.source_entity_id,
        target_entity_id: query.target_entity_id,
        event_type: query.event_type.clone(),
        severity: query.severity.clone(),
        command_id: query.command_id,
        raw_message_id: query.raw_message_id,
        correlation_id: query.correlation_id.clone(),
    };
    let events = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .query_events(state.tenant_id, filter.clone())?
    } else if is_admin_all(&auth) {
        state
            .storage
            .list_all_events()?
            .into_iter()
            .filter(|event| {
                filter
                    .source_entity_id
                    .map(|id| event.source_entity_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .target_entity_id
                    .map(|id| event.target_entity_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .event_type
                    .as_deref()
                    .map(|event_type| event.event_type == event_type)
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .severity
                    .as_ref()
                    .map(|severity| event.severity == *severity)
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .command_id
                    .map(|id| event.command_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .raw_message_id
                    .map(|id| event.raw_message_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .correlation_id
                    .as_deref()
                    .map(|correlation_id| event.correlation_id.as_deref() == Some(correlation_id))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>()
    } else {
        state
            .storage
            .query_events(principal_tenant_id(&auth)?, filter.clone())?
    };
    let events = events
        .into_iter()
        .filter(|event| event_matches_metadata_filters(event, &query))
        .collect::<Vec<_>>();

    Ok(Json(events))
}

async fn create_observation(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateObservationRequest>,
) -> Result<(StatusCode, Json<Observation>), ApiError> {
    require_scope_for_write(&state, &auth, "/observations", "observations:write")?;
    require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/observations",
        request.producer_entity_id,
    )?;
    require_same_tenant_for_target_entity(
        &state,
        &auth,
        "/observations",
        request.feature_of_interest_id,
    )?;
    let scoped_state = state_for_tenant(&state, tenant_for_created_resource(&state, &auth)?);

    let observation = Observation::new(
        scoped_state.tenant_id,
        request.producer_entity_id,
        request.feature_of_interest_id,
        request.observed_property,
        request.value,
        request.unit,
        request.observed_at,
        request.received_at,
        request.protocol,
        request.payload_format,
        request.raw_message_id,
        request.quality,
        request.metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let observation = scoped_state.storage.store_observation(observation)?;
    evaluate_rules_for_observation(&scoped_state, &observation, true)?;
    Ok((StatusCode::CREATED, Json(observation)))
}

async fn query_observations(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ObservationQuery>,
) -> Result<Json<Vec<Observation>>, ApiError> {
    require_scope(&state, &auth, "/observations", "observations:read")?;
    let observations = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.query_observations(
            state.tenant_id,
            query.feature_of_interest_id,
            query.observed_property.as_deref(),
            None,
            None,
            query.limit.unwrap_or(100),
        )?
    } else if is_admin_all(&auth) {
        let mut observations = state.storage.list_all_observations()?;
        if let Some(feature_of_interest_id) = query.feature_of_interest_id {
            observations
                .retain(|observation| observation.feature_of_interest_id == feature_of_interest_id);
        }
        if let Some(observed_property) = query.observed_property.as_deref() {
            observations.retain(|observation| observation.observed_property == observed_property);
        }
        observations.truncate(query.limit.unwrap_or(100) as usize);
        observations
    } else {
        state.storage.query_observations(
            principal_tenant_id(&auth)?,
            query.feature_of_interest_id,
            query.observed_property.as_deref(),
            None,
            None,
            query.limit.unwrap_or(100),
        )?
    };
    let observations = if let Some(raw_message_id) = query.raw_message_id {
        observations
            .into_iter()
            .filter(|observation| observation.raw_message_id == Some(raw_message_id))
            .collect::<Vec<_>>()
    } else {
        observations
    };

    Ok(Json(observations))
}

async fn get_raw_message(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(raw_message_id): Path<Uuid>,
) -> Result<Json<RawMessageResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/raw-messages/:raw_message_id",
        "raw-messages:read",
    )?;
    let raw_message = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_raw_message(state.tenant_id, raw_message_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_raw_message_any_tenant(raw_message_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_raw_message(tenant_id, raw_message_id)? {
            Some(raw_message) => raw_message,
            None => {
                if state
                    .storage
                    .get_raw_message_any_tenant(raw_message_id)?
                    .is_some()
                {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /raw-messages/:raw_message_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(raw_message_response(raw_message)))
}

async fn query_raw_messages(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<RawMessageQuery>,
) -> Result<Json<Vec<RawMessageResponse>>, ApiError> {
    require_scope(&state, &auth, "/raw-messages", "raw-messages:read")?;
    let raw_messages = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_raw_messages(state.tenant_id)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_raw_messages()?
    } else {
        state
            .storage
            .list_raw_messages(principal_tenant_id(&auth)?)?
    }
    .into_iter()
    .filter(|raw_message| {
        query
            .producer_entity_id
            .map(|id| raw_message_uuid_header(raw_message, "producer_entity_id") == Some(id))
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .feature_of_interest_id
            .map(|id| raw_message_uuid_header(raw_message, "feature_of_interest_id") == Some(id))
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .payload_format
            .as_deref()
            .map(|payload_format| {
                raw_message_string_header(raw_message, "payload_format")
                    .map(|value| value.eq_ignore_ascii_case(payload_format))
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .connector_id
            .map(|id| raw_message_uuid_header(raw_message, "connector_id") == Some(id))
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .connector_key
            .as_deref()
            .map(|connector_key| {
                raw_message_string_header(raw_message, "connector_key")
                    .map(|value| value == connector_key)
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .connector_profile
            .as_deref()
            .map(|connector_profile| {
                raw_message_string_header(raw_message, "connector_profile")
                    .map(|value| value.eq_ignore_ascii_case(connector_profile))
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    })
    .filter(|raw_message| raw_message_matches_provenance_filters(raw_message, &query))
    .map(raw_message_response)
    .collect::<Vec<_>>();

    Ok(Json(raw_messages))
}

async fn search_provenance(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ProvenanceSearchQuery>,
) -> Result<Json<ProvenanceSearchResponse>, ApiError> {
    require_scope(&state, &auth, "/provenance/search", "provenance:read")?;
    let limit = query.limit.unwrap_or(100).min(1000);
    let events = state
        .storage
        .query_events(state.tenant_id, EventFilter::default())?
        .into_iter()
        .filter(|event| event_matches_provenance_search(event, &query))
        .take(limit as usize)
        .collect::<Vec<_>>();
    let raw_messages = state
        .storage
        .list_raw_messages(state.tenant_id)?
        .into_iter()
        .filter(|raw_message| raw_message_matches_provenance_search(raw_message, &query))
        .take(limit as usize)
        .map(raw_message_response)
        .collect::<Vec<_>>();
    let observations = state
        .storage
        .query_observations(state.tenant_id, None, None, None, None, limit)?
        .into_iter()
        .filter(|observation| observation_matches_provenance_search(observation, &query))
        .collect::<Vec<_>>();
    let counts = ProvenanceSearchCounts {
        matching_events: events.len(),
        matching_raw_messages: raw_messages.len(),
        matching_observations: observations.len(),
    };
    let query_metadata = provenance_search_query_metadata(&query, limit);

    Ok(Json(ProvenanceSearchResponse {
        matching_events: events,
        matching_raw_messages: raw_messages,
        matching_observations: observations,
        counts,
        query: query_metadata,
    }))
}

fn event_matches_metadata_filters(event: &Event, query: &EventQuery) -> bool {
    let metadata = event.metadata.as_ref();
    optional_metadata_string_matches(metadata, "incident_id", query.incident_id.as_deref())
        && optional_metadata_string_matches(metadata, "alert_id", query.alert_id.as_deref())
        && optional_metadata_string_matches(metadata, "trace_id", query.trace_id.as_deref())
        && optional_metadata_string_matches(metadata, "run_id", query.run_id.as_deref())
        && optional_metadata_string_matches(metadata, "workflow_id", query.workflow_id.as_deref())
        && optional_metadata_string_matches(metadata, "cycle_id", query.cycle_id.as_deref())
        && optional_metadata_evidence_matches(
            metadata,
            query.evidence_id.as_deref(),
            query.external_id.as_deref(),
        )
}

fn raw_message_matches_provenance_filters(
    raw_message: &RawMessage,
    query: &RawMessageQuery,
) -> bool {
    optional_raw_header_string_matches(raw_message, "snapshot_id", query.snapshot_id.as_deref())
        && optional_raw_header_string_matches(raw_message, "node_id", query.node_id.as_deref())
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "trace_id",
            query.trace_id.as_deref(),
        )
        && optional_raw_smartsentinel_string_matches(raw_message, "run_id", query.run_id.as_deref())
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "workflow_id",
            query.workflow_id.as_deref(),
        )
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "cycle_id",
            query.cycle_id.as_deref(),
        )
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "correlation_id",
            query.correlation_id.as_deref(),
        )
}

fn event_matches_provenance_search(event: &Event, query: &ProvenanceSearchQuery) -> bool {
    let metadata = event.metadata.as_ref();
    optional_metadata_string_matches(metadata, "incident_id", query.incident_id.as_deref())
        && optional_metadata_string_matches(metadata, "alert_id", query.alert_id.as_deref())
        && optional_metadata_string_matches(metadata, "trace_id", query.trace_id.as_deref())
        && optional_metadata_string_matches(metadata, "run_id", query.run_id.as_deref())
        && optional_metadata_string_matches(metadata, "workflow_id", query.workflow_id.as_deref())
        && optional_metadata_string_matches(metadata, "cycle_id", query.cycle_id.as_deref())
        && optional_metadata_string_matches(
            metadata,
            "correlation_id",
            query.correlation_id.as_deref(),
        )
        && optional_metadata_string_matches(metadata, "snapshot_id", query.snapshot_id.as_deref())
        && optional_metadata_string_matches(metadata, "node_id", query.node_id.as_deref())
        && optional_metadata_evidence_matches(
            metadata,
            query.evidence_id.as_deref(),
            query.external_id.as_deref(),
        )
}

fn raw_message_matches_provenance_search(
    raw_message: &RawMessage,
    query: &ProvenanceSearchQuery,
) -> bool {
    optional_raw_header_string_matches(raw_message, "snapshot_id", query.snapshot_id.as_deref())
        && optional_raw_header_string_matches(raw_message, "node_id", query.node_id.as_deref())
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "trace_id",
            query.trace_id.as_deref(),
        )
        && optional_raw_smartsentinel_string_matches(raw_message, "run_id", query.run_id.as_deref())
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "workflow_id",
            query.workflow_id.as_deref(),
        )
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "cycle_id",
            query.cycle_id.as_deref(),
        )
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "correlation_id",
            query.correlation_id.as_deref(),
        )
        && optional_raw_smartsentinel_evidence_id_matches(raw_message, query.evidence_id.as_deref())
        && optional_raw_smartsentinel_external_id_matches(raw_message, query.external_id.as_deref())
        && optional_raw_smartsentinel_external_id_matches(raw_message, query.incident_id.as_deref())
        && query.alert_id.is_none()
}

fn observation_matches_provenance_search(
    observation: &Observation,
    query: &ProvenanceSearchQuery,
) -> bool {
    optional_metadata_string_matches(
        Some(&observation.metadata),
        "trace_id",
        query.trace_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "run_id",
        query.run_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "workflow_id",
        query.workflow_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "cycle_id",
        query.cycle_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "correlation_id",
        query.correlation_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "snapshot_id",
        query.snapshot_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "node_id",
        query.node_id.as_deref(),
    ) && optional_metadata_evidence_matches(
        Some(&observation.metadata),
        query.evidence_id.as_deref(),
        query.external_id.as_deref(),
    ) && query.incident_id.is_none()
        && query.alert_id.is_none()
}

fn optional_metadata_string_matches(
    metadata: Option<&Value>,
    key: &str,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| {
            metadata
                .map(|metadata| metadata_string_matches(metadata, key, expected))
                .unwrap_or(false)
        })
        .unwrap_or(true)
}

fn metadata_string_matches(metadata: &Value, key: &str, expected: &str) -> bool {
    value_string_matches(metadata.get(key), expected)
        || metadata
            .get("provenance")
            .map(|provenance| value_string_matches(provenance.get(key), expected))
            .unwrap_or(false)
}

fn optional_metadata_evidence_matches(
    metadata: Option<&Value>,
    evidence_id: Option<&str>,
    external_id: Option<&str>,
) -> bool {
    evidence_id
        .map(|expected| {
            metadata
                .map(|metadata| metadata_evidence_id_matches(metadata, expected))
                .unwrap_or(false)
        })
        .unwrap_or(true)
        && external_id
            .map(|expected| {
                metadata
                    .map(|metadata| metadata_external_id_matches(metadata, expected))
                    .unwrap_or(false)
            })
            .unwrap_or(true)
}

fn metadata_evidence_id_matches(metadata: &Value, expected: &str) -> bool {
    metadata
        .get("evidence_refs")
        .and_then(Value::as_array)
        .map(|refs| refs.iter().any(|value| value.as_str() == Some(expected)))
        .unwrap_or(false)
        || metadata
            .get("evidence")
            .and_then(Value::as_array)
            .map(|evidence| {
                evidence
                    .iter()
                    .any(|item| value_string_matches(item.get("evidence_id"), expected))
            })
            .unwrap_or(false)
}

fn metadata_external_id_matches(metadata: &Value, expected: &str) -> bool {
    metadata
        .get("external_id")
        .map(|value| value.as_str() == Some(expected))
        .unwrap_or(false)
        || metadata
            .get("evidence")
            .and_then(Value::as_array)
            .map(|evidence| {
                evidence
                    .iter()
                    .any(|item| value_string_matches(item.get("external_id"), expected))
            })
            .unwrap_or(false)
        || metadata
            .get("provenance")
            .and_then(|provenance| provenance.get("external_refs"))
            .and_then(Value::as_array)
            .map(|refs| {
                refs.iter()
                    .any(|item| value_string_matches(item.get("external_id"), expected))
            })
            .unwrap_or(false)
}

fn optional_raw_header_string_matches(
    raw_message: &RawMessage,
    key: &str,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| {
            raw_message
                .headers
                .get(key)
                .and_then(Value::as_str)
                .map(|value| value == expected)
                .unwrap_or(false)
        })
        .unwrap_or(true)
}

fn optional_raw_smartsentinel_string_matches(
    raw_message: &RawMessage,
    key: &str,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| raw_smartsentinel_string_matches(raw_message, key, expected))
        .unwrap_or(true)
}

fn raw_smartsentinel_string_matches(raw_message: &RawMessage, key: &str, expected: &str) -> bool {
    raw_message
        .headers
        .get("smartsentinel")
        .map(|metadata| metadata_string_matches(metadata, key, expected))
        .unwrap_or(false)
}

fn optional_raw_smartsentinel_evidence_id_matches(
    raw_message: &RawMessage,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| {
            raw_message
                .headers
                .get("smartsentinel")
                .map(|metadata| metadata_evidence_id_matches(metadata, expected))
                .unwrap_or(false)
        })
        .unwrap_or(true)
}

fn optional_raw_smartsentinel_external_id_matches(
    raw_message: &RawMessage,
    expected: Option<&str>,
) -> bool {
    expected
        .map(|expected| {
            raw_message
                .headers
                .get("smartsentinel")
                .map(|metadata| metadata_external_id_matches(metadata, expected))
                .unwrap_or(false)
        })
        .unwrap_or(true)
}

fn value_string_matches(value: Option<&Value>, expected: &str) -> bool {
    value.and_then(Value::as_str) == Some(expected)
}

fn provenance_search_query_metadata(query: &ProvenanceSearchQuery, limit: u32) -> Value {
    json!({
        "incident_id": query.incident_id.as_deref(),
        "alert_id": query.alert_id.as_deref(),
        "trace_id": query.trace_id.as_deref(),
        "run_id": query.run_id.as_deref(),
        "workflow_id": query.workflow_id.as_deref(),
        "cycle_id": query.cycle_id.as_deref(),
        "correlation_id": query.correlation_id.as_deref(),
        "snapshot_id": query.snapshot_id.as_deref(),
        "node_id": query.node_id.as_deref(),
        "evidence_id": query.evidence_id.as_deref(),
        "external_id": query.external_id.as_deref(),
        "limit": limit
    })
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

async fn create_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateIngestionConnectorRequest>,
) -> Result<(StatusCode, Json<IngestionConnector>), ApiError> {
    require_scope(&state, &auth, "/ingestion/connectors", "connectors:admin")?;
    ensure_connector_secret_exists(&state, request.secret_ref_id)?;
    let connector = IngestionConnector::new(
        state.tenant_id,
        request.connector_key,
        request.connector_type,
        request.connector_profile,
        request.enabled,
        request.display_name,
        request.protocol,
        request.endpoint,
        request.broker_url,
        request.client_id,
        request.topic_filter,
        request.http_path,
        request.payload_format,
        request.content_type,
        request.default_producer_entity_id,
        request.default_feature_of_interest_id,
        request.metadata,
        Utc::now(),
    )?;
    let mut connector = connector;
    connector.secret_ref_id = request.secret_ref_id;
    let connector = state.storage.create_ingestion_connector(connector)?;
    record_connector_event(
        &state,
        "aion:IngestionConnectorCreated",
        &connector,
        Some("Ingestion connector created".to_string()),
    )?;
    reconcile_connector_workers_after_mutation(&state).await;
    Ok((StatusCode::CREATED, Json(connector)))
}

async fn list_ingestion_connectors(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<IngestionConnector>>, ApiError> {
    require_scope(&state, &auth, "/ingestion/connectors", "connectors:read")?;
    Ok(Json(
        state.storage.list_ingestion_connectors(state.tenant_id)?,
    ))
}

async fn get_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<IngestionConnector>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id",
        "connectors:read",
    )?;
    Ok(Json(get_connector(&state, connector_id)?))
}

async fn update_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
    Json(request): Json<UpdateIngestionConnectorRequest>,
) -> Result<Json<IngestionConnector>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id",
        "connectors:admin",
    )?;
    let mut connector = get_connector(&state, connector_id)?;
    let now = Utc::now();

    if let Some(display_name) = request.display_name {
        connector.display_name = Some(display_name);
    }
    if let Some(enabled) = request.enabled {
        connector.enabled = enabled;
    }
    if let Some(protocol) = request.protocol {
        connector.protocol = Some(protocol);
    }
    if let Some(endpoint) = request.endpoint {
        connector.endpoint = Some(endpoint);
    }
    if let Some(broker_url) = request.broker_url {
        connector.broker_url = Some(broker_url);
    }
    if let Some(client_id) = request.client_id {
        connector.client_id = Some(client_id);
    }
    if let Some(topic_filter) = request.topic_filter {
        connector.topic_filter = Some(topic_filter);
    }
    if let Some(http_path) = request.http_path {
        connector.http_path = Some(http_path);
    }
    if let Some(payload_format) = request.payload_format {
        connector.payload_format = Some(payload_format);
    }
    if let Some(content_type) = request.content_type {
        connector.content_type = Some(content_type);
    }
    if let Some(secret_ref_id) = request.secret_ref_id {
        ensure_connector_secret_exists(&state, Some(secret_ref_id))?;
        connector.secret_ref_id = Some(secret_ref_id);
    }
    if let Some(default_producer_entity_id) = request.default_producer_entity_id {
        connector.default_producer_entity_id = Some(default_producer_entity_id);
    }
    if let Some(default_feature_of_interest_id) = request.default_feature_of_interest_id {
        connector.default_feature_of_interest_id = Some(default_feature_of_interest_id);
    }
    if let Some(metadata) = request.metadata {
        connector.metadata = Some(metadata);
    }
    connector.updated_at = now;

    let connector = state.storage.update_ingestion_connector(connector)?;
    record_connector_event(
        &state,
        "aion:IngestionConnectorUpdated",
        &connector,
        Some("Ingestion connector updated".to_string()),
    )?;
    reconcile_connector_workers_after_mutation(&state).await;
    Ok(Json(connector))
}

async fn enable_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<IngestionConnector>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/enable",
        "connectors:admin",
    )?;
    let mut connector = get_connector(&state, connector_id)?;
    connector.set_enabled(true, Utc::now());
    let connector = state.storage.update_ingestion_connector(connector)?;
    record_connector_event(
        &state,
        "aion:IngestionConnectorEnabled",
        &connector,
        Some("Ingestion connector enabled".to_string()),
    )?;
    reconcile_connector_workers_after_mutation(&state).await;
    Ok(Json(connector))
}

async fn disable_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<IngestionConnector>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/disable",
        "connectors:admin",
    )?;
    let mut connector = get_connector(&state, connector_id)?;
    connector.set_enabled(false, Utc::now());
    let connector = state.storage.update_ingestion_connector(connector)?;
    record_connector_event(
        &state,
        "aion:IngestionConnectorDisabled",
        &connector,
        Some("Ingestion connector disabled".to_string()),
    )?;
    reconcile_connector_workers_after_mutation(&state).await;
    Ok(Json(connector))
}

async fn get_ingestion_connector_status(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<IngestionConnectorStatusResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/status",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    Ok(Json(connector_status(&state, &connector)))
}

async fn validate_ingestion_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<TtnConnectorValidation>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/validate",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    Ok(Json(connector_validation(&state, &connector)?))
}

async fn get_ttn_live_readiness_plan(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<TtnLiveReadinessPlan>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-live-readiness-plan",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    Ok(Json(ttn_live_readiness_plan(&state, &connector)?))
}

async fn ttn_live_validate_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
    Json(request): Json<TtnLiveValidationRequest>,
) -> Result<Json<TtnLiveValidationResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-live-validate",
        "connectors:admin",
    )?;
    let connector = get_connector(&state, connector_id)?;
    if connector.connector_profile != ConnectorProfile::TtnV3 {
        return Err(ApiError::bad_request(
            "TTN live validation applies only to ttn-v3 connectors",
        ));
    }

    Ok(Json(
        ttn_live_validation_preflight(&state, &connector, request).await?,
    ))
}

async fn create_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
    Json(request): Json<CreateTtnDeviceMappingRequest>,
) -> Result<(StatusCode, Json<TtnDeviceMappingResponse>), ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings",
        "connectors:admin",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    ensure_entity_exists(&state, request.producer_entity_id)?;
    if let Some(feature_of_interest_id) = request.feature_of_interest_id {
        ensure_entity_exists(&state, feature_of_interest_id)?;
    }

    let now = Utc::now();
    let mut mapping = TtnDeviceMapping::new(
        state.tenant_id,
        connector_id,
        request.ttn_application_id,
        request.ttn_device_id,
        request.producer_entity_id,
        request.feature_of_interest_id,
        request.enabled.unwrap_or(true),
        request.metadata,
        now,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    mapping = state.storage.create_ttn_device_mapping(mapping)?;
    record_ttn_device_mapping_event(
        &state,
        "aion:TtnDeviceMappingCreated",
        &mapping,
        Some("TTN device mapping created".to_string()),
    )?;

    Ok((
        StatusCode::CREATED,
        Json(ttn_device_mapping_response(mapping)),
    ))
}

async fn list_ttn_device_mappings(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
) -> Result<Json<Vec<TtnDeviceMappingResponse>>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mappings = state
        .storage
        .list_ttn_device_mappings(state.tenant_id, connector_id)?
        .into_iter()
        .map(ttn_device_mapping_response)
        .collect();
    Ok(Json(mappings))
}

async fn get_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id",
        "connectors:read",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mapping = state
        .storage
        .get_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(ttn_device_mapping_response(mapping)))
}

async fn update_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<UpdateTtnDeviceMappingRequest>,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id",
        "connectors:admin",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mut mapping = state
        .storage
        .get_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?
        .ok_or_else(ApiError::not_found)?;

    if let Some(producer_entity_id) = request.producer_entity_id {
        ensure_entity_exists(&state, producer_entity_id)?;
    }
    if let Some(Some(feature_of_interest_id)) = request.feature_of_interest_id {
        ensure_entity_exists(&state, feature_of_interest_id)?;
    }

    mapping
        .update_fields(
            request.ttn_application_id,
            request.ttn_device_id,
            request.producer_entity_id,
            request.feature_of_interest_id,
            request.enabled,
            request.metadata,
            Utc::now(),
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let mapping = state.storage.update_ttn_device_mapping(mapping)?;
    record_ttn_device_mapping_event(
        &state,
        "aion:TtnDeviceMappingUpdated",
        &mapping,
        Some("TTN device mapping updated".to_string()),
    )?;
    Ok(Json(ttn_device_mapping_response(mapping)))
}

async fn delete_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id",
        "connectors:admin",
    )?;
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mapping = state
        .storage
        .get_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?
        .ok_or_else(ApiError::not_found)?;
    state
        .storage
        .delete_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?;
    record_ttn_device_mapping_event(
        &state,
        "aion:TtnDeviceMappingDeleted",
        &mapping,
        Some("TTN device mapping deleted".to_string()),
    )?;
    Ok(StatusCode::NO_CONTENT)
}

async fn enable_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id/enable",
        "connectors:admin",
    )?;
    set_ttn_device_mapping_enabled(state, connector_id, mapping_id, true).await
}

async fn disable_ttn_device_mapping(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((connector_id, mapping_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ttn-device-mappings/:mapping_id/disable",
        "connectors:admin",
    )?;
    set_ttn_device_mapping_enabled(state, connector_id, mapping_id, false).await
}

async fn set_ttn_device_mapping_enabled(
    state: AppState,
    connector_id: Uuid,
    mapping_id: Uuid,
    enabled: bool,
) -> Result<Json<TtnDeviceMappingResponse>, ApiError> {
    let connector = get_connector(&state, connector_id)?;
    ensure_ttn_connector(&connector)?;
    let mut mapping = state
        .storage
        .get_ttn_device_mapping(state.tenant_id, connector_id, mapping_id)?
        .ok_or_else(ApiError::not_found)?;
    mapping.set_enabled(enabled, Utc::now());
    let mapping = state.storage.update_ttn_device_mapping(mapping)?;
    record_ttn_device_mapping_event(
        &state,
        if enabled {
            "aion:TtnDeviceMappingEnabled"
        } else {
            "aion:TtnDeviceMappingDisabled"
        },
        &mapping,
        Some(if enabled {
            "TTN device mapping enabled".to_string()
        } else {
            "TTN device mapping disabled".to_string()
        }),
    )?;
    Ok(Json(ttn_device_mapping_response(mapping)))
}

async fn get_ingestion_worker_plan(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<IngestionWorkerPlan>, ApiError> {
    require_scope(&state, &auth, "/ingestion/workers/plan", "connectors:read")?;
    Ok(Json(build_ingestion_worker_plan(&state)?))
}

async fn get_ingestion_workers_status(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<IngestionWorkersStatusResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/workers/status",
        "connectors:read",
    )?;
    Ok(Json(connector_workers_status(&state)?))
}

async fn reconcile_ingestion_workers(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<ReconcileConnectorWorkersResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/workers/reconcile",
        "connectors:admin",
    )?;
    reconcile_connector_workers(state, true).await.map(Json)
}

async fn ingest_smartsentinel_snapshot(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(payload): Json<Value>,
) -> Result<(StatusCode, Json<SmartSentinelSnapshotResponse>), ApiError> {
    require_scope(
        &state,
        &auth,
        "/integrations/smartsentinel/snapshots",
        "smartsentinel:ingest",
    )?;
    let received_at = Utc::now();
    let snapshot_id = payload
        .get("snapshot_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let node_id = payload
        .get("node_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let raw_snapshot_id = snapshot_id.clone();
    let raw_node_id = node_id.clone();
    let provenance_metadata = smartsentinel_provenance_metadata_from_payload(&payload);
    let provenance_summary = smartsentinel_provenance_summary(&provenance_metadata);
    let mut raw_message = RawMessage::new(
        state.tenant_id,
        RawMessageSource::Http,
        Some("/integrations/smartsentinel/snapshots".to_string()),
        node_id.clone(),
        Some(SMARTSENTINEL_PAYLOAD_FORMAT.to_string()),
        Some("application/json".to_string()),
        None,
        None,
        Some(SMARTSENTINEL_PAYLOAD_FORMAT.to_string()),
        json!({
            "protocol": "http",
            "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
            "connector_profile": "smartsentinel",
            "source_endpoint": "/integrations/smartsentinel/snapshots",
            "topic_or_path": "/integrations/smartsentinel/snapshots",
            "snapshot_id": snapshot_id,
            "node_id": node_id,
            "smartsentinel": provenance_metadata,
            "decoder_metadata": {
                "adapter": "SmartSentinelSnapshotDecoder",
                "domain_agnostic": true,
                "actions_executed": false
            }
        }),
        payload_to_bytes(&payload),
        received_at,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    raw_message = state.storage.store_raw_message(raw_message)?;
    let validation = validate_smartsentinel_snapshot(&state, &payload)?;
    record_ingest_event_optional(
        &state,
        "aion:SmartSentinelSnapshotReceived",
        EventSeverity::Info,
        None,
        None,
        Some(raw_message.id),
        Some("SmartSentinel snapshot received".to_string()),
        json!({
            "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
            "snapshot_id": raw_snapshot_id,
            "node_id": raw_node_id,
            "source": provenance_metadata.get("source").cloned(),
            "provenance": provenance_metadata.get("provenance").cloned(),
            "evidence_count": provenance_summary.evidence_count,
            "external_ref_count": provenance_summary.external_ref_count,
            "correlation_id": provenance_summary.correlation_id,
            "trace_id": provenance_summary.trace_id,
            "run_id": provenance_summary.run_id,
            "cycle_id": provenance_summary.cycle_id,
            "validation_warning_count": validation.warnings.len(),
            "validation_error_count": validation.errors.len(),
            "skipped_item_count": validation.skipped_items.len()
        }),
    )?;

    if !validation.errors.is_empty() {
        let message = "SmartSentinel snapshot validation failed";
        state
            .storage
            .mark_raw_message_failed(state.tenant_id, raw_message.id, message)?;
        record_ingest_event_optional(
            &state,
            "aion:SmartSentinelSnapshotMappingFailed",
            EventSeverity::Error,
            None,
            None,
            Some(raw_message.id),
            Some(message.to_string()),
            json!({
                "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
                "snapshot_id": raw_snapshot_id,
                "node_id": raw_node_id,
                "reason": "validation_failed",
                "source": provenance_metadata.get("source").cloned(),
                "provenance": provenance_metadata.get("provenance").cloned(),
                "evidence_count": provenance_summary.evidence_count,
                "external_ref_count": provenance_summary.external_ref_count,
                "correlation_id": provenance_summary.correlation_id,
                "trace_id": provenance_summary.trace_id,
                "run_id": provenance_summary.run_id,
                "cycle_id": provenance_summary.cycle_id,
                "validation_warning_count": validation.warnings.len(),
                "validation_error_count": validation.errors.len(),
                "skipped_item_count": validation.skipped_items.len()
            }),
        )?;
        return Err(ApiError::smartsentinel_validation(message, validation));
    }

    let snapshot = serde_json::from_value::<SmartSentinelSnapshot>(payload.clone())
        .map_err(|err| ApiError::bad_request(format!("invalid SmartSentinel snapshot: {err}")))?;

    let summary =
        match map_smartsentinel_snapshot(&state, snapshot, raw_message.id, received_at, validation)
        {
            Ok(summary) => summary,
            Err(err) => {
                state.storage.mark_raw_message_failed(
                    state.tenant_id,
                    raw_message.id,
                    &err.message,
                )?;
                record_ingest_event_optional(
                    &state,
                    "aion:SmartSentinelSnapshotMappingFailed",
                    EventSeverity::Error,
                    None,
                    None,
                    Some(raw_message.id),
                    Some(err.message.clone()),
                    json!({
                        "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
                        "snapshot_id": raw_snapshot_id,
                        "node_id": raw_node_id,
                        "reason": "mapping_error",
                        "source": provenance_metadata.get("source").cloned(),
                        "provenance": provenance_metadata.get("provenance").cloned(),
                        "evidence_count": provenance_summary.evidence_count,
                        "external_ref_count": provenance_summary.external_ref_count,
                        "correlation_id": provenance_summary.correlation_id,
                        "trace_id": provenance_summary.trace_id,
                        "run_id": provenance_summary.run_id,
                        "cycle_id": provenance_summary.cycle_id
                    }),
                )?;
                return Err(err);
            }
        };
    state
        .storage
        .mark_raw_message_normalized(state.tenant_id, raw_message.id)?;
    record_ingest_event_optional(
        &state,
        "aion:SmartSentinelSnapshotMapped",
        EventSeverity::Info,
        None,
        None,
        Some(raw_message.id),
        Some("SmartSentinel snapshot mapped".to_string()),
        json!({
            "payload_format": SMARTSENTINEL_PAYLOAD_FORMAT,
            "snapshot_id": summary.snapshot_id,
            "node_id": summary.node_id,
            "source": provenance_metadata.get("source").cloned(),
            "provenance": provenance_metadata.get("provenance").cloned(),
            "entities_created": summary.entities_created,
            "entities_updated": summary.entities_updated,
            "entities_reused": summary.entities_reused,
            "entities_skipped": summary.entities_skipped,
            "relationships_created": summary.relationships_created,
            "relationships_reused": summary.relationships_reused,
            "relationships_skipped": summary.relationships_skipped,
            "observations_created": summary.observations_created,
            "events_created": summary.events_created,
            "provenance_present": summary.provenance_present,
            "evidence_count": summary.evidence_count,
            "external_ref_count": summary.external_ref_count,
            "correlation_id": summary.correlation_id,
            "trace_id": summary.trace_id,
            "run_id": summary.run_id,
            "cycle_id": summary.cycle_id,
            "validation_warning_count": summary.validation_warnings.len(),
            "validation_error_count": summary.validation_errors.len(),
            "skipped_item_count": summary.skipped_items.len()
        }),
    )?;

    Ok((StatusCode::CREATED, Json(summary)))
}

async fn ingest_http_for_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
    Json(request): Json<ConnectorHttpIngestRequest>,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ingest",
        "ingestion:write",
    )?;
    let connector = get_connector(&state, connector_id)?;
    if !connector.enabled {
        return Err(ApiError::bad_request("ingestion connector is disabled"));
    }

    let payload_format = request
        .payload_format
        .or_else(|| connector.payload_format.clone())
        .ok_or_else(|| ApiError::bad_request("payload_format is required"))?;
    let protocol = request
        .protocol
        .or_else(|| connector.protocol.clone())
        .unwrap_or_else(|| "http".to_string());
    let content_type = request
        .content_type
        .or_else(|| connector.content_type.clone());

    let mut producer_entity_id = request
        .producer_entity_id
        .or(connector.default_producer_entity_id);
    let mut feature_of_interest_id = request
        .feature_of_interest_id
        .or(connector.default_feature_of_interest_id);
    let mut resolved_ttn_mapping = None;

    if connector.connector_profile == ConnectorProfile::TtnV3
        && is_ttn_uplink_payload_format(&payload_format)
        && (producer_entity_id.is_none() || feature_of_interest_id.is_none())
    {
        let ttn_ids = extract_ttn_uplink_ids(&request.payload)?;
        match resolve_ttn_device_mapping(&state, connector.id, &ttn_ids)? {
            TtnDeviceMappingResolution::Resolved {
                mapping,
                resolution,
            } => {
                if producer_entity_id.is_none() {
                    producer_entity_id = Some(mapping.producer_entity_id);
                }
                if feature_of_interest_id.is_none() {
                    feature_of_interest_id = mapping.feature_of_interest_id;
                }
                resolved_ttn_mapping = Some(ResolvedTtnDeviceMapping {
                    mapping_id: mapping.id,
                    ttn_device_id: mapping.ttn_device_id.clone(),
                    ttn_application_id: mapping.ttn_application_id.clone(),
                    mapping_resolution: resolution,
                });
                record_ttn_device_mapping_event(
                    &state,
                    "aion:TtnDeviceMappingResolved",
                    &mapping,
                    Some("TTN device mapping resolved".to_string()),
                )?;
            }
            TtnDeviceMappingResolution::Missing if producer_entity_id.is_none() => {
                return fail_ttn_device_mapping_resolution(
                    &state,
                    &connector,
                    &payload_format,
                    &protocol,
                    content_type.clone(),
                    &request.payload,
                    &ttn_ids,
                    "ttn_device_mapping_missing",
                    "aion:TtnDeviceMappingMissing",
                );
            }
            TtnDeviceMappingResolution::Ambiguous(error) if producer_entity_id.is_none() => {
                return fail_ttn_device_mapping_resolution(
                    &state,
                    &connector,
                    &payload_format,
                    &protocol,
                    content_type.clone(),
                    &request.payload,
                    &ttn_ids,
                    &error,
                    "aion:TtnDeviceMappingAmbiguous",
                );
            }
            TtnDeviceMappingResolution::Missing | TtnDeviceMappingResolution::Ambiguous(_) => {}
        }
    }

    let producer_entity_id = producer_entity_id
        .ok_or_else(|| ApiError::bad_request("producer_entity_id is required"))?;
    let feature_of_interest_id = feature_of_interest_id
        .ok_or_else(|| ApiError::bad_request("feature_of_interest_id is required"))?;

    let request = HttpIngestRequest {
        producer_entity_id,
        feature_of_interest_id,
        payload_format,
        protocol,
        content_type,
        observed_at: request.observed_at,
        payload: request.payload,
        mapping: request.mapping,
    };

    ingest_http_resolved(&state, request, Some(connector), resolved_ttn_mapping).await
}

async fn ingest_http(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<HttpIngestRequest>,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    require_scope(&state, &auth, "/ingest/http", "ingestion:write")?;
    ingest_http_resolved(&state, request, None, None).await
}

async fn ingest_http_resolved(
    state: &AppState,
    request: HttpIngestRequest,
    connector: Option<IngestionConnector>,
    resolved_ttn_mapping: Option<ResolvedTtnDeviceMapping>,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    ensure_entity_exists(&state, request.producer_entity_id)?;
    ensure_entity_exists(&state, request.feature_of_interest_id)?;

    let received_at = Utc::now();
    let payload_bytes = payload_to_bytes(&request.payload);
    let profile = state
        .storage
        .get_payload_profile(state.tenant_id, request.producer_entity_id)?;
    let mapping_source = if request.mapping.is_some() {
        "request"
    } else if profile
        .as_ref()
        .and_then(|profile| profile.attribute_mapping.as_ref())
        .is_some()
    {
        "payload_profile"
    } else {
        "none"
    };
    let connector_metadata = connector.as_ref().map(connector_event_metadata);
    let source_ref = connector
        .as_ref()
        .and_then(|connector| connector.http_path.clone())
        .unwrap_or_else(|| "/ingest/http".to_string());
    let mut headers = json!({
        "protocol": request.protocol,
        "payload_format": request.payload_format,
        "producer_entity_id": request.producer_entity_id,
        "feature_of_interest_id": request.feature_of_interest_id,
        "source_endpoint": connector
            .as_ref()
            .and_then(|connector| connector.endpoint.clone())
            .or_else(|| Some(source_ref.clone())),
        "topic_or_path": source_ref,
        "decoder_metadata": {
            "decoder": request.payload_format,
            "mapping_source": mapping_source
        }
    });
    if let (Some(object), Some(metadata)) = (headers.as_object_mut(), connector_metadata.clone()) {
        object.insert("connector".to_string(), metadata.clone());
        object.insert("connector_id".to_string(), metadata["connector_id"].clone());
        object.insert(
            "connector_key".to_string(),
            metadata["connector_key"].clone(),
        );
        object.insert(
            "connector_profile".to_string(),
            metadata["connector_profile"].clone(),
        );
    }

    let mut raw_message = RawMessage::new(
        state.tenant_id,
        RawMessageSource::Http,
        Some(source_ref),
        Some(request.producer_entity_id.to_string()),
        Some(request.payload_format.clone()),
        request.content_type.clone(),
        Some(request.producer_entity_id),
        Some(request.feature_of_interest_id),
        Some(request.payload_format.clone()),
        headers,
        payload_bytes.clone(),
        received_at,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    raw_message = state.storage.store_raw_message(raw_message)?;

    let decoder_config = request
        .mapping
        .or_else(|| profile.and_then(|profile| profile.attribute_mapping))
        .or_else(|| {
            if is_ttn_uplink_payload_format(&request.payload_format) {
                connector
                    .as_ref()
                    .and_then(|connector| connector.metadata.clone())
            } else {
                None
            }
        });
    if payload_format_requires_mapping(&request.payload_format) && decoder_config.is_none() {
        let message = format!(
            "{} payloads require request mapping or producer PayloadProfile attribute_mapping",
            request.payload_format
        );
        state
            .storage
            .mark_raw_message_failed(state.tenant_id, raw_message.id, &message)?;
        record_ingest_event(
            &state,
            "aion:PayloadIngestionFailed",
            EventSeverity::Error,
            request.producer_entity_id,
            request.feature_of_interest_id,
            raw_message.id,
            Some(message.clone()),
            metadata_with_connector(
                json!({
                    "payload_format": request.payload_format,
                    "reason": "missing_mapping"
                }),
                connector_metadata.clone(),
            ),
        )?;
        return Err(ApiError::bad_request(message));
    }

    let decoder = match decoder_for_format(&request.payload_format) {
        Ok(decoder) => decoder,
        Err(err) => {
            state
                .storage
                .mark_raw_message_failed(state.tenant_id, raw_message.id, &err.message)?;
            record_ingest_event(
                &state,
                "aion:PayloadIngestionFailed",
                EventSeverity::Error,
                request.producer_entity_id,
                request.feature_of_interest_id,
                raw_message.id,
                Some(err.message.clone()),
                metadata_with_connector(
                    json!({
                        "payload_format": request.payload_format,
                        "reason": "unsupported_payload_format"
                    }),
                    connector_metadata.clone(),
                ),
            )?;
            return Err(err);
        }
    };
    let decode_result = decoder.decode(DecodeInput {
        tenant_id: state.tenant_id,
        device_key: Some(request.producer_entity_id.to_string()),
        format: PayloadFormat::from_str(&request.payload_format).unwrap(),
        content_type: request.content_type,
        payload: payload_bytes,
        received_at: request.observed_at.unwrap_or(received_at),
        config: decoder_config,
    });

    let decoded = match decode_result {
        Ok(decoded) => decoded,
        Err(err) => {
            state.storage.mark_raw_message_failed(
                state.tenant_id,
                raw_message.id,
                err.message(),
            )?;
            record_ingest_event(
                &state,
                "aion:PayloadIngestionFailed",
                EventSeverity::Error,
                request.producer_entity_id,
                request.feature_of_interest_id,
                raw_message.id,
                Some(err.message().to_string()),
                metadata_with_connector(
                    json!({
                        "payload_format": request.payload_format,
                        "reason": "decoder_error"
                    }),
                    connector_metadata.clone(),
                ),
            )?;
            return Err(ApiError::bad_request(err.to_string()));
        }
    };

    let ingest_metadata = decoded_ingest_metadata(&decoded);
    let mut observations = Vec::with_capacity(decoded.len());
    for measurement in decoded {
        let observation = Observation::new(
            state.tenant_id,
            request.producer_entity_id,
            request.feature_of_interest_id,
            measurement.observed_property,
            measurement.value,
            measurement.unit,
            measurement.time,
            received_at,
            request.protocol.clone(),
            request.payload_format.clone(),
            Some(raw_message.id),
            connector_metadata.clone().unwrap_or_else(|| json!({})),
            measurement.metadata,
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let observation = state.storage.store_observation(observation)?;
        evaluate_rules_for_observation(&state, &observation, true)?;
        observations.push(observation);
    }

    state
        .storage
        .mark_raw_message_normalized(state.tenant_id, raw_message.id)?;
    let mut payload_event_metadata = json!({
        "payload_format": request.payload_format,
        "observation_count": observations.len()
    });
    merge_json_object(&mut payload_event_metadata, ingest_metadata);
    if let Some(mapping) = resolved_ttn_mapping {
        merge_json_object(
            &mut payload_event_metadata,
            json!({
                "ttn_mapping_id": mapping.mapping_id,
                "mapping_resolution": mapping.mapping_resolution,
                "ttn_device_id": mapping.ttn_device_id,
                "ttn_application_id": mapping.ttn_application_id
            }),
        );
    }
    record_ingest_event(
        &state,
        "aion:PayloadIngested",
        EventSeverity::Info,
        request.producer_entity_id,
        request.feature_of_interest_id,
        raw_message.id,
        Some("Payload ingested and normalized".to_string()),
        metadata_with_connector(payload_event_metadata, connector_metadata),
    )?;

    Ok((
        StatusCode::CREATED,
        Json(HttpIngestResponse {
            raw_message_id: raw_message.id,
            observations,
        }),
    ))
}

fn smartsentinel_provenance_metadata_from_payload(payload: &Value) -> Value {
    json!({
        "source": payload.get("source").cloned(),
        "provenance": payload.get("provenance").cloned(),
        "evidence": payload.get("evidence").cloned().unwrap_or_else(|| json!([]))
    })
}

fn smartsentinel_provenance_metadata(snapshot: &SmartSentinelSnapshot) -> Value {
    json!({
        "source": snapshot.source,
        "provenance": snapshot.provenance,
        "evidence": snapshot.evidence
    })
}

fn smartsentinel_provenance_summary(metadata: &Value) -> SmartSentinelProvenanceSummary {
    let provenance = metadata.get("provenance").filter(|value| value.is_object());
    SmartSentinelProvenanceSummary {
        provenance_present: provenance.is_some(),
        evidence_count: metadata
            .get("evidence")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter(|value| smartsentinel_evidence_reference_is_usable(value))
                    .count()
            })
            .unwrap_or(0),
        external_ref_count: provenance
            .and_then(|value| value.get("external_refs"))
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0),
        correlation_id: provenance
            .and_then(|value| optional_trimmed_string(value, "correlation_id")),
        trace_id: provenance.and_then(|value| optional_trimmed_string(value, "trace_id")),
        run_id: provenance.and_then(|value| optional_trimmed_string(value, "run_id")),
        cycle_id: provenance.and_then(|value| optional_trimmed_string(value, "cycle_id")),
    }
}

fn optional_trimmed_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn smartsentinel_evidence_reference_is_usable(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object
        .get("uri")
        .map(|value| value.is_string())
        .unwrap_or(true)
}

fn smartsentinel_base_metadata(
    snapshot_id: &str,
    node_id: &str,
    provenance_metadata: &Value,
) -> Value {
    let summary = smartsentinel_provenance_summary(provenance_metadata);
    json!({
        "adapter": "SmartSentinelSnapshotDecoder",
        "snapshot_id": snapshot_id,
        "node_id": node_id,
        "source": provenance_metadata.get("source").cloned(),
        "provenance": provenance_metadata.get("provenance").cloned(),
        "evidence": provenance_metadata.get("evidence").cloned().unwrap_or_else(|| json!([])),
        "evidence_count": summary.evidence_count,
        "external_ref_count": summary.external_ref_count,
        "correlation_id": summary.correlation_id,
        "trace_id": summary.trace_id,
        "run_id": summary.run_id,
        "cycle_id": summary.cycle_id,
        "uri_fetch_attempted": false
    })
}

fn validate_smartsentinel_snapshot(
    state: &AppState,
    payload: &Value,
) -> Result<SmartSentinelValidationReport, ApiError> {
    let mut report = SmartSentinelValidationReport {
        warnings: Vec::new(),
        errors: Vec::new(),
        skipped_items: Vec::new(),
    };

    let Some(object) = payload.as_object() else {
        report.errors.push(smartsentinel_issue(
            "$",
            "snapshot_not_object",
            "SmartSentinel snapshot must be a JSON object",
        ));
        return Ok(report);
    };

    let node_id = required_string(object.get("node_id"), "$.node_id", "node_id", &mut report);
    required_string(
        object.get("snapshot_id"),
        "$.snapshot_id",
        "snapshot_id",
        &mut report,
    );
    if let Some(value) = object.get("observed_at") {
        validate_optional_rfc3339(value, "$.observed_at", "observed_at", &mut report);
    }

    let mut snapshot_entity_ids = HashSet::new();
    if let Some(entities) = object.get("entities") {
        match entities.as_array() {
            Some(entities) => {
                for (index, entity) in entities.iter().enumerate() {
                    let path = format!("$.entities[{index}]");
                    let Some(entity_object) = entity.as_object() else {
                        report.errors.push(smartsentinel_issue(
                            path,
                            "entity_not_object",
                            "SmartSentinel entity must be a JSON object",
                        ));
                        continue;
                    };
                    if let Some(entity_id) = required_string(
                        entity_object.get("id"),
                        format!("{path}.id"),
                        "entity id",
                        &mut report,
                    ) {
                        snapshot_entity_ids.insert(entity_id);
                    }
                    required_string(
                        entity_object.get("type"),
                        format!("{path}.type"),
                        "entity type",
                        &mut report,
                    );
                    if let Some(properties) = entity_object.get("properties") {
                        if !properties.is_object() {
                            report.errors.push(smartsentinel_issue(
                                format!("{path}.properties"),
                                "entity_properties_not_object",
                                "SmartSentinel entity properties must be a JSON object when present",
                            ));
                        }
                    }
                }
            }
            None => report.errors.push(smartsentinel_issue(
                "$.entities",
                "entities_not_array",
                "entities must be an array when present",
            )),
        }
    }

    let node_id = node_id.unwrap_or_default();
    let relationships = object.get("relationships").and_then(Value::as_array);
    if object.get("relationships").is_some() && relationships.is_none() {
        report.errors.push(smartsentinel_issue(
            "$.relationships",
            "relationships_not_array",
            "relationships must be an array when present",
        ));
    }
    if let Some(relationships) = relationships {
        for (index, relationship) in relationships.iter().enumerate() {
            let path = format!("$.relationships[{index}]");
            let Some(relationship_object) = relationship.as_object() else {
                report.errors.push(smartsentinel_issue(
                    path,
                    "relationship_not_object",
                    "SmartSentinel relationship must be a JSON object",
                ));
                continue;
            };
            let source = required_string(
                relationship_object.get("source"),
                format!("{path}.source"),
                "relationship source",
                &mut report,
            );
            let relationship_type = required_string(
                relationship_object.get("type"),
                format!("{path}.type"),
                "relationship type",
                &mut report,
            );
            let target = required_string(
                relationship_object.get("target"),
                format!("{path}.target"),
                "relationship target",
                &mut report,
            );
            if let (Some(source), Some(target)) = (source.as_deref(), target.as_deref()) {
                if source == target {
                    report.warnings.push(smartsentinel_issue(
                        path.clone(),
                        "relationship_self_reference",
                        "relationship source and target are the same; item will be skipped",
                    ));
                    report.skipped_items.push(SmartSentinelSkippedItem {
                        path: path.clone(),
                        reason: "relationship_self_reference".to_string(),
                    });
                }
                if !smartsentinel_entity_ref_resolves(
                    state,
                    &node_id,
                    &snapshot_entity_ids,
                    source,
                )? {
                    report.errors.push(smartsentinel_issue(
                        format!("{path}.source"),
                        "relationship_source_unknown",
                        "relationship source does not reference a snapshot entity or existing mapped entity",
                    ));
                }
                if !smartsentinel_entity_ref_resolves(
                    state,
                    &node_id,
                    &snapshot_entity_ids,
                    target,
                )? {
                    report.errors.push(smartsentinel_issue(
                        format!("{path}.target"),
                        "relationship_target_unknown",
                        "relationship target does not reference a snapshot entity or existing mapped entity",
                    ));
                }
            }
            if relationship_type.is_none() {
                report.skipped_items.push(SmartSentinelSkippedItem {
                    path,
                    reason: "relationship_missing_type".to_string(),
                });
            }
        }
    }

    validate_smartsentinel_observation_items(
        state,
        object.get("observations"),
        &node_id,
        &snapshot_entity_ids,
        &mut report,
    )?;
    validate_smartsentinel_event_items(
        state,
        object.get("events"),
        &node_id,
        &snapshot_entity_ids,
        &mut report,
    )?;
    validate_smartsentinel_evidence_items(object.get("evidence"), &mut report);

    Ok(report)
}

fn validate_smartsentinel_evidence_items(
    value: Option<&Value>,
    report: &mut SmartSentinelValidationReport,
) {
    let Some(value) = value else {
        return;
    };
    let Some(evidence) = value.as_array() else {
        report.warnings.push(smartsentinel_issue(
            "$.evidence",
            "evidence_not_array",
            "evidence must be an array when present; evidence references will be ignored",
        ));
        report.skipped_items.push(SmartSentinelSkippedItem {
            path: "$.evidence".to_string(),
            reason: "evidence_not_array".to_string(),
        });
        return;
    };

    for (index, evidence) in evidence.iter().enumerate() {
        let path = format!("$.evidence[{index}]");
        let Some(object) = evidence.as_object() else {
            report.warnings.push(smartsentinel_issue(
                path.clone(),
                "evidence_not_object",
                "evidence entry must be a JSON object; item will be skipped",
            ));
            report.skipped_items.push(SmartSentinelSkippedItem {
                path,
                reason: "evidence_not_object".to_string(),
            });
            continue;
        };

        if object
            .get("evidence_type")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            report.warnings.push(smartsentinel_issue(
                format!("{path}.evidence_type"),
                "evidence_type_defaulted",
                "evidence_type is missing; it will be interpreted as custom",
            ));
        }

        if let Some(uri) = object.get("uri") {
            if !uri.is_string() {
                report.warnings.push(smartsentinel_issue(
                    format!("{path}.uri"),
                    "evidence_uri_invalid",
                    "evidence uri must be a string when present; item will be skipped",
                ));
                report.skipped_items.push(SmartSentinelSkippedItem {
                    path: path.clone(),
                    reason: "evidence_uri_invalid".to_string(),
                });
            }
        }

        if let Some(collected_at) = object.get("collected_at") {
            let before = report.errors.len();
            validate_optional_rfc3339(
                collected_at,
                format!("{path}.collected_at"),
                "collected_at",
                report,
            );
            if report.errors.len() > before {
                if let Some(issue) = report.errors.pop() {
                    report.warnings.push(issue);
                }
            }
        }
    }
}

fn validate_smartsentinel_observation_items(
    state: &AppState,
    value: Option<&Value>,
    node_id: &str,
    snapshot_entity_ids: &HashSet<String>,
    report: &mut SmartSentinelValidationReport,
) -> Result<(), ApiError> {
    let Some(value) = value else {
        return Ok(());
    };
    let Some(observations) = value.as_array() else {
        report.errors.push(smartsentinel_issue(
            "$.observations",
            "observations_not_array",
            "observations must be an array when present",
        ));
        return Ok(());
    };
    for (index, observation) in observations.iter().enumerate() {
        let path = format!("$.observations[{index}]");
        let Some(observation_object) = observation.as_object() else {
            report.errors.push(smartsentinel_issue(
                path,
                "observation_not_object",
                "SmartSentinel observation must be a JSON object",
            ));
            continue;
        };
        let entity_id = required_string(
            observation_object.get("entity_id"),
            format!("{path}.entity_id"),
            "observation entity_id",
            report,
        );
        required_string(
            observation_object.get("observed_property"),
            format!("{path}.observed_property"),
            "observation observed_property",
            report,
        );
        if !observation_object.contains_key("value") {
            report.errors.push(smartsentinel_issue(
                format!("{path}.value"),
                "observation_value_missing",
                "observation value is required",
            ));
        }
        if let Some(value) = observation_object.get("observed_at") {
            validate_optional_rfc3339(value, format!("{path}.observed_at"), "observed_at", report);
        }
        if let Some(entity_id) = entity_id {
            if !smartsentinel_entity_ref_resolves(state, node_id, snapshot_entity_ids, &entity_id)?
            {
                report.errors.push(smartsentinel_issue(
                    format!("{path}.entity_id"),
                    "observation_entity_unknown",
                    "observation entity_id does not reference a snapshot entity or existing mapped entity",
                ));
            }
        }
    }
    Ok(())
}

fn require_same_tenant_for_target_entity(
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

fn require_same_tenant_for_target_command(
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

fn require_same_tenant_for_target_executor(
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

fn require_same_tenant_for_target_action(
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

fn validate_smartsentinel_event_items(
    state: &AppState,
    value: Option<&Value>,
    node_id: &str,
    snapshot_entity_ids: &HashSet<String>,
    report: &mut SmartSentinelValidationReport,
) -> Result<(), ApiError> {
    let Some(value) = value else {
        return Ok(());
    };
    let Some(events) = value.as_array() else {
        report.errors.push(smartsentinel_issue(
            "$.events",
            "events_not_array",
            "events must be an array when present",
        ));
        return Ok(());
    };
    for (index, event) in events.iter().enumerate() {
        let path = format!("$.events[{index}]");
        let Some(event_object) = event.as_object() else {
            report.errors.push(smartsentinel_issue(
                path,
                "event_not_object",
                "SmartSentinel event must be a JSON object",
            ));
            continue;
        };
        required_string(
            event_object.get("event_type"),
            format!("{path}.event_type"),
            "event_type",
            report,
        );
        if let Some(severity) = event_object.get("severity") {
            match severity.as_str() {
                Some("debug" | "info" | "warning" | "error" | "critical") => {}
                _ => report.errors.push(smartsentinel_issue(
                    format!("{path}.severity"),
                    "event_severity_invalid",
                    "event severity must be debug, info, warning, error, or critical",
                )),
            }
        }
        for field_name in ["source_entity_id", "target_entity_id"] {
            if let Some(value) = event_object.get(field_name) {
                match value.as_str().map(str::trim).filter(|value| !value.is_empty()) {
                    Some(entity_id)
                        if smartsentinel_entity_ref_resolves(
                            state,
                            node_id,
                            snapshot_entity_ids,
                            entity_id,
                        )? => {}
                    Some(_) => report.errors.push(smartsentinel_issue(
                        format!("{path}.{field_name}"),
                        "event_entity_unknown",
                        "event entity reference does not reference a snapshot entity or existing mapped entity",
                    )),
                    None => report.errors.push(smartsentinel_issue(
                        format!("{path}.{field_name}"),
                        "event_entity_invalid",
                        "event entity reference must be a non-empty string when present",
                    )),
                }
            }
        }
        if let Some(value) = event_object.get("occurred_at") {
            validate_optional_rfc3339(value, format!("{path}.occurred_at"), "occurred_at", report);
        }
    }
    Ok(())
}

fn required_string(
    value: Option<&Value>,
    path: impl Into<String>,
    label: &str,
    report: &mut SmartSentinelValidationReport,
) -> Option<String> {
    let path = path.into();
    match value.and_then(Value::as_str).map(str::trim) {
        Some(value) if !value.is_empty() => Some(value.to_string()),
        _ => {
            report.errors.push(smartsentinel_issue(
                path,
                format!("{}_missing", label.replace(' ', "_")),
                format!("{label} is required"),
            ));
            None
        }
    }
}

fn validate_optional_rfc3339(
    value: &Value,
    path: impl Into<String>,
    label: &str,
    report: &mut SmartSentinelValidationReport,
) {
    if value
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .is_none()
    {
        report.errors.push(smartsentinel_issue(
            path,
            format!("{label}_invalid"),
            format!("{label} must be an RFC3339 timestamp when present"),
        ));
    }
}

fn smartsentinel_entity_ref_resolves(
    state: &AppState,
    node_id: &str,
    snapshot_entity_ids: &HashSet<String>,
    snapshot_entity_id: &str,
) -> Result<bool, ApiError> {
    if snapshot_entity_ids.contains(snapshot_entity_id) {
        return Ok(true);
    }
    let entity_key = smartsentinel_entity_key(node_id, snapshot_entity_id);
    Ok(state
        .storage
        .get_entity_by_key(state.tenant_id, &entity_key)?
        .is_some())
}

fn smartsentinel_issue(
    path: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> SmartSentinelValidationIssue {
    SmartSentinelValidationIssue {
        path: path.into(),
        code: code.into(),
        message: message.into(),
    }
}

fn map_smartsentinel_snapshot(
    state: &AppState,
    snapshot: SmartSentinelSnapshot,
    raw_message_id: Uuid,
    received_at: DateTime<Utc>,
    validation: SmartSentinelValidationReport,
) -> Result<SmartSentinelSnapshotResponse, ApiError> {
    let snapshot_id = snapshot.snapshot_id.clone();
    let node_id = snapshot.node_id.clone();
    let observed_at = snapshot.observed_at.unwrap_or(received_at);
    let provenance_metadata = smartsentinel_provenance_metadata(&snapshot);
    let provenance_summary = smartsentinel_provenance_summary(&provenance_metadata);
    let mut entity_ids = HashMap::new();
    let mut entities_created = 0;
    let mut entities_updated = 0;
    let mut entities_reused = 0;
    let entities_skipped = 0;
    let mut relationships_created = 0;
    let mut relationships_reused = 0;
    let mut relationships_skipped = 0;
    let mut observations_created = 0;
    let mut events_created = 0;
    let observer_raw_id = format!("host:{node_id}");

    for snapshot_entity in &snapshot.entities {
        let entity_key = smartsentinel_entity_key(&node_id, &snapshot_entity.id);
        let (entity, status) = upsert_smartsentinel_entity(
            state,
            &entity_key,
            &node_id,
            &snapshot_id,
            &snapshot,
            snapshot_entity,
            received_at,
        )?;
        entity_ids.insert(snapshot_entity.id.clone(), entity.id);
        match status {
            SmartSentinelEntityMappingStatus::Created => entities_created += 1,
            SmartSentinelEntityMappingStatus::Updated => entities_updated += 1,
            SmartSentinelEntityMappingStatus::Reused => entities_reused += 1,
        }
    }

    let observer_entity_id = entity_ids.get(&observer_raw_id).copied().or_else(|| {
        snapshot
            .entities
            .first()
            .and_then(|entity| entity_ids.get(&entity.id).copied())
    });

    for relationship in &snapshot.relationships {
        let source_entity_id = resolve_smartsentinel_mapped_entity_id(
            state,
            &node_id,
            &entity_ids,
            &relationship.source,
        )?;
        let target_entity_id = resolve_smartsentinel_mapped_entity_id(
            state,
            &node_id,
            &entity_ids,
            &relationship.target,
        )?;
        if relationship.relationship_type.trim().is_empty() || source_entity_id == target_entity_id
        {
            relationships_skipped += 1;
            continue;
        }
        if smartsentinel_relationship_exists(
            state,
            source_entity_id,
            &relationship.relationship_type,
            target_entity_id,
        )? {
            relationships_reused += 1;
        } else {
            let relationship = Relationship::new(
                state.tenant_id,
                source_entity_id,
                relationship.relationship_type.clone(),
                target_entity_id,
                json!({
                    "@context": smartsentinel_jsonld_context(),
                    "@type": "aion:Relationship",
                    "sentinel:snapshotId": snapshot_id,
                    "sentinel:nodeId": node_id,
                    "sentinel:source": relationship.source,
                    "sentinel:target": relationship.target
                }),
                received_at,
            )
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
            state.storage.create_relationship(relationship)?;
            relationships_created += 1;
        }
    }

    for snapshot_entity in &snapshot.entities {
        if let Some(status) = snapshot_entity
            .status
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let feature_of_interest_id = entity_ids[&snapshot_entity.id];
            let producer_entity_id = observer_entity_id.unwrap_or(feature_of_interest_id);
            let observation = Observation::new(
                state.tenant_id,
                producer_entity_id,
                feature_of_interest_id,
                format!("{}Status", snapshot_entity.entity_type),
                ObservationValue::Text {
                    value: status.to_string(),
                },
                None,
                observed_at,
                received_at,
                "http",
                SMARTSENTINEL_PAYLOAD_FORMAT,
                Some(raw_message_id),
                json!({"source": "smartsentinel"}),
                {
                    let mut metadata =
                        smartsentinel_base_metadata(&snapshot_id, &node_id, &provenance_metadata);
                    merge_json_object(
                        &mut metadata,
                        json!({
                        "source": "entity_status",
                        "snapshot_entity_id": snapshot_entity.id
                            }),
                    );
                    metadata
                },
            )
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
            let observation = state.storage.store_observation(observation)?;
            evaluate_rules_for_observation(state, &observation, true)?;
            observations_created += 1;
        }
    }

    for snapshot_observation in &snapshot.observations {
        let feature_of_interest_id = resolve_smartsentinel_mapped_entity_id(
            state,
            &node_id,
            &entity_ids,
            &snapshot_observation.entity_id,
        )?;
        let producer_entity_id = observer_entity_id.unwrap_or(feature_of_interest_id);
        let observation = Observation::new(
            state.tenant_id,
            producer_entity_id,
            feature_of_interest_id,
            snapshot_observation.observed_property.clone(),
            observation_value_from_json(&snapshot_observation.value),
            snapshot_observation.unit.clone(),
            snapshot_observation.observed_at.unwrap_or(observed_at),
            received_at,
            "http",
            SMARTSENTINEL_PAYLOAD_FORMAT,
            Some(raw_message_id),
            json!({"source": "smartsentinel"}),
            {
                let mut metadata =
                    smartsentinel_base_metadata(&snapshot_id, &node_id, &provenance_metadata);
                merge_json_object(
                    &mut metadata,
                    json!({
                    "source": "snapshot_observation",
                    "snapshot_entity_id": snapshot_observation.entity_id,
                    "observation_source": snapshot_observation.source,
                    "evidence_refs": snapshot_observation.evidence_refs
                        }),
                );
                metadata
            },
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let observation = state.storage.store_observation(observation)?;
        evaluate_rules_for_observation(state, &observation, true)?;
        observations_created += 1;
    }

    for snapshot_event in &snapshot.events {
        let source_entity_id = snapshot_event
            .source_entity_id
            .as_deref()
            .map(|entity_id| {
                resolve_smartsentinel_mapped_entity_id(state, &node_id, &entity_ids, entity_id)
            })
            .transpose()?;
        let target_entity_id = snapshot_event
            .target_entity_id
            .as_deref()
            .map(|entity_id| {
                resolve_smartsentinel_mapped_entity_id(state, &node_id, &entity_ids, entity_id)
            })
            .transpose()?;
        let event = Event::new(
            state.tenant_id,
            snapshot_event.event_type.clone(),
            snapshot_event
                .severity
                .clone()
                .unwrap_or(EventSeverity::Info),
            source_entity_id,
            target_entity_id,
            snapshot_event.message.clone(),
            snapshot_event.occurred_at.unwrap_or(observed_at),
            Some(observed_at),
            Some(snapshot_id.clone()),
            Some(raw_message_id),
            None,
            None,
            None,
            None,
            Some(smartsentinel_event_metadata(
                &snapshot_id,
                &node_id,
                &provenance_metadata,
                snapshot_event,
            )),
            received_at,
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let event = state.storage.store_event(event)?;
        evaluate_rules_for_event(state, &event, true)?;
        events_created += 1;
    }

    Ok(SmartSentinelSnapshotResponse {
        raw_message_id,
        snapshot_id,
        node_id,
        entities_created,
        entities_updated,
        entities_reused,
        entities_skipped,
        relationships_created,
        relationships_reused,
        relationships_skipped,
        observations_created,
        events_created,
        validation_warnings: validation.warnings,
        validation_errors: validation.errors,
        skipped_items: validation.skipped_items,
        provenance_present: provenance_summary.provenance_present,
        evidence_count: provenance_summary.evidence_count,
        external_ref_count: provenance_summary.external_ref_count,
        correlation_id: provenance_summary.correlation_id,
        trace_id: provenance_summary.trace_id,
        run_id: provenance_summary.run_id,
        cycle_id: provenance_summary.cycle_id,
    })
}

fn upsert_smartsentinel_entity(
    state: &AppState,
    entity_key: &str,
    node_id: &str,
    snapshot_id: &str,
    snapshot: &SmartSentinelSnapshot,
    snapshot_entity: &SmartSentinelSnapshotEntity,
    now: DateTime<Utc>,
) -> Result<(Entity, SmartSentinelEntityMappingStatus), ApiError> {
    let jsonld =
        smartsentinel_entity_jsonld(entity_key, node_id, snapshot_id, snapshot, snapshot_entity);
    if let Some(mut entity) = state
        .storage
        .get_entity_by_key(state.tenant_id, entity_key)?
    {
        let new_entity_type = snapshot_entity.entity_type.clone();
        let unchanged = entity.entity_type == new_entity_type && entity.jsonld == jsonld;
        if unchanged {
            return Ok((entity, SmartSentinelEntityMappingStatus::Reused));
        }
        entity.entity_type = new_entity_type;
        entity.jsonld = jsonld;
        entity.updated_at = now;
        let entity = state.storage.update_entity(entity)?;
        return Ok((entity, SmartSentinelEntityMappingStatus::Updated));
    }

    let entity = Entity::new(
        state.tenant_id,
        entity_key,
        snapshot_entity.entity_type.clone(),
        jsonld,
        now,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok((
        state.storage.create_entity(entity)?,
        SmartSentinelEntityMappingStatus::Created,
    ))
}

fn smartsentinel_entity_jsonld(
    entity_key: &str,
    node_id: &str,
    snapshot_id: &str,
    snapshot: &SmartSentinelSnapshot,
    snapshot_entity: &SmartSentinelSnapshotEntity,
) -> Value {
    let related_evidence = snapshot
        .evidence
        .iter()
        .filter(|evidence| {
            evidence
                .get("related_entity_id")
                .and_then(Value::as_str)
                .map(|entity_id| entity_id == snapshot_entity.id)
                .unwrap_or(false)
                && smartsentinel_evidence_reference_is_usable(evidence)
        })
        .cloned()
        .collect::<Vec<_>>();
    let jsonld = json!({
        "@context": smartsentinel_jsonld_context(),
        "@id": format!("urn:aion:smartsentinel:{node_id}:{}", snapshot_entity.id),
        "@type": snapshot_entity.entity_type,
        "entity_key": entity_key,
        "name": snapshot_entity.name,
        "sentinel:externalId": snapshot_entity.id,
        "sentinel:nodeId": node_id,
        "sentinel:snapshotId": snapshot_id,
        "sentinel:status": snapshot_entity.status,
        "sentinel:properties": snapshot_entity.properties,
        "sentinel:evidence": related_evidence
    });
    jsonld
}

fn smartsentinel_event_metadata(
    snapshot_id: &str,
    node_id: &str,
    provenance_metadata: &Value,
    snapshot_event: &SmartSentinelSnapshotEvent,
) -> Value {
    let mut metadata = smartsentinel_base_metadata(snapshot_id, node_id, provenance_metadata);
    merge_json_object(
        &mut metadata,
        json!({
            "source": "snapshot_event",
            "incident_id": snapshot_event.incident_id,
            "alert_id": snapshot_event.alert_id,
            "workflow_id": snapshot_event.workflow_id,
            "run_id": snapshot_event.run_id,
            "trace_id": snapshot_event.trace_id,
            "evidence_refs": snapshot_event.evidence_refs
        }),
    );
    metadata
}

fn smartsentinel_relationship_exists(
    state: &AppState,
    source_entity_id: Uuid,
    relationship_type: &str,
    target_entity_id: Uuid,
) -> Result<bool, ApiError> {
    Ok(state
        .storage
        .list_relationships(
            state.tenant_id,
            Some(source_entity_id),
            Some(target_entity_id),
        )?
        .into_iter()
        .any(|relationship| relationship.relationship_type == relationship_type))
}

fn resolve_smartsentinel_mapped_entity_id(
    state: &AppState,
    node_id: &str,
    entity_ids: &HashMap<String, Uuid>,
    snapshot_entity_id: &str,
) -> Result<Uuid, ApiError> {
    if let Some(entity_id) = entity_ids.get(snapshot_entity_id).copied() {
        return Ok(entity_id);
    }
    let entity_key = smartsentinel_entity_key(node_id, snapshot_entity_id);
    state
        .storage
        .get_entity_by_key(state.tenant_id, &entity_key)?
        .map(|entity| entity.id)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "SmartSentinel entity reference '{}' does not reference a mapped entity",
                snapshot_entity_id
            ))
        })
}

fn smartsentinel_entity_key(node_id: &str, snapshot_entity_id: &str) -> String {
    format!("smartsentinel:{node_id}:{snapshot_entity_id}")
}

fn smartsentinel_jsonld_context() -> Value {
    json!({
        "aion": "https://aioncore.org/ns#",
        "sentinel": "https://aioncore.org/ns/smartsentinel#"
    })
}

fn observation_value_from_json(value: &Value) -> ObservationValue {
    if let Some(value) = value.as_f64() {
        ObservationValue::Number { value }
    } else if let Some(value) = value.as_str() {
        ObservationValue::Text {
            value: value.to_string(),
        }
    } else if let Some(value) = value.as_bool() {
        ObservationValue::Bool { value }
    } else {
        ObservationValue::Json {
            value: value.clone(),
        }
    }
}

fn decoder_for_format(payload_format: &str) -> Result<Box<dyn PayloadDecoder>, ApiError> {
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

fn payload_format_requires_mapping(payload_format: &str) -> bool {
    matches!(
        payload_format
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .as_str(),
        "ultralight" | "ultra_light"
    )
}

fn is_ttn_uplink_payload_format(payload_format: &str) -> bool {
    matches!(
        payload_format
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .as_str(),
        "ttn_uplink_json"
    )
}

fn extract_ttn_uplink_ids(payload: &Value) -> Result<TtnUplinkIds, ApiError> {
    let payload = payload_as_json_value(payload)?;
    let end_device_ids = payload
        .get("end_device_ids")
        .and_then(Value::as_object)
        .ok_or_else(|| ApiError::bad_request("TTN uplink payload is missing end_device_ids"))?;
    let device_id = end_device_ids
        .get("device_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ApiError::bad_request("TTN uplink payload is missing device_id"))?;
    let application_id = end_device_ids
        .get("application_ids")
        .and_then(Value::as_object)
        .and_then(|application_ids| application_ids.get("application_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    Ok(TtnUplinkIds {
        device_id,
        application_id,
    })
}

fn payload_as_json_value(payload: &Value) -> Result<Value, ApiError> {
    if let Some(payload) = payload.as_str() {
        serde_json::from_str(payload)
            .map_err(|err| ApiError::bad_request(format!("invalid TTN uplink JSON: {err}")))
    } else {
        Ok(payload.clone())
    }
}

fn resolve_ttn_device_mapping(
    state: &AppState,
    connector_id: Uuid,
    ttn_ids: &TtnUplinkIds,
) -> Result<TtnDeviceMappingResolution, ApiError> {
    let mappings = state
        .storage
        .list_ttn_device_mappings(state.tenant_id, connector_id)?;
    let enabled_matches = mappings
        .into_iter()
        .filter(|mapping| mapping.enabled && mapping.ttn_device_id == ttn_ids.device_id)
        .collect::<Vec<_>>();

    if let Some(application_id) = ttn_ids.application_id.as_deref() {
        let exact = enabled_matches
            .iter()
            .filter(|mapping| mapping.ttn_application_id.as_deref() == Some(application_id))
            .cloned()
            .collect::<Vec<_>>();
        if exact.len() > 1 {
            return Ok(TtnDeviceMappingResolution::Ambiguous(format!(
                "ambiguous_exact_application_mapping: multiple enabled TTN mappings found for connector {connector_id}, device '{}', application '{}'",
                ttn_ids.device_id, application_id
            )));
        }
        if let Some(mapping) = exact.into_iter().next() {
            return Ok(TtnDeviceMappingResolution::Resolved {
                mapping,
                resolution: "exact_application_match",
            });
        }
    }

    let fallback = enabled_matches
        .into_iter()
        .filter(|mapping| mapping.ttn_application_id.is_none())
        .collect::<Vec<_>>();
    if fallback.len() > 1 {
        return Ok(TtnDeviceMappingResolution::Ambiguous(format!(
            "ambiguous_fallback_device_mapping: multiple enabled fallback TTN mappings found for connector {connector_id}, device '{}'",
            ttn_ids.device_id
        )));
    }
    if let Some(mapping) = fallback.into_iter().next() {
        return Ok(TtnDeviceMappingResolution::Resolved {
            mapping,
            resolution: "fallback_device_match",
        });
    }

    Ok(TtnDeviceMappingResolution::Missing)
}

fn fail_ttn_device_mapping_resolution(
    state: &AppState,
    connector: &IngestionConnector,
    payload_format: &str,
    protocol: &str,
    content_type: Option<String>,
    payload: &Value,
    ttn_ids: &TtnUplinkIds,
    mapping_resolution_error: &str,
    event_type: &'static str,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    let received_at = Utc::now();
    let source_ref = connector
        .http_path
        .clone()
        .or_else(|| connector.endpoint.clone())
        .or_else(|| connector.topic_filter.clone())
        .unwrap_or_else(|| "/ingestion/connectors/{connector_id}/ingest".to_string());
    let connector_metadata = Some(connector_event_metadata(connector));
    let mut headers = json!({
        "protocol": protocol,
        "payload_format": payload_format,
        "source_endpoint": connector.endpoint.clone().unwrap_or_else(|| source_ref.clone()),
        "topic_or_path": source_ref,
        "decoder_metadata": {
            "decoder": payload_format,
            "mapping_source": "ttn_device_mapping",
            "reason": mapping_resolution_error
        },
        "ttn_device_id": ttn_ids.device_id,
        "ttn_application_id": ttn_ids.application_id
    });
    if let Some(object) = headers.as_object_mut() {
        let metadata = connector_metadata.clone().unwrap_or_else(|| json!({}));
        object.insert("connector".to_string(), metadata.clone());
        object.insert("connector_id".to_string(), metadata["connector_id"].clone());
        object.insert(
            "connector_key".to_string(),
            metadata["connector_key"].clone(),
        );
        object.insert(
            "connector_profile".to_string(),
            metadata["connector_profile"].clone(),
        );
    }

    let mut raw_message = RawMessage::new(
        state.tenant_id,
        RawMessageSource::Http,
        Some(source_ref),
        Some(ttn_ids.device_id.clone()),
        Some(payload_format.to_string()),
        content_type,
        None,
        None,
        Some(payload_format.to_string()),
        headers,
        payload_to_bytes(payload),
        received_at,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    raw_message = state.storage.store_raw_message(raw_message)?;

    let message = format!(
        "TTN device mapping resolution failed for connector {} and device {}: {}",
        connector.id, ttn_ids.device_id, mapping_resolution_error
    );
    state
        .storage
        .mark_raw_message_failed(state.tenant_id, raw_message.id, &message)?;
    let event_metadata = metadata_with_connector(
        json!({
            "payload_format": payload_format,
            "reason": mapping_resolution_error,
            "ttn_device_id": ttn_ids.device_id,
            "ttn_application_id": ttn_ids.application_id,
            "connector_id": connector.id,
            "mapping_resolution_error": mapping_resolution_error
        }),
        connector_metadata,
    );
    record_ingest_event_optional(
        state,
        event_type,
        EventSeverity::Error,
        None,
        None,
        Some(raw_message.id),
        Some(message.clone()),
        event_metadata.clone(),
    )?;
    record_ingest_event_optional(
        state,
        "aion:PayloadIngestionFailed",
        EventSeverity::Error,
        None,
        None,
        Some(raw_message.id),
        Some(message.clone()),
        event_metadata,
    )?;

    Err(ApiError::bad_request(message))
}

fn payload_to_bytes(payload: &Value) -> Vec<u8> {
    payload
        .as_str()
        .map(|value| value.as_bytes().to_vec())
        .unwrap_or_else(|| payload.to_string().into_bytes())
}

fn raw_message_response(raw_message: RawMessage) -> RawMessageResponse {
    let protocol = raw_message_string_header(&raw_message, "protocol");
    let payload_format = raw_message_string_header(&raw_message, "payload_format")
        .or(raw_message.decoder_hint.clone());
    let producer_entity_id = raw_message_uuid_header(&raw_message, "producer_entity_id");
    let feature_of_interest_id = raw_message_uuid_header(&raw_message, "feature_of_interest_id");
    let connector_id = raw_message_uuid_header(&raw_message, "connector_id");
    let connector_key = raw_message_string_header(&raw_message, "connector_key");
    let connector_profile = raw_message_string_header(&raw_message, "connector_profile");
    let source_endpoint = raw_message_string_header(&raw_message, "source_endpoint");
    let topic_or_path = raw_message_string_header(&raw_message, "topic_or_path")
        .or_else(|| raw_message.source_ref.clone());
    let decoder_metadata = raw_message
        .headers
        .get("decoder_metadata")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let payload = raw_payload_value(&raw_message.payload);

    RawMessageResponse {
        id: raw_message.id,
        raw_message_id: raw_message.id,
        source_type: raw_message.source_type,
        protocol,
        content_type: raw_message.content_type,
        payload_format,
        connector_id,
        connector_key,
        connector_profile,
        source_endpoint,
        topic_or_path,
        producer_entity_id,
        feature_of_interest_id,
        received_at: raw_message.received_at,
        normalization_status: raw_message.normalization_status,
        normalization_error: raw_message.normalization_error,
        decoder_metadata,
        payload,
    }
}

fn raw_message_string_header(raw_message: &RawMessage, key: &str) -> Option<String> {
    raw_message
        .headers
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn raw_message_uuid_header(raw_message: &RawMessage, key: &str) -> Option<Uuid> {
    raw_message
        .headers
        .get(key)
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
}

fn raw_payload_value(payload: &[u8]) -> Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        String::from_utf8(payload.to_vec())
            .map(Value::String)
            .unwrap_or_else(|_| json!({"encoding": "binary", "byte_length": payload.len()}))
    })
}

fn ensure_entity_exists(state: &AppState, entity_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn ensure_connector_secret_exists(
    state: &AppState,
    secret_id: Option<Uuid>,
) -> Result<(), ApiError> {
    let Some(secret_id) = secret_id else {
        return Ok(());
    };
    state
        .storage
        .get_connector_secret(state.tenant_id, secret_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn ensure_ttn_connector(connector: &IngestionConnector) -> Result<(), ApiError> {
    if connector.connector_profile != ConnectorProfile::TtnV3 {
        return Err(ApiError::bad_request(
            "TTN device mappings require connector_profile ttn-v3",
        ));
    }
    Ok(())
}

fn get_connector(state: &AppState, connector_id: Uuid) -> Result<IngestionConnector, ApiError> {
    state
        .storage
        .get_ingestion_connector(state.tenant_id, connector_id)?
        .ok_or_else(ApiError::not_found)
}

fn connector_status(
    state: &AppState,
    connector: &IngestionConnector,
) -> IngestionConnectorStatusResponse {
    if let Some(worker) = state
        .connector_worker_statuses
        .read()
        .ok()
        .and_then(|statuses| statuses.get(&connector.id).cloned())
    {
        return IngestionConnectorStatusResponse {
            connector_id: connector.id,
            connector_key: connector.connector_key.clone(),
            connector_type: connector.connector_type.clone(),
            connector_profile: connector.connector_profile.clone(),
            enabled: connector.enabled,
            status: if !connector.enabled {
                "disabled"
            } else {
                connector_runtime_state_label(&worker.status)
            },
            last_error: worker.last_error,
            last_message_at: worker.last_message_at,
            last_successful_ingest_at: worker.last_successful_ingest_at,
            last_failed_ingest_at: worker.last_failed_ingest_at,
        };
    }

    let (status, last_error) = if !connector.enabled {
        ("disabled", None)
    } else {
        match connector.connector_type {
            IngestionConnectorType::Http => ("ready", None),
            IngestionConnectorType::Mqtt => (
                "planned",
                Some("dynamic connector workers are disabled unless AIONCORE_CONNECTOR_WORKERS_ENABLED=true".to_string()),
            ),
            IngestionConnectorType::Future => (
                "unsupported",
                Some("future connector runtime is not implemented yet".to_string()),
            ),
        }
    };

    IngestionConnectorStatusResponse {
        connector_id: connector.id,
        connector_key: connector.connector_key.clone(),
        connector_type: connector.connector_type.clone(),
        connector_profile: connector.connector_profile.clone(),
        enabled: connector.enabled,
        status,
        last_error,
        last_message_at: None,
        last_successful_ingest_at: None,
        last_failed_ingest_at: None,
    }
}

fn connector_runtime_state_label(status: &ConnectorWorkerRuntimeState) -> &'static str {
    match status {
        ConnectorWorkerRuntimeState::Planned => "planned",
        ConnectorWorkerRuntimeState::Starting => "starting",
        ConnectorWorkerRuntimeState::Running => "ready",
        ConnectorWorkerRuntimeState::Reconnecting => "reconnecting",
        ConnectorWorkerRuntimeState::Degraded => "degraded",
        ConnectorWorkerRuntimeState::Stopped => "stopped",
        ConnectorWorkerRuntimeState::Skipped => "skipped",
        ConnectorWorkerRuntimeState::Invalid => "error",
        ConnectorWorkerRuntimeState::Error => "error",
        ConnectorWorkerRuntimeState::Unsupported => "unsupported",
    }
}

fn connector_validation(
    state: &AppState,
    connector: &IngestionConnector,
) -> Result<TtnConnectorValidation, ApiError> {
    let mut issues = Vec::new();
    let mut warnings = Vec::new();
    let mappings = if connector.connector_profile == ConnectorProfile::TtnV3 {
        state
            .storage
            .list_ttn_device_mappings(state.tenant_id, connector.id)?
    } else {
        Vec::new()
    };
    let mapping_count = mappings.len();
    let enabled_mapping_count = mappings.iter().filter(|mapping| mapping.enabled).count();
    let payload_format_supported = connector
        .payload_format
        .as_deref()
        .map(is_ttn_uplink_payload_format)
        .unwrap_or(false);
    let mut secret_configured = false;
    let mut secret_type = None;
    let mut operator_hints = Vec::new();

    if connector.connector_profile != ConnectorProfile::TtnV3 {
        warnings.push(ttn_validation_issue(
            "profile_specific_validation_unavailable",
            "profile-specific validation is currently available only for ttn-v3 connectors",
        ));
    } else {
        operator_hints.extend(ttn_operator_hints());
        if connector.connector_type != IngestionConnectorType::Mqtt {
            issues.push(ttn_validation_issue(
                "invalid_connector_type",
                "TTN v3 connectors must use connector_type mqtt",
            ));
        }
        if connector
            .broker_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            issues.push(ttn_validation_issue(
                "missing_broker_url",
                "TTN v3 connectors should include broker_url before live MQTT operation",
            ));
        }
        match connector.topic_filter.as_deref().map(str::trim) {
            Some(topic_filter) if !topic_filter.is_empty() => {
                if !is_plausible_ttn_topic_filter(topic_filter) {
                    issues.push(ttn_validation_issue(
                        "implausible_ttn_topic_filter",
                        "TTN v3 topic_filter should look like v3/{application_id}/devices/+/up",
                    ));
                }
            }
            _ => issues.push(ttn_validation_issue(
                "missing_topic_filter",
                "TTN v3 connectors should include topic_filter before live MQTT operation",
            )),
        }
        if !payload_format_supported {
            issues.push(ttn_validation_issue(
                "unsupported_ttn_payload_format",
                "TTN v3 connectors require payload_format ttn-uplink-json",
            ));
        }
        if mapping_count == 0 {
            warnings.push(ttn_validation_issue(
                "missing_ttn_device_mappings",
                "TTN v3 connector has no device mappings; unmapped uplinks without explicit entity IDs will fail safely",
            ));
        } else if enabled_mapping_count == 0 {
            warnings.push(ttn_validation_issue(
                "no_enabled_ttn_device_mappings",
                "TTN v3 connector has mappings, but none are enabled",
            ));
        }
        if connector
            .broker_url
            .as_deref()
            .map(is_public_ttn_broker_url)
            .unwrap_or(false)
            && connector.secret_ref_id.is_none()
        {
            warnings.push(ttn_validation_issue(
                "missing_secret_ref",
                "public TTN/The Things Stack brokers usually require MQTT authentication; configure secret_ref_id before live operation",
            ));
        }
        if let Some(secret_ref_id) = connector.secret_ref_id {
            match state
                .storage
                .get_connector_secret(state.tenant_id, secret_ref_id)?
            {
                Some(secret) => {
                    secret_type = Some(secret.secret_type.clone());
                    let has_secret_value = !secret.secret_value.is_empty();
                    if secret.secret_type != ConnectorSecretType::MqttBasicAuth {
                        issues.push(ttn_validation_issue(
                            "incompatible_secret_type",
                            "TTN v3 MQTT authentication currently expects a mqtt_basic_auth connector secret",
                        ));
                    }
                    if secret.secret_type == ConnectorSecretType::MqttBasicAuth
                        && secret
                            .username
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .is_none()
                    {
                        issues.push(ttn_validation_issue(
                            "missing_secret_username",
                            "TTN v3 mqtt_basic_auth secrets should include the MQTT username/application identifier",
                        ));
                    }
                    if !has_secret_value {
                        issues.push(ttn_validation_issue(
                            "missing_secret_value",
                            "referenced connector secret does not contain an internal secret value",
                        ));
                    }
                    secret_configured = secret.secret_type == ConnectorSecretType::MqttBasicAuth
                        && secret
                            .username
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .is_some()
                        && has_secret_value;
                }
                None => issues.push(ttn_validation_issue(
                    "secret_ref_not_found",
                    "connector secret_ref_id does not reference an existing connector secret",
                )),
            }
        }
        if !connector.enabled {
            warnings.push(ttn_validation_issue(
                "connector_disabled",
                "connector is disabled; configuration can be valid, but runtime readiness is degraded until enabled",
            ));
        }
    }

    let readiness = if !issues.is_empty() {
        TtnConnectorReadiness::Invalid
    } else if connector.connector_profile == ConnectorProfile::TtnV3
        && connector.enabled
        && enabled_mapping_count > 0
        && warnings.is_empty()
    {
        TtnConnectorReadiness::Ready
    } else {
        TtnConnectorReadiness::Degraded
    };

    Ok(TtnConnectorValidation {
        connector_id: connector.id,
        connector_key: connector.connector_key.clone(),
        valid: issues.is_empty(),
        readiness,
        issues,
        warnings,
        detected_profile: connector.connector_profile.clone(),
        expected_topic_shape: "v3/{application_id}/devices/{device_id}/up",
        mapping_count,
        enabled_mapping_count,
        has_secret_ref: connector.secret_ref_id.is_some(),
        secret_configured,
        secret_type,
        payload_format_supported,
        operator_hints,
        generated_at: Utc::now(),
    })
}

fn ttn_validation_issue(
    code: impl Into<String>,
    message: impl Into<String>,
) -> TtnConnectorValidationIssue {
    TtnConnectorValidationIssue {
        code: code.into(),
        message: message.into(),
    }
}

fn ttn_operator_hints() -> Vec<String> {
    vec![
        "Public TTN/The Things Stack MQTT brokers typically require authentication.".to_string(),
        "The MQTT username is usually application-specific and may include tenant or deployment context.".to_string(),
        "Store the MQTT password or API token as a connector secret; validation never returns secret_value.".to_string(),
        "Use a topic_filter shaped like v3/{application_id}/devices/{device_id}/up or v3/{application_id}/devices/+/up.".to_string(),
        "No live credential or broker verification is performed by this validation endpoint.".to_string(),
    ]
}

fn ttn_live_readiness_plan(
    state: &AppState,
    connector: &IngestionConnector,
) -> Result<TtnLiveReadinessPlan, ApiError> {
    let validation = connector_validation(state, connector)?;
    let mut checks = Vec::new();
    let mut blockers = Vec::new();
    let mut warnings = Vec::new();
    let mut required_operator_steps = Vec::new();

    if connector.connector_profile != ConnectorProfile::TtnV3 {
        checks.push(ttn_live_check(
            "connector_profile_is_ttn_v3",
            "Connector profile is TTN v3",
            TtnLiveReadinessCheckStatus::Skipped,
            Some("profile-specific TTN live readiness planning is not applicable".to_string()),
            false,
        ));
        warnings.push(ttn_validation_issue(
            "not_applicable",
            "TTN live readiness planning applies only to ttn-v3 connectors",
        ));
        return Ok(TtnLiveReadinessPlan {
            connector_id: connector.id,
            connector_key: connector.connector_key.clone(),
            dry_run: true,
            can_attempt_live_validation: false,
            readiness: TtnConnectorReadiness::Degraded,
            checks,
            blockers,
            warnings,
            required_operator_steps,
            safe_to_connect: false,
            generated_at: Utc::now(),
        });
    }

    let profile_ok = connector.connector_profile == ConnectorProfile::TtnV3;
    checks.push(ttn_live_check_from_bool(
        "connector_profile_is_ttn_v3",
        "Connector profile is TTN v3",
        profile_ok,
        "connector_profile must be ttn-v3",
        false,
    ));
    if !profile_ok {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "connector_profile_not_ttn_v3",
            "connector profile must be ttn-v3",
            "Create or select a connector with connector_profile = ttn-v3.",
        );
    }

    let connector_type_ok = connector.connector_type == IngestionConnectorType::Mqtt;
    checks.push(ttn_live_check_from_bool(
        "connector_type_is_mqtt",
        "Connector type is MQTT",
        connector_type_ok,
        "connector_type must be mqtt",
        false,
    ));
    if !connector_type_ok {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "connector_type_not_mqtt",
            "connector_type must be mqtt",
            "Create or update the TTN connector so connector_type = mqtt.",
        );
    }

    let broker_url_present = connector
        .broker_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    checks.push(ttn_live_check_from_bool(
        "broker_url_present",
        "Broker URL is configured",
        broker_url_present,
        "broker_url is missing",
        true,
    ));
    if !broker_url_present {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_broker_url",
            "broker_url is required before any future live TTN validation attempt",
            "Set broker_url to the TTN/The Things Stack MQTT endpoint for the deployment.",
        );
    }

    let topic_filter_present = connector
        .topic_filter
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    checks.push(ttn_live_check_from_bool(
        "topic_filter_present",
        "Topic filter is configured",
        topic_filter_present,
        "topic_filter is missing",
        true,
    ));
    if !topic_filter_present {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_topic_filter",
            "topic_filter is required before any future live TTN validation attempt",
            "Set topic_filter to a TTN uplink topic such as v3/{application_id}/devices/+/up.",
        );
    }

    let topic_filter_plausibly_ttn = connector
        .topic_filter
        .as_deref()
        .map(is_plausible_ttn_topic_filter)
        .unwrap_or(false);
    checks.push(ttn_live_check(
        "topic_filter_plausibly_ttn",
        "Topic filter looks like a TTN uplink topic",
        if topic_filter_present {
            if topic_filter_plausibly_ttn {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if topic_filter_present && !topic_filter_plausibly_ttn {
            Some("topic_filter should contain v3/, /devices/, and /up".to_string())
        } else if !topic_filter_present {
            Some("topic_filter is missing".to_string())
        } else {
            None
        },
        true,
    ));
    if topic_filter_present && !topic_filter_plausibly_ttn {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "implausible_ttn_topic_filter",
            "topic_filter does not look like a TTN uplink topic",
            "Set topic_filter to match the application/device uplink topic shape.",
        );
    }

    checks.push(ttn_live_check_from_bool(
        "payload_format_is_ttn_uplink_json",
        "Payload format is ttn-uplink-json",
        validation.payload_format_supported,
        "payload_format must be ttn-uplink-json",
        false,
    ));
    if !validation.payload_format_supported {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "unsupported_ttn_payload_format",
            "payload_format must be ttn-uplink-json",
            "Set payload_format = ttn-uplink-json.",
        );
    }

    checks.push(ttn_live_check_from_bool(
        "secret_ref_present",
        "Connector references a connector secret",
        validation.has_secret_ref,
        "secret_ref_id is missing",
        false,
    ));
    if !validation.has_secret_ref {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_secret_ref",
            "secret_ref_id is required before any future live TTN validation attempt",
            "Create a mqtt_basic_auth connector secret and attach it with secret_ref_id.",
        );
    }

    let secret_ref_resolves =
        validation.has_secret_ref && !ttn_validation_has_issue(&validation, "secret_ref_not_found");
    checks.push(ttn_live_check(
        "secret_ref_resolves",
        "Connector secret reference resolves",
        if validation.has_secret_ref {
            if secret_ref_resolves {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if validation.has_secret_ref && !secret_ref_resolves {
            Some("secret_ref_id does not reference an existing connector secret".to_string())
        } else if !validation.has_secret_ref {
            Some("secret_ref_id is missing".to_string())
        } else {
            None
        },
        false,
    ));
    if validation.has_secret_ref && !secret_ref_resolves {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "secret_ref_not_found",
            "secret_ref_id does not reference an existing connector secret",
            "Attach secret_ref_id to an existing connector secret.",
        );
    }

    let secret_type_is_mqtt_basic_auth =
        validation.secret_type == Some(ConnectorSecretType::MqttBasicAuth);
    checks.push(ttn_live_check(
        "secret_type_is_mqtt_basic_auth",
        "Connector secret type is mqtt_basic_auth",
        if secret_ref_resolves {
            if secret_type_is_mqtt_basic_auth {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if secret_ref_resolves && !secret_type_is_mqtt_basic_auth {
            Some("referenced secret must use secret_type mqtt_basic_auth".to_string())
        } else if !secret_ref_resolves {
            Some("secret reference does not resolve".to_string())
        } else {
            None
        },
        false,
    ));
    if secret_ref_resolves && !secret_type_is_mqtt_basic_auth {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "incompatible_secret_type",
            "referenced secret must use secret_type mqtt_basic_auth",
            "Create or attach a connector secret with secret_type = mqtt_basic_auth.",
        );
    }

    let secret_username_present = secret_type_is_mqtt_basic_auth
        && !ttn_validation_has_issue(&validation, "missing_secret_username");
    checks.push(ttn_live_check(
        "secret_username_present",
        "Connector secret has a username",
        if secret_type_is_mqtt_basic_auth {
            if secret_username_present {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if secret_type_is_mqtt_basic_auth && !secret_username_present {
            Some("mqtt_basic_auth secret is missing username".to_string())
        } else if !secret_type_is_mqtt_basic_auth {
            Some("secret type is not mqtt_basic_auth".to_string())
        } else {
            None
        },
        false,
    ));
    if secret_type_is_mqtt_basic_auth && !secret_username_present {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_secret_username",
            "mqtt_basic_auth secret is missing username",
            "Set the connector secret username to the TTN MQTT username for the deployment.",
        );
    }

    let secret_value_present = validation.secret_configured
        && !ttn_validation_has_issue(&validation, "missing_secret_value");
    checks.push(ttn_live_check(
        "secret_value_present_internally",
        "Connector secret has an internal secret value",
        if secret_type_is_mqtt_basic_auth {
            if secret_value_present {
                TtnLiveReadinessCheckStatus::Pass
            } else {
                TtnLiveReadinessCheckStatus::Fail
            }
        } else {
            TtnLiveReadinessCheckStatus::Skipped
        },
        if secret_type_is_mqtt_basic_auth && !secret_value_present {
            Some("secret value is missing internally".to_string())
        } else if !secret_type_is_mqtt_basic_auth {
            Some("secret type is not mqtt_basic_auth".to_string())
        } else {
            None
        },
        false,
    ));
    if secret_type_is_mqtt_basic_auth && !secret_value_present {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_secret_value",
            "connector secret has no internal secret value",
            "Store the TTN MQTT password or API token in the connector secret_value.",
        );
    }

    let has_enabled_mapping = validation.enabled_mapping_count > 0;
    checks.push(ttn_live_check_from_bool(
        "at_least_one_enabled_ttn_mapping",
        "At least one enabled TTN device mapping exists",
        has_enabled_mapping,
        "no enabled TTN device mapping exists",
        false,
    ));
    if !has_enabled_mapping {
        add_ttn_live_blocker(
            &mut blockers,
            &mut required_operator_steps,
            "missing_enabled_ttn_device_mapping",
            "at least one enabled TTN device mapping is required before live validation",
            "Create and enable a TTN device mapping for the connector.",
        );
    }

    checks.push(ttn_live_check(
        "no_network_call_performed",
        "Dry-run plan did not contact TTN or any broker",
        TtnLiveReadinessCheckStatus::Pass,
        Some("this endpoint is deterministic and non-network".to_string()),
        false,
    ));

    if !connector.enabled {
        warnings.push(ttn_validation_issue(
            "connector_disabled",
            "connector is disabled; enable it before any future live validation attempt",
        ));
        push_unique_step(
            &mut required_operator_steps,
            "Enable the TTN connector before attempting live validation.",
        );
    }

    for issue in &validation.issues {
        push_unique_issue(&mut blockers, issue.clone());
    }
    for warning in &validation.warnings {
        push_unique_issue(&mut warnings, warning.clone());
    }

    let safe_to_connect = connector.enabled && blockers.is_empty();
    let readiness = if safe_to_connect {
        TtnConnectorReadiness::Ready
    } else if blockers.is_empty() {
        TtnConnectorReadiness::Degraded
    } else {
        TtnConnectorReadiness::Invalid
    };

    Ok(TtnLiveReadinessPlan {
        connector_id: connector.id,
        connector_key: connector.connector_key.clone(),
        dry_run: true,
        can_attempt_live_validation: safe_to_connect,
        readiness,
        checks,
        blockers,
        warnings,
        required_operator_steps,
        safe_to_connect,
        generated_at: Utc::now(),
    })
}

async fn ttn_live_validation_preflight(
    state: &AppState,
    connector: &IngestionConnector,
    request: TtnLiveValidationRequest,
) -> Result<TtnLiveValidationResponse, ApiError> {
    let started_at = Utc::now();
    let timer = Instant::now();
    let requested_timeout_seconds = request.timeout_seconds.unwrap_or(5);
    let timeout_seconds = requested_timeout_seconds.clamp(1, 60);
    let expect_message = request.expect_message.unwrap_or(false);
    let dry_run_only = request.dry_run_only.unwrap_or(false);
    let plan = ttn_live_readiness_plan(state, connector)?;
    let plan_summary = ttn_live_validation_plan_summary(&plan);
    let mut response_warnings = plan.warnings.clone();
    if requested_timeout_seconds > 60 {
        response_warnings.push(ttn_validation_issue(
            "timeout_seconds_capped",
            "timeout_seconds was capped at 60 seconds",
        ));
    }
    let broker_url_redacted_or_safe = connector
        .broker_url
        .as_deref()
        .map(redact_broker_url_for_response);
    let topic_filter = connector.topic_filter.clone();

    if dry_run_only {
        let response = ttn_live_validation_response(
            connector,
            false,
            plan.safe_to_connect,
            false,
            false,
            false,
            broker_url_redacted_or_safe,
            topic_filter,
            timer.elapsed().as_millis(),
            started_at,
            TtnLiveValidationResultStatus::Skipped,
            Vec::new(),
            response_warnings.clone(),
            plan_summary,
        );
        record_ttn_live_validation_event(
            state,
            "aion:TtnLiveValidationSkipped",
            connector,
            &response,
            Some("TTN live validation skipped because dry_run_only=true".to_string()),
        )?;
        return Ok(response);
    }

    if !plan.safe_to_connect {
        let response = ttn_live_validation_response(
            connector,
            false,
            false,
            false,
            false,
            false,
            broker_url_redacted_or_safe,
            topic_filter,
            timer.elapsed().as_millis(),
            started_at,
            TtnLiveValidationResultStatus::Skipped,
            plan.blockers.clone(),
            response_warnings.clone(),
            plan_summary,
        );
        record_ttn_live_validation_event(
            state,
            "aion:TtnLiveValidationSkipped",
            connector,
            &response,
            Some("TTN live validation skipped because dry-run blockers remain".to_string()),
        )?;
        return Ok(response);
    }

    record_ttn_live_validation_started_event(state, connector, timeout_seconds, expect_message)?;

    let Some(secret_ref_id) = connector.secret_ref_id else {
        let response = ttn_live_validation_response(
            connector,
            false,
            true,
            false,
            false,
            false,
            broker_url_redacted_or_safe,
            topic_filter,
            timer.elapsed().as_millis(),
            started_at,
            TtnLiveValidationResultStatus::Failed,
            vec![ttn_validation_issue(
                "missing_secret_ref",
                "connector has no secret_ref_id for TTN MQTT authentication",
            )],
            response_warnings.clone(),
            plan_summary,
        );
        record_ttn_live_validation_event(
            state,
            "aion:TtnLiveValidationFailed",
            connector,
            &response,
            Some("TTN live validation failed before connection".to_string()),
        )?;
        return Ok(response);
    };

    let secret = state
        .storage
        .get_connector_secret(state.tenant_id, secret_ref_id)?
        .ok_or_else(|| ApiError::bad_request("connector secret_ref_id does not resolve"))?;
    if secret.secret_type != ConnectorSecretType::MqttBasicAuth {
        return Err(ApiError::bad_request(
            "TTN live validation requires a mqtt_basic_auth connector secret",
        ));
    }

    let broker_url = connector
        .broker_url
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("broker_url is required"))?;
    let topic_filter_value = connector
        .topic_filter
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("topic_filter is required"))?;

    let live_result = run_ttn_mqtt_live_preflight(
        connector,
        &secret,
        broker_url,
        topic_filter_value,
        timeout_seconds,
        expect_message,
        request.client_id_suffix.as_deref(),
    )
    .await;

    let duration_ms = timer.elapsed().as_millis();
    let response = match live_result {
        Ok(result) => {
            let succeeded = result.connected
                && result.subscribed
                && (!expect_message || result.message_received);
            ttn_live_validation_response(
                connector,
                true,
                true,
                result.connected,
                result.subscribed,
                result.message_received,
                broker_url_redacted_or_safe,
                topic_filter,
                duration_ms,
                started_at,
                if succeeded {
                    TtnLiveValidationResultStatus::Success
                } else {
                    TtnLiveValidationResultStatus::Failed
                },
                result.errors,
                response_warnings.clone(),
                plan_summary,
            )
        }
        Err(error) => ttn_live_validation_response(
            connector,
            true,
            true,
            false,
            false,
            false,
            broker_url_redacted_or_safe,
            topic_filter,
            duration_ms,
            started_at,
            TtnLiveValidationResultStatus::Failed,
            vec![ttn_validation_issue(
                "live_validation_failed",
                sanitize_live_validation_error(error, &secret.secret_value),
            )],
            response_warnings.clone(),
            plan_summary,
        ),
    };

    let event_type = if response.result == TtnLiveValidationResultStatus::Success {
        "aion:TtnLiveValidationSucceeded"
    } else {
        "aion:TtnLiveValidationFailed"
    };
    record_ttn_live_validation_event(
        state,
        event_type,
        connector,
        &response,
        Some("TTN live validation preflight completed".to_string()),
    )?;

    Ok(response)
}

struct TtnMqttLivePreflightResult {
    connected: bool,
    subscribed: bool,
    message_received: bool,
    errors: Vec<TtnConnectorValidationIssue>,
}

async fn run_ttn_mqtt_live_preflight(
    connector: &IngestionConnector,
    secret: &ConnectorSecret,
    broker_url: &str,
    topic_filter: &str,
    timeout_seconds: u64,
    expect_message: bool,
    client_id_suffix: Option<&str>,
) -> Result<TtnMqttLivePreflightResult, String> {
    let (host, port) = parse_mqtt_broker_url_for_live_preflight(broker_url)?;
    let client_id = ttn_live_validation_client_id(connector, client_id_suffix);
    let mut options = rumqttc::MqttOptions::new(client_id, host, port);
    options.set_keep_alive(std::time::Duration::from_secs(timeout_seconds.max(5)));
    if let Some(username) = secret.username.as_deref() {
        options.set_credentials(username, &secret.secret_value);
    }

    let (client, mut eventloop) = rumqttc::AsyncClient::new(options, 10);
    client
        .subscribe(topic_filter, rumqttc::QoS::AtLeastOnce)
        .await
        .map_err(|err| format!("failed to send MQTT subscribe request: {err}"))?;

    let deadline = std::time::Duration::from_secs(timeout_seconds);
    let mut connected = false;
    let mut subscribed = false;
    let mut message_received = false;
    let mut errors = Vec::new();

    let poll_result = time::timeout(deadline, async {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                    connected = true;
                }
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::SubAck(_))) => {
                    subscribed = true;
                    if !expect_message {
                        break;
                    }
                }
                Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(_publish))) => {
                    message_received = true;
                    break;
                }
                Ok(_) => {}
                Err(err) => {
                    errors.push(ttn_validation_issue(
                        "mqtt_event_loop_error",
                        format!("MQTT event loop failed: {err}"),
                    ));
                    break;
                }
            }
        }
    })
    .await;

    if poll_result.is_err() {
        let code = if expect_message {
            "message_wait_timeout"
        } else {
            "subscribe_timeout"
        };
        let message = if expect_message {
            format!(
                "MQTT connection/subscription did not receive a matching message within {timeout_seconds} seconds"
            )
        } else {
            format!(
                "MQTT connection/subscription did not complete within {timeout_seconds} seconds"
            )
        };
        errors.push(ttn_validation_issue(code, message));
    }

    let _ = client.disconnect().await;

    Ok(TtnMqttLivePreflightResult {
        connected,
        subscribed,
        message_received,
        errors,
    })
}

fn parse_mqtt_broker_url_for_live_preflight(value: &str) -> Result<(String, u16), String> {
    let trimmed = value.trim();
    let without_scheme = trimmed
        .strip_prefix("mqtt://")
        .ok_or_else(|| "unsupported MQTT broker URL; expected mqtt://host:port".to_string())?;
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    let host_port = host_port.split('@').next_back().unwrap_or(host_port);
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .map_err(|err| format!("invalid MQTT broker port: {err}"))?;
            (host.to_string(), port)
        }
        None => (host_port.to_string(), 1883),
    };

    if host.trim().is_empty() {
        return Err("invalid MQTT broker URL: host is empty".to_string());
    }

    Ok((host, port))
}

fn ttn_live_validation_client_id(
    connector: &IngestionConnector,
    client_id_suffix: Option<&str>,
) -> String {
    let base = connector
        .client_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("aioncore-ttn-live-{}", connector.connector_key));
    match client_id_suffix
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(suffix) => format!("{base}-{suffix}"),
        None => base,
    }
}

fn ttn_live_validation_response(
    connector: &IngestionConnector,
    attempted_live_connection: bool,
    dry_run_passed: bool,
    connected: bool,
    subscribed: bool,
    message_received: bool,
    broker_url_redacted_or_safe: Option<String>,
    topic_filter: Option<String>,
    duration_ms: u128,
    started_at: DateTime<Utc>,
    result: TtnLiveValidationResultStatus,
    errors: Vec<TtnConnectorValidationIssue>,
    warnings: Vec<TtnConnectorValidationIssue>,
    dry_run_plan_summary: TtnLiveValidationPlanSummary,
) -> TtnLiveValidationResponse {
    TtnLiveValidationResponse {
        connector_id: connector.id,
        connector_key: connector.connector_key.clone(),
        attempted_live_connection,
        dry_run_passed,
        connected,
        subscribed,
        message_received,
        broker_url_redacted_or_safe,
        topic_filter,
        duration_ms,
        started_at,
        finished_at: Utc::now(),
        result,
        errors,
        warnings,
        dry_run_plan_summary,
        secret_exposed: false,
    }
}

fn ttn_live_validation_plan_summary(plan: &TtnLiveReadinessPlan) -> TtnLiveValidationPlanSummary {
    TtnLiveValidationPlanSummary {
        safe_to_connect: plan.safe_to_connect,
        can_attempt_live_validation: plan.can_attempt_live_validation,
        readiness: plan.readiness.clone(),
        blocker_count: plan.blockers.len(),
        warning_count: plan.warnings.len(),
    }
}

fn redact_broker_url_for_response(value: &str) -> String {
    let trimmed = value.trim();
    let Some((scheme, rest)) = trimmed.split_once("://") else {
        return trimmed.to_string();
    };
    if let Some((_, after_userinfo)) = rest.rsplit_once('@') {
        format!("{scheme}://***REDACTED***@{after_userinfo}")
    } else {
        trimmed.to_string()
    }
}

fn sanitize_live_validation_error(error: String, secret_value: &str) -> String {
    if secret_value.is_empty() {
        return error;
    }
    error.replace(secret_value, "***REDACTED***")
}

fn record_ttn_live_validation_started_event(
    state: &AppState,
    connector: &IngestionConnector,
    timeout_seconds: u64,
    expect_message: bool,
) -> Result<Event, ApiError> {
    record_connector_worker_event(
        state,
        "aion:TtnLiveValidationStarted",
        EventSeverity::Info,
        Some("TTN live validation preflight started".to_string()),
        json!({
            "connector_id": connector.id,
            "connector_key": connector.connector_key,
            "connector_profile": connector.connector_profile,
            "broker_url": connector.broker_url.as_deref().map(redact_broker_url_for_response),
            "topic_filter": connector.topic_filter,
            "timeout_seconds": timeout_seconds,
            "expect_message": expect_message,
            "secret_exposed": false
        }),
    )
}

fn record_ttn_live_validation_event(
    state: &AppState,
    event_type: impl Into<String>,
    connector: &IngestionConnector,
    response: &TtnLiveValidationResponse,
    message: Option<String>,
) -> Result<Event, ApiError> {
    record_connector_worker_event(
        state,
        event_type,
        match response.result {
            TtnLiveValidationResultStatus::Success | TtnLiveValidationResultStatus::Skipped => {
                EventSeverity::Info
            }
            TtnLiveValidationResultStatus::Failed => EventSeverity::Error,
        },
        message,
        json!({
            "connector_id": connector.id,
            "connector_key": connector.connector_key,
            "connector_profile": connector.connector_profile,
            "attempted_live_connection": response.attempted_live_connection,
            "dry_run_passed": response.dry_run_passed,
            "connected": response.connected,
            "subscribed": response.subscribed,
            "message_received": response.message_received,
            "broker_url": response.broker_url_redacted_or_safe,
            "topic_filter": response.topic_filter,
            "duration_ms": response.duration_ms,
            "result": response.result,
            "error_codes": response.errors.iter().map(|issue| issue.code.clone()).collect::<Vec<_>>(),
            "warning_codes": response.warnings.iter().map(|issue| issue.code.clone()).collect::<Vec<_>>(),
            "secret_exposed": false
        }),
    )
}

fn ttn_validation_has_issue(validation: &TtnConnectorValidation, code: &str) -> bool {
    validation.issues.iter().any(|issue| issue.code == code)
}

fn ttn_live_check_from_bool(
    check_key: &'static str,
    description: &'static str,
    passed: bool,
    failure_reason: &'static str,
    future_live_check: bool,
) -> TtnLiveReadinessCheck {
    ttn_live_check(
        check_key,
        description,
        if passed {
            TtnLiveReadinessCheckStatus::Pass
        } else {
            TtnLiveReadinessCheckStatus::Fail
        },
        if passed {
            None
        } else {
            Some(failure_reason.to_string())
        },
        future_live_check,
    )
}

fn ttn_live_check(
    check_key: &'static str,
    description: &'static str,
    status: TtnLiveReadinessCheckStatus,
    reason: Option<String>,
    future_live_check: bool,
) -> TtnLiveReadinessCheck {
    TtnLiveReadinessCheck {
        check_key,
        description,
        status,
        reason,
        future_live_check,
    }
}

fn add_ttn_live_blocker(
    blockers: &mut Vec<TtnConnectorValidationIssue>,
    required_operator_steps: &mut Vec<String>,
    code: &'static str,
    message: &'static str,
    step: &'static str,
) {
    push_unique_issue(blockers, ttn_validation_issue(code, message));
    push_unique_step(required_operator_steps, step);
}

fn push_unique_issue(
    issues: &mut Vec<TtnConnectorValidationIssue>,
    issue: TtnConnectorValidationIssue,
) {
    if !issues.iter().any(|existing| existing.code == issue.code) {
        issues.push(issue);
    }
}

fn push_unique_step(steps: &mut Vec<String>, step: &'static str) {
    if !steps.iter().any(|existing| existing == step) {
        steps.push(step.to_string());
    }
}

fn is_plausible_ttn_topic_filter(topic_filter: &str) -> bool {
    let normalized = topic_filter.trim().to_ascii_lowercase();
    normalized.contains("v3/")
        && normalized.contains("/devices/")
        && (normalized.ends_with("/up") || normalized.contains("/up/"))
}

fn is_public_ttn_broker_url(broker_url: &str) -> bool {
    let normalized = broker_url.trim().to_ascii_lowercase();
    normalized.contains("thethings.network")
        || normalized.contains("thethings.industries")
        || normalized.contains("thethingsstack")
}

fn worker_plan_summary(state: &AppState) -> ReadyWorkerPlanSummary {
    build_ingestion_worker_plan(state)
        .map(|plan| ReadyWorkerPlanSummary {
            planned_workers: plan.planned_workers,
            invalid_workers: plan.invalid_workers,
            unsupported_workers: plan.unsupported_workers,
        })
        .unwrap_or(ReadyWorkerPlanSummary {
            planned_workers: 0,
            invalid_workers: 0,
            unsupported_workers: 0,
        })
}

async fn start_connector_workers(
    state: AppState,
    config: ConnectorWorkerConfig,
) -> Result<(), StartupError> {
    set_connector_workers_enabled(&state, config.enabled);
    reconcile_connector_workers(state, true)
        .await
        .map(|_| ())
        .map_err(|err| StartupError::backend_initialization(err.message))
}

fn build_ingestion_worker_plan(state: &AppState) -> Result<IngestionWorkerPlan, ApiError> {
    let specs = state
        .storage
        .list_ingestion_connectors(state.tenant_id)?
        .into_iter()
        .map(|connector| connector_worker_spec(state, connector))
        .collect::<Result<Vec<_>, _>>()?;
    let planned_workers = specs
        .iter()
        .filter(|spec| spec.status == IngestionWorkerSpecStatus::Planned)
        .count();
    let skipped_workers = specs
        .iter()
        .filter(|spec| spec.status == IngestionWorkerSpecStatus::Skipped)
        .count();
    let invalid_workers = specs
        .iter()
        .filter(|spec| spec.status == IngestionWorkerSpecStatus::Invalid)
        .count();
    let unsupported_workers = specs
        .iter()
        .filter(|spec| spec.status == IngestionWorkerSpecStatus::Unsupported)
        .count();

    Ok(IngestionWorkerPlan {
        specs,
        planned_workers,
        skipped_workers,
        invalid_workers,
        unsupported_workers,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectorWorkerStartDecision {
    StartMqtt,
    Skip,
    Invalid,
    Unsupported,
    PlannedOnly,
}

fn connector_worker_start_decision(spec: &IngestionWorkerSpec) -> ConnectorWorkerStartDecision {
    match spec.status {
        IngestionWorkerSpecStatus::Skipped => ConnectorWorkerStartDecision::Skip,
        IngestionWorkerSpecStatus::Invalid => ConnectorWorkerStartDecision::Invalid,
        IngestionWorkerSpecStatus::Unsupported => ConnectorWorkerStartDecision::Unsupported,
        IngestionWorkerSpecStatus::Planned => match (&spec.worker_kind, &spec.connector_profile) {
            (IngestionWorkerKind::MqttSubscriber, ConnectorProfile::GenericAionMqtt)
            | (IngestionWorkerKind::MqttSubscriber, ConnectorProfile::GenericMqtt)
            | (IngestionWorkerKind::MqttSubscriber, ConnectorProfile::TtnV3) => {
                ConnectorWorkerStartDecision::StartMqtt
            }
            (IngestionWorkerKind::Unsupported, _) => ConnectorWorkerStartDecision::Unsupported,
            _ => ConnectorWorkerStartDecision::PlannedOnly,
        },
    }
}

async fn reconcile_connector_workers_after_mutation(state: &AppState) {
    if let Err(err) = reconcile_connector_workers(state.clone(), true).await {
        let _ = record_connector_worker_event(
            state,
            "aion:ConnectorWorkerReconcileFailed",
            EventSeverity::Error,
            Some("Connector worker reconciliation failed".to_string()),
            json!({
                "error": err.message
            }),
        );
    }
}

async fn reconcile_connector_workers(
    state: AppState,
    start_network: bool,
) -> Result<ReconcileConnectorWorkersResponse, ApiError> {
    let plan = build_ingestion_worker_plan(&state)?;
    let workers_enabled = connector_workers_enabled(&state);
    let mut actions = Vec::new();
    let now = Utc::now();

    if !workers_enabled {
        stop_all_connector_workers(&state, now, &mut actions)?;
        apply_connector_worker_plan_statuses(&state, &plan, now, false, &mut actions)?;
        let status = connector_workers_status(&state)?;
        return Ok(ReconcileConnectorWorkersResponse {
            connector_workers: status.connector_workers,
            actions,
            workers: status.workers,
        });
    }

    for spec in &plan.specs {
        reconcile_connector_worker_spec(&state, spec, start_network, now, &mut actions).await?;
    }

    stop_workers_missing_from_plan(&state, &plan, now, &mut actions)?;

    let status = connector_workers_status(&state)?;
    Ok(ReconcileConnectorWorkersResponse {
        connector_workers: status.connector_workers,
        actions,
        workers: status.workers,
    })
}

fn apply_connector_worker_plan_statuses(
    state: &AppState,
    plan: &IngestionWorkerPlan,
    reconciled_at: DateTime<Utc>,
    emit_skip_events: bool,
    actions: &mut Vec<ConnectorWorkerReconcileAction>,
) -> Result<(), ApiError> {
    for spec in &plan.specs {
        let mut status = connector_runtime_status_from_spec(spec);
        status.last_reconciled_at = Some(reconciled_at);
        set_connector_worker_runtime_status(state, status);

        if emit_skip_events
            && connector_worker_start_decision(spec) == ConnectorWorkerStartDecision::Skip
            && spec.enabled
            && spec.connector_profile == ConnectorProfile::TtnV3
        {
            record_connector_worker_event(
                state,
                "aion:ConnectorWorkerSkipped",
                EventSeverity::Warning,
                Some(
                    "TTN v3 connector worker skipped because TTN decoding is future work"
                        .to_string(),
                ),
                connector_worker_event_metadata(spec, Some("ttn_decoding_not_implemented")),
            )?;
            actions.push(connector_worker_action(
                spec,
                "skipped",
                Some("TTN v3 decoding is not implemented yet"),
            ));
        }
    }

    Ok(())
}

async fn reconcile_connector_worker_spec(
    state: &AppState,
    spec: &IngestionWorkerSpec,
    start_network: bool,
    reconciled_at: DateTime<Utc>,
    actions: &mut Vec<ConnectorWorkerReconcileAction>,
) -> Result<(), ApiError> {
    let decision = connector_worker_start_decision(spec);
    match decision {
        ConnectorWorkerStartDecision::StartMqtt => {
            let signature = connector_worker_signature(spec);
            let existing = remove_connector_worker_handle_if_changed_or_finished(
                state,
                spec.connector_id,
                &signature,
            );

            match existing {
                ExistingConnectorWorker::Same => {
                    update_connector_worker_runtime_status(state, spec.connector_id, |worker| {
                        worker.last_reconciled_at = Some(reconciled_at);
                    });
                    actions.push(connector_worker_action(spec, "unchanged", None));
                }
                ExistingConnectorWorker::Stopped { reason } => {
                    let restart_count = connector_worker_restart_count(state, spec.connector_id);
                    start_connector_worker_from_spec(
                        state,
                        spec,
                        signature,
                        start_network,
                        reconciled_at,
                        restart_count + 1,
                    )
                    .await?;
                    let action = if reason == "config_changed" {
                        "restarted"
                    } else {
                        "started"
                    };
                    record_connector_worker_event(
                        state,
                        if action == "restarted" {
                            "aion:ConnectorWorkerRestarted"
                        } else {
                            "aion:ConnectorWorkerStarted"
                        },
                        EventSeverity::Info,
                        Some(format!("Connector worker {action}")),
                        connector_worker_event_metadata(spec, Some(reason)),
                    )?;
                    actions.push(connector_worker_action(spec, action, Some(reason)));
                }
                ExistingConnectorWorker::None => {
                    let restart_count = connector_worker_restart_count(state, spec.connector_id);
                    start_connector_worker_from_spec(
                        state,
                        spec,
                        signature,
                        start_network,
                        reconciled_at,
                        restart_count,
                    )
                    .await?;
                    record_connector_worker_event(
                        state,
                        "aion:ConnectorWorkerStarted",
                        EventSeverity::Info,
                        Some("Connector worker started".to_string()),
                        connector_worker_event_metadata(spec, None),
                    )?;
                    actions.push(connector_worker_action(spec, "started", None));
                }
            }
        }
        ConnectorWorkerStartDecision::Skip => {
            let stopped = stop_connector_worker_if_running(
                state,
                spec,
                reconciled_at,
                "connector_not_startable",
            )?;
            let mut status = connector_runtime_status_from_spec(spec);
            if stopped {
                status.status = ConnectorWorkerRuntimeState::Stopped;
                status.stopped_at = Some(reconciled_at);
            }
            status.last_reconciled_at = Some(reconciled_at);
            set_connector_worker_runtime_status(state, status);
            actions.push(connector_worker_action(spec, "skipped", None));
        }
        ConnectorWorkerStartDecision::Invalid | ConnectorWorkerStartDecision::Unsupported => {
            stop_connector_worker_if_running(state, spec, reconciled_at, "invalid_or_unsupported")?;
            let mut status = connector_runtime_status_from_spec(spec);
            status.last_reconciled_at = Some(reconciled_at);
            set_connector_worker_runtime_status(state, status);
            actions.push(connector_worker_action(
                spec,
                if decision == ConnectorWorkerStartDecision::Invalid {
                    "invalid"
                } else {
                    "unsupported"
                },
                None,
            ));
        }
        ConnectorWorkerStartDecision::PlannedOnly => {
            stop_connector_worker_if_running(state, spec, reconciled_at, "not_runtime_worker")?;
            let mut status = connector_runtime_status_from_spec(spec);
            status.last_reconciled_at = Some(reconciled_at);
            set_connector_worker_runtime_status(state, status);
            actions.push(connector_worker_action(spec, "planned", None));
        }
    }

    Ok(())
}

enum ExistingConnectorWorker {
    None,
    Same,
    Stopped { reason: &'static str },
}

fn remove_connector_worker_handle_if_changed_or_finished(
    state: &AppState,
    connector_id: Uuid,
    expected_signature: &ConnectorWorkerSignature,
) -> ExistingConnectorWorker {
    let Ok(mut handles) = state.connector_worker_handles.write() else {
        return ExistingConnectorWorker::None;
    };

    let Some(handle) = handles.get(&connector_id) else {
        return ExistingConnectorWorker::None;
    };

    if handle.task.is_finished() {
        handles.remove(&connector_id);
        return ExistingConnectorWorker::Stopped { reason: "finished" };
    }

    if &handle.signature == expected_signature {
        return ExistingConnectorWorker::Same;
    }

    if let Some(handle) = handles.remove(&connector_id) {
        handle.task.abort();
    }
    ExistingConnectorWorker::Stopped {
        reason: "config_changed",
    }
}

async fn start_connector_worker_from_spec(
    state: &AppState,
    spec: &IngestionWorkerSpec,
    signature: ConnectorWorkerSignature,
    start_network: bool,
    started_at: DateTime<Utc>,
    restart_count: u32,
) -> Result<(), ApiError> {
    let mut status = connector_runtime_status_from_spec(spec);
    status.status = if start_network {
        ConnectorWorkerRuntimeState::Starting
    } else {
        ConnectorWorkerRuntimeState::Planned
    };
    status.started_at = if start_network {
        Some(started_at)
    } else {
        None
    };
    status.restart_count = restart_count;
    status.last_reconciled_at = Some(started_at);
    status.last_error = if start_network {
        None
    } else {
        Some("network start skipped by test/dry-run mode".to_string())
    };
    set_connector_worker_runtime_status(state, status);

    if !start_network {
        return Ok(());
    }

    let connector_metadata = mqtt_ingest::MqttConnectorMetadata {
        connector_id: spec.connector_id,
        connector_key: spec.connector_key.clone(),
        connector_profile: spec.connector_profile.clone(),
    };
    let mqtt_config = if let Some(secret_ref_id) = spec.secret_ref_id {
        let secret = state
            .storage
            .get_connector_secret(state.tenant_id, secret_ref_id)?
            .ok_or_else(|| {
                ApiError::bad_request(
                    "connector secret_ref_id does not reference an existing connector secret",
                )
            })?;
        if secret.secret_type != ConnectorSecretType::MqttBasicAuth {
            return Err(ApiError::bad_request(
                "dynamic MQTT connector workers currently support only mqtt_basic_auth secrets",
            ));
        }
        mqtt_ingest::MqttIngestConfig::for_connector_with_basic_auth(
            spec.broker_url.clone().unwrap_or_default(),
            spec.client_id
                .clone()
                .unwrap_or_else(|| format!("aioncore-connector-{}", spec.connector_id)),
            spec.topic_filter.clone().unwrap_or_default(),
            spec.payload_format.clone(),
            spec.content_type.clone(),
            secret.username,
            secret.secret_value,
            connector_metadata,
        )
    } else {
        mqtt_ingest::MqttIngestConfig::for_connector(
            spec.broker_url.clone().unwrap_or_default(),
            spec.client_id
                .clone()
                .unwrap_or_else(|| format!("aioncore-connector-{}", spec.connector_id)),
            spec.topic_filter.clone().unwrap_or_default(),
            spec.payload_format.clone(),
            spec.content_type.clone(),
            connector_metadata,
        )
    };

    match mqtt_ingest::start_connector_worker(state.clone(), mqtt_config).await {
        Ok(task) => {
            if let Ok(mut handles) = state.connector_worker_handles.write() {
                handles.insert(spec.connector_id, ConnectorWorkerHandle { signature, task });
            }
            Ok(())
        }
        Err(err) => {
            let message = err.to_string();
            update_connector_worker_runtime_status(state, spec.connector_id, |worker| {
                worker.status = ConnectorWorkerRuntimeState::Error;
                worker.last_error = Some(message.clone());
                worker.last_failed_ingest_at = Some(Utc::now());
            });
            record_connector_worker_event(
                state,
                "aion:ConnectorWorkerReconcileFailed",
                EventSeverity::Error,
                Some("Connector worker failed to start".to_string()),
                metadata_with_connector(
                    json!({
                        "reason": "start_failed",
                        "error": message
                    }),
                    Some(connector_worker_event_metadata(spec, None)),
                ),
            )?;
            Ok(())
        }
    }
}

fn stop_connector_worker_if_running(
    state: &AppState,
    spec: &IngestionWorkerSpec,
    stopped_at: DateTime<Utc>,
    reason: &'static str,
) -> Result<bool, ApiError> {
    let handle = state
        .connector_worker_handles
        .write()
        .ok()
        .and_then(|mut handles| handles.remove(&spec.connector_id));

    let Some(handle) = handle else {
        return Ok(false);
    };

    handle.task.abort();
    update_connector_worker_runtime_status(state, spec.connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Stopped;
        worker.connected = false;
        worker.subscribed = false;
        worker.stopped_at = Some(stopped_at);
        worker.last_reconciled_at = Some(stopped_at);
    });
    record_connector_worker_event(
        state,
        "aion:ConnectorWorkerStopped",
        EventSeverity::Info,
        Some("Connector worker stopped".to_string()),
        connector_worker_event_metadata(spec, Some(reason)),
    )?;

    Ok(true)
}

fn stop_all_connector_workers(
    state: &AppState,
    stopped_at: DateTime<Utc>,
    actions: &mut Vec<ConnectorWorkerReconcileAction>,
) -> Result<(), ApiError> {
    let handles = state
        .connector_worker_handles
        .write()
        .map(|mut handles| handles.drain().collect::<Vec<_>>())
        .unwrap_or_default();
    for (connector_id, handle) in handles {
        handle.task.abort();
        update_connector_worker_runtime_status(state, connector_id, |worker| {
            worker.status = ConnectorWorkerRuntimeState::Stopped;
            worker.connected = false;
            worker.subscribed = false;
            worker.stopped_at = Some(stopped_at);
            worker.last_reconciled_at = Some(stopped_at);
            actions.push(ConnectorWorkerReconcileAction {
                connector_id,
                connector_key: worker.connector_key.clone(),
                action: "stopped".to_string(),
                reason: Some("connector_workers_disabled".to_string()),
            });
        });
    }
    Ok(())
}

fn stop_workers_missing_from_plan(
    state: &AppState,
    plan: &IngestionWorkerPlan,
    stopped_at: DateTime<Utc>,
    actions: &mut Vec<ConnectorWorkerReconcileAction>,
) -> Result<(), ApiError> {
    let planned_ids = plan
        .specs
        .iter()
        .map(|spec| spec.connector_id)
        .collect::<std::collections::HashSet<_>>();
    let stale_ids = state
        .connector_worker_handles
        .read()
        .map(|handles| {
            handles
                .keys()
                .copied()
                .filter(|id| !planned_ids.contains(id))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    for connector_id in stale_ids {
        let handle = state
            .connector_worker_handles
            .write()
            .ok()
            .and_then(|mut handles| handles.remove(&connector_id));
        if let Some(handle) = handle {
            handle.task.abort();
        }
        update_connector_worker_runtime_status(state, connector_id, |worker| {
            worker.status = ConnectorWorkerRuntimeState::Stopped;
            worker.connected = false;
            worker.subscribed = false;
            worker.stopped_at = Some(stopped_at);
            worker.last_reconciled_at = Some(stopped_at);
            actions.push(ConnectorWorkerReconcileAction {
                connector_id,
                connector_key: worker.connector_key.clone(),
                action: "stopped".to_string(),
                reason: Some("connector_removed_from_plan".to_string()),
            });
        });
    }

    Ok(())
}

fn connector_worker_signature(spec: &IngestionWorkerSpec) -> ConnectorWorkerSignature {
    ConnectorWorkerSignature {
        broker_url: spec.broker_url.clone(),
        client_id: spec.client_id.clone(),
        topic_filter: spec.topic_filter.clone(),
        payload_format: spec.payload_format.clone(),
        content_type: spec.content_type.clone(),
        secret_ref_id: spec.secret_ref_id,
        connector_profile: spec.connector_profile.clone(),
    }
}

fn connector_worker_restart_count(state: &AppState, connector_id: Uuid) -> u32 {
    state
        .connector_worker_statuses
        .read()
        .ok()
        .and_then(|statuses| {
            statuses
                .get(&connector_id)
                .map(|status| status.restart_count)
        })
        .unwrap_or(0)
}

fn connector_worker_event_metadata(spec: &IngestionWorkerSpec, reason: Option<&str>) -> Value {
    let mut metadata = json!({
        "connector_id": spec.connector_id,
        "connector_key": spec.connector_key,
        "connector_type": spec.connector_type,
        "connector_profile": spec.connector_profile,
        "worker_kind": spec.worker_kind,
        "broker_url": spec.broker_url,
        "topic_filter": spec.topic_filter,
        "payload_format": spec.payload_format,
        "secret_ref_id": spec.secret_ref_id,
        "secret_configured": spec.secret_ref_id.is_some()
    });
    if let (Some(object), Some(reason)) = (metadata.as_object_mut(), reason) {
        object.insert("reason".to_string(), json!(reason));
    }
    metadata
}

fn connector_worker_action(
    spec: &IngestionWorkerSpec,
    action: &str,
    reason: Option<&str>,
) -> ConnectorWorkerReconcileAction {
    ConnectorWorkerReconcileAction {
        connector_id: spec.connector_id,
        connector_key: spec.connector_key.clone(),
        action: action.to_string(),
        reason: reason.map(ToOwned::to_owned),
    }
}

fn connector_runtime_status_from_spec(spec: &IngestionWorkerSpec) -> ConnectorWorkerRuntimeStatus {
    let decision = connector_worker_start_decision(spec);
    let status = match decision {
        ConnectorWorkerStartDecision::StartMqtt => ConnectorWorkerRuntimeState::Planned,
        ConnectorWorkerStartDecision::Skip => ConnectorWorkerRuntimeState::Skipped,
        ConnectorWorkerStartDecision::Invalid => ConnectorWorkerRuntimeState::Invalid,
        ConnectorWorkerStartDecision::Unsupported => ConnectorWorkerRuntimeState::Unsupported,
        ConnectorWorkerStartDecision::PlannedOnly => ConnectorWorkerRuntimeState::Planned,
    };
    let last_error = if matches!(
        status,
        ConnectorWorkerRuntimeState::Invalid | ConnectorWorkerRuntimeState::Unsupported
    ) {
        Some(
            spec.validation_issues
                .iter()
                .map(|issue| issue.message.as_str())
                .collect::<Vec<_>>()
                .join("; "),
        )
        .filter(|value| !value.is_empty())
    } else {
        None
    };

    ConnectorWorkerRuntimeStatus {
        connector_id: spec.connector_id,
        connector_key: spec.connector_key.clone(),
        connector_type: spec.connector_type.clone(),
        connector_profile: spec.connector_profile.clone(),
        enabled: spec.enabled,
        worker_kind: spec.worker_kind.clone(),
        status,
        connected: false,
        subscribed: false,
        broker_url: spec.broker_url.clone(),
        client_id: spec.client_id.clone(),
        topic_filter: spec.topic_filter.clone(),
        http_path: spec.http_path.clone(),
        payload_format: spec.payload_format.clone(),
        content_type: spec.content_type.clone(),
        secret_ref_id: spec.secret_ref_id,
        last_error,
        last_message_at: None,
        last_successful_ingest_at: None,
        last_failed_ingest_at: None,
        started_at: None,
        stopped_at: None,
        restart_count: 0,
        reconnect_attempts: 0,
        last_disconnect_at: None,
        last_reconnect_at: None,
        next_reconnect_at: None,
        last_reconciled_at: None,
        validation_issues: spec.validation_issues.clone(),
        metadata: spec.metadata.clone(),
    }
}

fn connector_workers_status(state: &AppState) -> Result<IngestionWorkersStatusResponse, ApiError> {
    let plan = build_ingestion_worker_plan(state)?;
    let runtime_statuses = state
        .connector_worker_statuses
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_default();
    let workers = plan
        .specs
        .iter()
        .map(|spec| {
            runtime_statuses
                .get(&spec.connector_id)
                .cloned()
                .unwrap_or_else(|| connector_runtime_status_from_spec(spec))
        })
        .collect::<Vec<_>>();

    Ok(IngestionWorkersStatusResponse {
        connector_workers: connector_workers_readiness_from_workers(
            connector_workers_enabled(state),
            &workers,
        ),
        workers,
    })
}

fn connector_workers_readiness(state: &AppState) -> ConnectorWorkersReadiness {
    connector_workers_status(state)
        .map(|status| status.connector_workers)
        .unwrap_or_else(|_| ConnectorWorkersReadiness {
            enabled: connector_workers_enabled(state),
            total: 0,
            running: 0,
            degraded: 0,
            stopped: 0,
            skipped: 0,
            invalid: 0,
            errors: 1,
        })
}

fn connector_workers_readiness_from_workers(
    enabled: bool,
    workers: &[ConnectorWorkerRuntimeStatus],
) -> ConnectorWorkersReadiness {
    ConnectorWorkersReadiness {
        enabled,
        total: workers.len(),
        running: workers
            .iter()
            .filter(|worker| worker.status == ConnectorWorkerRuntimeState::Running)
            .count(),
        degraded: workers
            .iter()
            .filter(|worker| {
                matches!(
                    worker.status,
                    ConnectorWorkerRuntimeState::Degraded
                        | ConnectorWorkerRuntimeState::Reconnecting
                )
            })
            .count(),
        stopped: workers
            .iter()
            .filter(|worker| worker.status == ConnectorWorkerRuntimeState::Stopped)
            .count(),
        skipped: workers
            .iter()
            .filter(|worker| worker.status == ConnectorWorkerRuntimeState::Skipped)
            .count(),
        invalid: workers
            .iter()
            .filter(|worker| worker.status == ConnectorWorkerRuntimeState::Invalid)
            .count(),
        errors: workers
            .iter()
            .filter(|worker| {
                matches!(
                    worker.status,
                    ConnectorWorkerRuntimeState::Degraded
                        | ConnectorWorkerRuntimeState::Reconnecting
                        | ConnectorWorkerRuntimeState::Invalid
                        | ConnectorWorkerRuntimeState::Error
                )
            })
            .count(),
    }
}

fn connector_workers_enabled(state: &AppState) -> bool {
    state
        .connector_workers_enabled
        .read()
        .map(|guard| *guard)
        .unwrap_or(false)
}

fn set_connector_workers_enabled(state: &AppState, enabled: bool) {
    if let Ok(mut guard) = state.connector_workers_enabled.write() {
        *guard = enabled;
    }
}

fn connector_worker_spec(
    state: &AppState,
    connector: IngestionConnector,
) -> Result<IngestionWorkerSpec, ApiError> {
    let mut validation_issues = Vec::new();
    let worker_kind = match &connector.connector_type {
        IngestionConnectorType::Http => IngestionWorkerKind::HttpListener,
        IngestionConnectorType::Mqtt => IngestionWorkerKind::MqttSubscriber,
        IngestionConnectorType::Future => IngestionWorkerKind::Unsupported,
    };

    let status = if !connector.enabled {
        IngestionWorkerSpecStatus::Skipped
    } else {
        if let Some(secret_ref_id) = connector.secret_ref_id {
            match state
                .storage
                .get_connector_secret(state.tenant_id, secret_ref_id)?
            {
                Some(secret) if secret.secret_type != ConnectorSecretType::MqttBasicAuth => {
                    validation_issues.push(worker_issue(
                        "unsupported_secret_type",
                        "dynamic MQTT connector workers currently support only mqtt_basic_auth secrets",
                    ));
                }
                Some(_) => {}
                None => validation_issues.push(worker_issue(
                    "missing_secret_ref",
                    "connector secret_ref_id does not reference an existing connector secret",
                )),
            }
        }
        match &connector.connector_type {
            IngestionConnectorType::Http => {
                if connector.connector_profile == ConnectorProfile::TtnV3 {
                    validation_issues.push(worker_issue(
                        "invalid_connector_type",
                        "TTN v3 connector workers require connector_type mqtt",
                    ));
                }
                if connector
                    .http_path
                    .as_deref()
                    .or(connector.endpoint.as_deref())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    validation_issues.push(worker_issue(
                        "missing_http_path",
                        "HTTP connectors require http_path or endpoint before a listener can be planned",
                    ));
                }
                if validation_issues.is_empty() {
                    IngestionWorkerSpecStatus::Planned
                } else {
                    IngestionWorkerSpecStatus::Invalid
                }
            }
            IngestionConnectorType::Mqtt => {
                if connector
                    .broker_url
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    validation_issues.push(worker_issue(
                        "missing_broker_url",
                        "MQTT connectors require broker_url before a subscriber can be planned",
                    ));
                }
                if connector
                    .topic_filter
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    validation_issues.push(worker_issue(
                        "missing_topic_filter",
                        "MQTT connectors require topic_filter before a subscriber can be planned",
                    ));
                } else if connector.connector_profile == ConnectorProfile::TtnV3
                    && !connector
                        .topic_filter
                        .as_deref()
                        .map(is_plausible_ttn_topic_filter)
                        .unwrap_or(false)
                {
                    validation_issues.push(worker_issue(
                        "implausible_ttn_topic_filter",
                        "TTN v3 topic_filter should look like v3/{application_id}/devices/+/up",
                    ));
                }
                if connector.connector_profile == ConnectorProfile::TtnV3
                    && !connector
                        .payload_format
                        .as_deref()
                        .map(is_ttn_uplink_payload_format)
                        .unwrap_or(false)
                {
                    validation_issues.push(worker_issue(
                        "unsupported_ttn_payload_format",
                        "TTN v3 connector workers require payload_format = ttn-uplink-json in this milestone",
                    ));
                }
                if validation_issues.iter().any(|issue| {
                    matches!(
                        issue.code.as_str(),
                        "missing_broker_url"
                            | "missing_topic_filter"
                            | "invalid_connector_type"
                            | "implausible_ttn_topic_filter"
                            | "missing_secret_ref"
                            | "unsupported_secret_type"
                            | "unsupported_ttn_payload_format"
                    )
                }) {
                    IngestionWorkerSpecStatus::Invalid
                } else {
                    IngestionWorkerSpecStatus::Planned
                }
            }
            IngestionConnectorType::Future => {
                validation_issues.push(worker_issue(
                    "unsupported_connector_type",
                    "future connector types do not have runtime worker support yet",
                ));
                IngestionWorkerSpecStatus::Unsupported
            }
        }
    };

    Ok(IngestionWorkerSpec {
        connector_id: connector.id,
        connector_key: connector.connector_key,
        connector_type: connector.connector_type,
        connector_profile: connector.connector_profile,
        enabled: connector.enabled,
        worker_kind,
        broker_url: connector.broker_url,
        client_id: connector.client_id,
        topic_filter: connector.topic_filter,
        http_path: connector.http_path.or(connector.endpoint),
        payload_format: connector.payload_format,
        content_type: connector.content_type,
        secret_ref_id: connector.secret_ref_id,
        status,
        validation_issues,
        metadata: connector.metadata,
    })
}

fn worker_issue(
    code: impl Into<String>,
    message: impl Into<String>,
) -> IngestionWorkerValidationIssue {
    IngestionWorkerValidationIssue {
        code: code.into(),
        message: message.into(),
    }
}

fn connector_event_metadata(connector: &IngestionConnector) -> Value {
    json!({
        "connector_id": connector.id,
        "connector_key": connector.connector_key,
        "connector_type": connector.connector_type,
        "connector_profile": connector.connector_profile,
        "enabled": connector.enabled,
        "secret_ref_id": connector.secret_ref_id
    })
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

fn ttn_device_mapping_response(mapping: TtnDeviceMapping) -> TtnDeviceMappingResponse {
    TtnDeviceMappingResponse {
        id: mapping.id,
        tenant_id: mapping.tenant_id,
        connector_id: mapping.connector_id,
        ttn_application_id: mapping.ttn_application_id,
        ttn_device_id: mapping.ttn_device_id,
        producer_entity_id: mapping.producer_entity_id,
        feature_of_interest_id: mapping.feature_of_interest_id,
        enabled: mapping.enabled,
        metadata: mapping.metadata,
        created_at: mapping.created_at,
        updated_at: mapping.updated_at,
    }
}

fn ttn_device_mapping_event_metadata(mapping: &TtnDeviceMapping) -> Value {
    json!({
        "mapping_id": mapping.id,
        "connector_id": mapping.connector_id,
        "ttn_application_id": mapping.ttn_application_id,
        "ttn_device_id": mapping.ttn_device_id,
        "producer_entity_id": mapping.producer_entity_id,
        "feature_of_interest_id": mapping.feature_of_interest_id,
        "enabled": mapping.enabled
    })
}

fn metadata_with_connector(mut metadata: Value, connector_metadata: Option<Value>) -> Value {
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

fn decoded_ingest_metadata(decoded: &[DecodedMeasurement]) -> Value {
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

fn merge_json_object(target: &mut Value, source: Value) {
    let (Some(target), Some(source)) = (target.as_object_mut(), source.as_object()) else {
        return;
    };
    for (key, value) in source {
        target.insert(key.clone(), value.clone());
    }
}

fn parse_bool_env_value(
    value: Option<&str>,
    default: bool,
    variable_name: &str,
) -> Result<bool, StartupError> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        Some(other) => Err(StartupError::backend_initialization(format!(
            "invalid boolean value '{other}' for {variable_name}"
        ))),
    }
}

fn set_connector_worker_runtime_status(state: &AppState, status: ConnectorWorkerRuntimeStatus) {
    if let Ok(mut statuses) = state.connector_worker_statuses.write() {
        statuses.insert(status.connector_id, status);
    }
}

fn update_connector_worker_runtime_status(
    state: &AppState,
    connector_id: Uuid,
    update: impl FnOnce(&mut ConnectorWorkerRuntimeStatus),
) {
    if let Ok(mut statuses) = state.connector_worker_statuses.write() {
        if let Some(status) = statuses.get_mut(&connector_id) {
            update(status);
        }
    }
}

fn mark_connector_worker_starting(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Starting;
        worker.connected = false;
        worker.subscribed = false;
        worker.last_error = None;
        worker.started_at = worker.started_at.or_else(|| Some(Utc::now()));
    });
}

fn mark_connector_worker_connected(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Degraded;
        worker.connected = true;
        worker.last_error = None;
    });
}

fn mark_connector_worker_subscribed(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        if worker.reconnect_attempts > 0 {
            worker.last_reconnect_at = Some(Utc::now());
        }
        worker.status = ConnectorWorkerRuntimeState::Running;
        worker.connected = true;
        worker.subscribed = true;
        worker.last_error = None;
        worker.next_reconnect_at = None;
    });
}

fn mark_connector_worker_failure(state: &AppState, connector_id: Uuid, message: String) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Degraded;
        worker.connected = false;
        worker.subscribed = false;
        worker.last_error = Some(message);
        worker.last_disconnect_at = Some(Utc::now());
        worker.last_failed_ingest_at = Some(Utc::now());
    });
}

fn mark_connector_worker_reconnect_scheduled(
    state: &AppState,
    connector_id: Uuid,
    message: String,
    delay: std::time::Duration,
) -> DateTime<Utc> {
    let next_reconnect_at =
        Utc::now() + Duration::from_std(delay).unwrap_or_else(|_| Duration::seconds(60));
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Reconnecting;
        worker.connected = false;
        worker.subscribed = false;
        worker.reconnect_attempts = worker.reconnect_attempts.saturating_add(1);
        worker.last_error = Some(message);
        worker.next_reconnect_at = Some(next_reconnect_at);
    });
    next_reconnect_at
}

fn mark_connector_worker_message(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.last_message_at = Some(Utc::now());
    });
}

fn mark_connector_worker_ingest_success(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.last_successful_ingest_at = Some(Utc::now());
        worker.last_error = None;
    });
}

fn mark_connector_worker_ingest_failed(state: &AppState, connector_id: Uuid, message: String) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.last_failed_ingest_at = Some(Utc::now());
        worker.last_error = Some(message);
        if worker.status == ConnectorWorkerRuntimeState::Running {
            worker.status = ConnectorWorkerRuntimeState::Degraded;
        }
    });
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

fn ensure_executor_exists(state: &AppState, executor_id: Uuid) -> Result<(), ApiError> {
    state
        .storage
        .get_executor(state.tenant_id, executor_id)?
        .map(|_| ())
        .ok_or_else(ApiError::not_found)
}

fn get_executor_agent(state: &AppState, executor_id: Uuid) -> Result<ExecutorAgent, ApiError> {
    state
        .storage
        .get_executor(state.tenant_id, executor_id)?
        .ok_or_else(ApiError::not_found)
}

fn ensure_smartsentinel_executor(executor: &ExecutorAgent) -> Result<(), ApiError> {
    if executor.agent_type != "smartsentinel" {
        return Err(ApiError::bad_request(
            "executor is not registered as a SmartSentinel executor",
        ));
    }
    Ok(())
}

fn smart_sentinel_executor_capabilities(
    executor_id: Uuid,
    requests: Vec<SmartSentinelExecutorCapabilityRequest>,
) -> Result<Vec<ExecutorCapability>, ApiError> {
    requests
        .into_iter()
        .map(|request| match request {
            SmartSentinelExecutorCapabilityRequest::CommandType(command_type) => {
                ExecutorCapability::new(
                    executor_id,
                    command_type,
                    Some("smartsentinel".to_string()),
                    Some(json!({"source": "smartsentinel_bridge"})),
                )
            }
            SmartSentinelExecutorCapabilityRequest::Detailed {
                command_type,
                protocol,
                metadata,
            } => ExecutorCapability::new(
                executor_id,
                command_type,
                protocol.or_else(|| Some("smartsentinel".to_string())),
                metadata,
            ),
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| ApiError::bad_request(err.to_string()))
}

fn smart_sentinel_executor_scopes(
    state: &AppState,
    executor_id: Uuid,
    requests: Vec<PutExecutorScopeRequest>,
) -> Result<Vec<ExecutorScope>, ApiError> {
    let mut scopes = Vec::with_capacity(requests.len());
    for request in requests {
        if let Some(target_entity_id) = request.target_entity_id {
            ensure_entity_exists(state, target_entity_id)?;
        }
        scopes.push(ExecutorScope::new(
            executor_id,
            request.target_entity_id,
            request.entity_type,
            request.relationship_type,
            request.metadata,
        ));
    }
    Ok(scopes)
}

fn smartsentinel_command_envelope(
    state: &AppState,
    command: Command,
) -> Result<SmartSentinelCommandEnvelope, ApiError> {
    let latest_lease = state
        .storage
        .get_latest_command_lease(state.tenant_id, command.id)?;
    let target_entity = state
        .storage
        .get_entity(state.tenant_id, command.target_entity_id)?;
    let recent_provenance = state
        .storage
        .query_events(
            state.tenant_id,
            EventFilter {
                target_entity_id: Some(command.target_entity_id),
                ..Default::default()
            },
        )?
        .into_iter()
        .filter_map(|event| event.metadata)
        .filter(smartsentinel_metadata_has_provenance)
        .take(5)
        .collect();

    Ok(SmartSentinelCommandEnvelope {
        command,
        latest_lease,
        target_entity,
        recent_provenance,
    })
}

fn smartsentinel_metadata_has_provenance(metadata: &Value) -> bool {
    metadata.get("smartsentinel").is_some()
        || metadata.get("evidence_refs").is_some()
        || metadata.get("incident_id").is_some()
        || metadata.get("alert_id").is_some()
        || metadata.get("workflow_id").is_some()
        || metadata.get("run_id").is_some()
        || metadata.get("trace_id").is_some()
        || metadata
            .get("provenance")
            .map(|provenance| {
                provenance.get("run_id").is_some()
                    || provenance.get("trace_id").is_some()
                    || provenance.get("workflow_id").is_some()
            })
            .unwrap_or(false)
}

fn smartsentinel_report_metadata(
    executor: &ExecutorAgent,
    request: &SmartSentinelCommandReportRequest,
) -> Value {
    let mut metadata = json!({
        "source": "smartsentinel_bridge",
        "executor_id": executor.id,
        "agent_key": executor.agent_key,
        "agent_type": executor.agent_type,
        "status": request.status.as_str(),
        "verified": request.verified
    });

    if let Some(object) = metadata.as_object_mut() {
        insert_optional_string(object, "incident_id", request.incident_id.as_deref());
        insert_optional_string(object, "alert_id", request.alert_id.as_deref());
        insert_optional_string(object, "workflow_id", request.workflow_id.as_deref());
        insert_optional_string(object, "run_id", request.run_id.as_deref());
        insert_optional_string(object, "trace_id", request.trace_id.as_deref());
        insert_optional_string(object, "correlation_id", request.correlation_id.as_deref());
        if let Some(evidence_refs) = &request.evidence_refs {
            object.insert("evidence_refs".to_string(), evidence_refs.clone());
        }
        if let Some(extra) = &request.metadata {
            object.insert("metadata".to_string(), extra.clone());
        }
    }

    metadata
}

fn insert_optional_string(
    object: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        object.insert(key.to_string(), json!(value));
    }
}

fn ensure_executor_can_run_command(
    state: &AppState,
    executor_id: Uuid,
    command_id: Uuid,
) -> Result<Command, ApiError> {
    let command = state
        .storage
        .get_command(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;

    if !executor_can_run_command(state, executor_id, &command)? {
        return Err(ApiError::bad_request(
            "command is not compatible with executor capabilities or scopes",
        ));
    }

    Ok(command)
}

fn executor_can_run_command(
    state: &AppState,
    executor_id: Uuid,
    command: &Command,
) -> Result<bool, ApiError> {
    let capabilities = state
        .storage
        .list_executor_capabilities(state.tenant_id, executor_id)?;
    let has_capability = capabilities
        .iter()
        .any(|capability| capability.command_type == command.command_type);
    if !has_capability {
        return Ok(false);
    }

    let scopes = state
        .storage
        .list_executor_scopes(state.tenant_id, executor_id)?;
    if scopes.is_empty() {
        return Ok(false);
    }

    for scope in scopes {
        if executor_scope_matches_command(state, &scope, command)? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn executor_scope_matches_command(
    state: &AppState,
    scope: &ExecutorScope,
    command: &Command,
) -> Result<bool, ApiError> {
    if let Some(target_entity_id) = scope.target_entity_id {
        if target_entity_id != command.target_entity_id {
            return Ok(false);
        }
    }

    if let Some(entity_type) = scope.entity_type.as_deref() {
        let entity = state
            .storage
            .get_entity(state.tenant_id, command.target_entity_id)?
            .ok_or_else(ApiError::not_found)?;
        if entity.entity_type != entity_type {
            return Ok(false);
        }
    }

    if let Some(relationship_type) = scope.relationship_type.as_deref() {
        let outgoing = state.storage.list_relationships(
            state.tenant_id,
            Some(command.target_entity_id),
            None,
        )?;
        let incoming = state.storage.list_relationships(
            state.tenant_id,
            None,
            Some(command.target_entity_id),
        )?;
        if !outgoing
            .iter()
            .chain(incoming.iter())
            .any(|relationship| relationship.relationship_type == relationship_type)
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn get_command_for_executor_mutation(
    state: &AppState,
    command_id: Uuid,
    agent_key: &str,
) -> Result<Command, ApiError> {
    let command = state
        .storage
        .get_command(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;
    if command.claimed_by.as_deref() != Some(agent_key) {
        return Err(ApiError::bad_request(
            "command must be claimed by this executor before completion",
        ));
    }
    let lease = state
        .storage
        .get_active_command_lease(state.tenant_id, command_id)?
        .ok_or_else(|| ApiError::bad_request("command has no active lease"))?;
    if !lease.is_active_at(Utc::now()) {
        return Err(ApiError::bad_request("command lease has expired"));
    }

    Ok(command)
}

fn claim_command_for_executor(
    state: &AppState,
    command_id: Uuid,
    executor: &ExecutorAgent,
    lease_duration_seconds: Option<i64>,
    max_retries: Option<u32>,
    metadata: Option<Value>,
) -> Result<Command, ApiError> {
    let now = Utc::now();
    if let Some(lease) = state
        .storage
        .get_active_command_lease(state.tenant_id, command_id)?
    {
        if lease.is_active_at(now) {
            return Err(ApiError::bad_request("command already has an active lease"));
        }
    }
    let expires_at = lease_expiry(now, lease_duration_seconds)?;
    let command = mutate_command_raw(state, command_id, |command, now| {
        if let Some(max_retries) = max_retries {
            command.max_retries = Some(max_retries);
        }
        command.claim(executor.agent_key.clone(), now)?;
        command.set_lease_expires_at(Some(expires_at), now);
        Ok(())
    })?;
    let lease = CommandLease::new(
        state.tenant_id,
        command.id,
        executor.id,
        now,
        expires_at,
        metadata,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let lease = state.storage.store_command_lease(lease)?;
    record_lease_event(
        state,
        "aion:CommandLeaseCreated",
        &lease,
        Some(&command),
        None,
    )?;
    record_command_event(
        state,
        "aion:CommandClaimed",
        EventSeverity::Info,
        &command,
        None,
    )?;
    Ok(command)
}

fn lease_expiry(
    now: DateTime<Utc>,
    lease_duration_seconds: Option<i64>,
) -> Result<DateTime<Utc>, ApiError> {
    let seconds = lease_duration_seconds.unwrap_or(DEFAULT_COMMAND_LEASE_SECONDS);
    if seconds <= 0 {
        return Err(ApiError::bad_request(
            "lease_duration_seconds must be greater than zero",
        ));
    }
    Ok(now + chrono::Duration::seconds(seconds))
}

fn active_lease_for_executor(
    state: &AppState,
    command_id: Uuid,
    executor_id: Uuid,
) -> Result<CommandLease, ApiError> {
    let lease = state
        .storage
        .get_active_command_lease(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;
    if lease.executor_id != executor_id {
        return Err(ApiError::bad_request(
            "active lease is owned by another executor",
        ));
    }
    if !lease.is_active_at(Utc::now()) {
        return Err(ApiError::bad_request("active lease has expired"));
    }
    Ok(lease)
}

fn release_active_lease(
    state: &AppState,
    command_id: Uuid,
    executor_id: Uuid,
) -> Result<CommandLease, ApiError> {
    let mut lease = active_lease_for_executor(state, command_id, executor_id)?;
    let now = Utc::now();
    lease.mark_released(now);
    let lease = state.storage.update_command_lease(lease)?;
    let command = mutate_command_raw(state, command_id, |command, now| command.release(now))?;
    record_lease_event(
        state,
        "aion:CommandLeaseReleased",
        &lease,
        Some(&command),
        None,
    )?;
    record_command_event(
        state,
        "aion:CommandReleased",
        EventSeverity::Info,
        &command,
        Some("command lease released".to_string()),
    )?;
    Ok(lease)
}

fn mark_active_lease_completed(
    state: &AppState,
    command_id: Uuid,
    executor_id: Uuid,
) -> Result<CommandLease, ApiError> {
    let mut lease = active_lease_for_executor(state, command_id, executor_id)?;
    lease.mark_completed(Utc::now());
    Ok(state.storage.update_command_lease(lease)?)
}

fn mark_active_lease_failed(
    state: &AppState,
    command_id: Uuid,
    executor_id: Uuid,
) -> Result<CommandLease, ApiError> {
    let mut lease = active_lease_for_executor(state, command_id, executor_id)?;
    lease.mark_failed(Utc::now());
    Ok(state.storage.update_command_lease(lease)?)
}

fn record_lease_event(
    state: &AppState,
    event_type: impl Into<String>,
    lease: &CommandLease,
    command: Option<&Command>,
    metadata: Option<Value>,
) -> Result<Event, ApiError> {
    let mut event_metadata = json!({
        "lease_id": lease.id,
        "executor_id": lease.executor_id,
        "lease_status": lease.lease_status,
        "expires_at": lease.expires_at
    });
    if let Some(object) = event_metadata.as_object_mut() {
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
            message: Some("Command lease lifecycle event".to_string()),
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: Some(lease.command_id),
            action_id: None,
            action_result_id: None,
            metadata: Some(event_metadata),
        },
    )
}

fn enrich_executor_result_metadata(executor: &ExecutorAgent, metadata: Option<Value>) -> Value {
    let mut enriched = json!({
        "executor_id": executor.id,
        "agent_key": executor.agent_key,
        "source": "executor_api"
    });
    if let (Some(object), Some(metadata)) = (enriched.as_object_mut(), metadata) {
        object.insert("executor_metadata".to_string(), metadata);
    }

    enriched
}

fn record_executor_event(
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

fn record_command_event(
    state: &AppState,
    event_type: impl Into<String>,
    severity: EventSeverity,
    command: &Command,
    message: Option<String>,
) -> Result<Event, ApiError> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity,
            source_entity_id: None,
            target_entity_id: Some(command.target_entity_id),
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: Some(command.id),
            action_id: None,
            action_result_id: None,
            metadata: Some(json!({
                "command_type": command.command_type,
                "status": command.status,
                "approval_status": command.approval_status,
                "claimed_by": command.claimed_by
            })),
        },
    )
}

fn record_connector_event(
    state: &AppState,
    event_type: impl Into<String>,
    connector: &IngestionConnector,
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
            metadata: Some(connector_event_metadata(connector)),
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

fn record_ttn_device_mapping_event(
    state: &AppState,
    event_type: impl Into<String>,
    mapping: &TtnDeviceMapping,
    message: Option<String>,
) -> Result<Event, ApiError> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.into(),
            severity: EventSeverity::Info,
            source_entity_id: Some(mapping.producer_entity_id),
            target_entity_id: mapping.feature_of_interest_id,
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(ttn_device_mapping_event_metadata(mapping)),
        },
    )
}

fn record_connector_worker_event(
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
fn record_ingest_event(
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
fn record_ingest_event_optional(
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

fn mutate_command(
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

fn mutate_command_raw(
    state: &AppState,
    command_id: Uuid,
    mutate: impl FnOnce(&mut Command, DateTime<Utc>) -> Result<(), aion_action::ActionModelError>,
) -> Result<Command, ApiError> {
    let mut command = state
        .storage
        .get_command(state.tenant_id, command_id)?
        .ok_or_else(ApiError::not_found)?;
    mutate(&mut command, Utc::now()).map_err(|err| ApiError::bad_request(err.to_string()))?;
    let command = state.storage.update_command(command)?;
    Ok(command)
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

fn evaluate_rules_for_observation(
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

fn evaluate_rules_for_event(
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

fn command_policy_decision(
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

fn empty_object() -> Value {
    json!({})
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use aion_storage::{ApiTokenStore, RelationshipStore};
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use serde_json::json;
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
            extract_jsonld_entity_key(explicit.as_object().unwrap()).as_deref(),
            Some("explicit-zone-key")
        );

        let semantic = json!({
            "aion:entityKey": "semantic-zone-key"
        });
        assert_eq!(
            extract_jsonld_entity_key(semantic.as_object().unwrap()).as_deref(),
            Some("semantic-zone-key")
        );
    }

    #[test]
    fn derives_semantic_entity_key_from_jsonld_id() {
        assert_eq!(
            derive_entity_key("urn:aion:farm:01:zone:01").as_deref(),
            Some("zone-01")
        );
        assert_eq!(
            derive_entity_key("urn:aion:farm:01:soil-sensor:01").as_deref(),
            Some("soil-sensor-01")
        );
        assert_eq!(
            derive_entity_key("urn:aion:sensor:runtime-jsonld-01").as_deref(),
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
}
