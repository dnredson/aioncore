use super::*;
use ::postgres::error::SqlState;
use ::postgres::types::{Json, ToSql};
use ::postgres::{Client, Config as PgConfig, NoTls, Row};
use aion_action::{ApprovalStatus, ExecutorAgentStatus};
use aion_action::{EdgeAdapter, EdgeAdapterStatus, EdgeAdapterStatusReport, EdgeAdapterType};
use aion_event::EventSeverity;
use aion_observation::ObservationValue;
use aion_raw_message::{NormalizationStatus, RawMessageSource};
use aion_rule::RuleTriggerType;
use serde::Serialize;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PostgresStorageConfig {
    pub database_url: String,
    pub connect_timeout: Option<Duration>,
}

impl PostgresStorageConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            connect_timeout: None,
        }
    }
}

pub struct PostgresStorage {
    client: Mutex<Client>,
}

impl fmt::Debug for PostgresStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresStorage").finish_non_exhaustive()
    }
}

impl PostgresStorage {
    pub fn connect(config: PostgresStorageConfig) -> StorageResult<Self> {
        let mut pg_config: PgConfig = config
            .database_url
            .parse()
            .map_err(|err| StorageError::Backend(format!("invalid postgres URL: {err}")))?;
        if let Some(connect_timeout) = config.connect_timeout {
            pg_config.connect_timeout(connect_timeout);
        }

        let client = pg_config.connect(NoTls).map_err(map_postgres_error)?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    pub fn from_client(client: Client) -> Self {
        Self {
            client: Mutex::new(client),
        }
    }

    pub fn run_embedded_migrations(&self) -> StorageResult<()> {
        self.with_client(|client| {
            for (name, sql) in ORDERED_MIGRATIONS {
                client
                    .batch_execute(sql)
                    .map_err(|err| backend_error_with_context(name, err))?;
            }
            Ok(())
        })
    }

    fn with_client<T>(&self, f: impl FnOnce(&mut Client) -> StorageResult<T>) -> StorageResult<T> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| StorageError::Backend("postgres storage lock was poisoned".to_string()))?;
        f(&mut client)
    }
}

impl StorageBackend for PostgresStorage {
    fn check_readiness(&self) -> StorageResult<()> {
        self.with_client(|client| {
            client
                .simple_query("SELECT 1")
                .map_err(map_postgres_error)?;
            Ok(())
        })
    }
}

fn backend_error_with_context(name: &str, err: ::postgres::Error) -> StorageError {
    StorageError::Backend(format!("failed to run migration {name}: {err}"))
}

fn map_postgres_error(err: ::postgres::Error) -> StorageError {
    if let Some(code) = err.code() {
        if *code == SqlState::UNIQUE_VIOLATION {
            return StorageError::Conflict;
        }
        if *code == SqlState::FOREIGN_KEY_VIOLATION {
            return StorageError::InvalidInput(err.to_string());
        }
    }

    StorageError::Backend(err.to_string())
}

fn json_column(value: &Value) -> Json<Value> {
    Json(value.clone())
}

fn json_option_column(value: Option<&Value>) -> Option<Json<Value>> {
    value.cloned().map(Json)
}

fn row_to_tenant(row: Row) -> Tenant {
    let Json(metadata) = row.get::<_, Json<Value>>("metadata");
    Tenant {
        id: row.get("id"),
        slug: row.get("slug"),
        name: row.get("name"),
        metadata,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_entity(row: Row) -> Entity {
    let Json(jsonld) = row.get::<_, Json<Value>>("jsonld");
    Entity {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        entity_key: row.get("entity_key"),
        entity_type: row.get("entity_type"),
        jsonld,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_relationship(row: Row) -> Relationship {
    let Json(jsonld) = row.get::<_, Json<Value>>("jsonld");
    Relationship {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        source_entity_id: row.get("source_entity_id"),
        relationship_type: row.get("relationship_type"),
        target_entity_id: row.get("target_entity_id"),
        jsonld,
        created_at: row.get("created_at"),
    }
}

fn row_to_payload_profile(row: Row) -> PayloadProfile {
    let attribute_mapping = row
        .get::<_, Option<Json<Value>>>("attribute_mapping")
        .map(|Json(value)| value);
    let metadata = row
        .get::<_, Option<Json<Value>>>("metadata")
        .map(|Json(value)| value);
    PayloadProfile {
        entity_id: row.get("entity_id"),
        payload_format: row.get("payload_format"),
        protocol: row.get("protocol"),
        content_type: row.get("content_type"),
        attribute_mapping,
        metadata,
    }
}

fn row_to_ingestion_connector(row: Row) -> StorageResult<IngestionConnector> {
    Ok(IngestionConnector {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        connector_key: row.get("connector_key"),
        connector_type: ingestion_connector_type_from_db(row.get::<_, String>("connector_type"))?,
        connector_profile: connector_profile_from_db(row.get::<_, String>("connector_profile"))?,
        enabled: row.get("enabled"),
        display_name: row.get("display_name"),
        protocol: row.get("protocol"),
        endpoint: row.get("endpoint"),
        broker_url: row.get("broker_url"),
        client_id: row.get("client_id"),
        topic_filter: row.get("topic_filter"),
        http_path: row.get("http_path"),
        payload_format: row.get("payload_format"),
        content_type: row.get("content_type"),
        secret_ref_id: row.get("secret_ref_id"),
        default_producer_entity_id: row.get("default_producer_entity_id"),
        default_feature_of_interest_id: row.get("default_feature_of_interest_id"),
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_connector_secret(row: Row) -> StorageResult<ConnectorSecret> {
    Ok(ConnectorSecret {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        secret_key: row.get("secret_key"),
        secret_type: connector_secret_type_from_db(row.get::<_, String>("secret_type"))?,
        username: row.get("username"),
        secret_value: row.get("secret_value"),
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_ttn_device_mapping(row: Row) -> TtnDeviceMapping {
    TtnDeviceMapping {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        connector_id: row.get("connector_id"),
        ttn_application_id: row.get("ttn_application_id"),
        ttn_device_id: row.get("ttn_device_id"),
        producer_entity_id: row.get("producer_entity_id"),
        feature_of_interest_id: row.get("feature_of_interest_id"),
        enabled: row.get("enabled"),
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn validate_ttn_device_mapping_conflict_postgres(
    client: &mut Client,
    mapping: &TtnDeviceMapping,
) -> StorageResult<()> {
    if !mapping.enabled {
        return Ok(());
    }

    let row = client
        .query_opt(
            "
            SELECT id, tenant_id, connector_id, ttn_application_id, ttn_device_id,
                producer_entity_id, feature_of_interest_id, enabled, metadata,
                created_at, updated_at
            FROM ttn_device_mappings
            WHERE tenant_id = $1
                AND connector_id = $2
                AND id <> $3
                AND enabled = TRUE
                AND ttn_device_id = $4
                AND (
                    (ttn_application_id IS NULL AND $5::TEXT IS NULL)
                    OR ttn_application_id = $5
                )
            LIMIT 1
            ",
            &[
                &mapping.tenant_id,
                &mapping.connector_id,
                &mapping.id,
                &mapping.ttn_device_id,
                &mapping.ttn_application_id,
            ],
        )
        .map_err(map_postgres_error)?;

    if row.is_some() {
        let scope = mapping
            .ttn_application_id
            .as_deref()
            .map(|application_id| format!("application '{application_id}'"))
            .unwrap_or_else(|| "fallback device".to_string());
        return Err(StorageError::ConflictWithMessage(format!(
            "enabled TTN mapping conflict for connector {}, device '{}', {scope}",
            mapping.connector_id, mapping.ttn_device_id
        )));
    }

    Ok(())
}

fn ingestion_connector_type_to_db(connector_type: &IngestionConnectorType) -> &'static str {
    match connector_type {
        IngestionConnectorType::Http => "http",
        IngestionConnectorType::Mqtt => "mqtt",
        IngestionConnectorType::Future => "future",
    }
}

fn ingestion_connector_type_from_db(value: String) -> StorageResult<IngestionConnectorType> {
    match value.as_str() {
        "http" => Ok(IngestionConnectorType::Http),
        "mqtt" => Ok(IngestionConnectorType::Mqtt),
        "future" => Ok(IngestionConnectorType::Future),
        other => Err(StorageError::Backend(format!(
            "unknown ingestion connector type in database: {other}"
        ))),
    }
}

fn connector_profile_to_db(connector_profile: &ConnectorProfile) -> &'static str {
    match connector_profile {
        ConnectorProfile::GenericAionMqtt => "generic-aion-mqtt",
        ConnectorProfile::GenericMqtt => "generic-mqtt",
        ConnectorProfile::TtnV3 => "ttn-v3",
        ConnectorProfile::Custom => "custom",
    }
}

fn connector_profile_from_db(value: String) -> StorageResult<ConnectorProfile> {
    match value.as_str() {
        "generic-aion-mqtt" => Ok(ConnectorProfile::GenericAionMqtt),
        "generic-mqtt" => Ok(ConnectorProfile::GenericMqtt),
        "ttn-v3" => Ok(ConnectorProfile::TtnV3),
        "custom" => Ok(ConnectorProfile::Custom),
        other => Err(StorageError::Backend(format!(
            "unknown connector profile in database: {other}"
        ))),
    }
}

fn connector_secret_type_to_db(secret_type: &ConnectorSecretType) -> &'static str {
    match secret_type {
        ConnectorSecretType::MqttBasicAuth => "mqtt_basic_auth",
        ConnectorSecretType::Token => "token",
        ConnectorSecretType::ApiKey => "api_key",
        ConnectorSecretType::Custom => "custom",
    }
}

fn connector_secret_type_from_db(value: String) -> StorageResult<ConnectorSecretType> {
    match value.as_str() {
        "mqtt_basic_auth" => Ok(ConnectorSecretType::MqttBasicAuth),
        "token" => Ok(ConnectorSecretType::Token),
        "api_key" => Ok(ConnectorSecretType::ApiKey),
        "custom" => Ok(ConnectorSecretType::Custom),
        other => Err(StorageError::Backend(format!(
            "unknown connector secret type in database: {other}"
        ))),
    }
}

fn row_to_capability(row: Row) -> Capability {
    let metadata = row
        .get::<_, Option<Json<Value>>>("metadata")
        .map(|Json(value)| value);
    Capability {
        entity_id: row.get("entity_id"),
        capability_name: row.get("capability_name"),
        command_type: row.get("command_type"),
        protocol: row.get("protocol"),
        metadata,
    }
}

fn row_to_executor(row: Row) -> StorageResult<ExecutorAgent> {
    Ok(ExecutorAgent {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        agent_key: row.get("agent_key"),
        agent_type: row.get("agent_type"),
        display_name: row.get("display_name"),
        status: executor_status_from_db(row.get::<_, String>("status"))?,
        last_seen_at: row.get("last_seen_at"),
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_edge_adapter(row: Row) -> StorageResult<EdgeAdapter> {
    Ok(EdgeAdapter {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        adapter_key: row.get("adapter_key"),
        display_name: row.get("display_name"),
        adapter_type: edge_adapter_type_from_db(row.get::<_, String>("adapter_type"))?,
        status: edge_adapter_status_from_db(row.get::<_, String>("status"))?,
        version: row.get("version"),
        host_id: row.get("host_id"),
        site_id: row.get("site_id"),
        environment: row.get("environment"),
        last_seen_at: row.get("last_seen_at"),
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_edge_adapter_status(row: Row) -> StorageResult<EdgeAdapterStatusReport> {
    Ok(EdgeAdapterStatusReport {
        adapter_id: row.get("adapter_id"),
        status: edge_adapter_status_from_db(row.get::<_, String>("status"))?,
        observed_at: row.get("observed_at"),
        uptime_seconds: row
            .get::<_, Option<i64>>("uptime_seconds")
            .map(|value| value as u64),
        active_connectors: row.get("active_connectors"),
        active_plugins: row.get("active_plugins"),
        dlq_depth: row
            .get::<_, Option<i64>>("dlq_depth")
            .map(|value| value as u64),
        dlq_oldest_record_at: row.get("dlq_oldest_record_at"),
        last_publish_success_at: row.get("last_publish_success_at"),
        last_publish_failure_at: row.get("last_publish_failure_at"),
        last_error: row.get("last_error"),
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
    })
}

fn row_to_executor_capability(row: Row) -> ExecutorCapability {
    let metadata = row
        .get::<_, Option<Json<Value>>>("metadata")
        .map(|Json(value)| value);
    ExecutorCapability {
        agent_id: row.get("agent_id"),
        command_type: row.get("command_type"),
        protocol: row.get("protocol"),
        metadata,
    }
}

fn row_to_executor_scope(row: Row) -> ExecutorScope {
    let metadata = row
        .get::<_, Option<Json<Value>>>("metadata")
        .map(|Json(value)| value);
    ExecutorScope {
        agent_id: row.get("agent_id"),
        target_entity_id: row.get("target_entity_id"),
        entity_type: row.get("entity_type"),
        relationship_type: row.get("relationship_type"),
        metadata,
    }
}

fn raw_message_source_to_db(source_type: &RawMessageSource) -> &'static str {
    match source_type {
        RawMessageSource::Http => "http",
        RawMessageSource::Mqtt => "mqtt",
    }
}

fn raw_message_source_from_db(source_type: String) -> StorageResult<RawMessageSource> {
    match source_type.as_str() {
        "http" => Ok(RawMessageSource::Http),
        "mqtt" => Ok(RawMessageSource::Mqtt),
        other => Err(StorageError::Backend(format!(
            "unknown raw message source type in database: {other}"
        ))),
    }
}

fn normalization_status_to_db(status: &NormalizationStatus) -> &'static str {
    match status {
        NormalizationStatus::Pending => "pending",
        NormalizationStatus::Normalized => "normalized",
        NormalizationStatus::Failed => "failed",
    }
}

fn normalization_status_from_db(status: String) -> StorageResult<NormalizationStatus> {
    match status.as_str() {
        "pending" => Ok(NormalizationStatus::Pending),
        "normalized" => Ok(NormalizationStatus::Normalized),
        "failed" => Ok(NormalizationStatus::Failed),
        other => Err(StorageError::Backend(format!(
            "unknown raw message normalization status in database: {other}"
        ))),
    }
}

fn event_severity_to_db(severity: &EventSeverity) -> &'static str {
    match severity {
        EventSeverity::Debug => "debug",
        EventSeverity::Info => "info",
        EventSeverity::Warning => "warning",
        EventSeverity::Error => "error",
        EventSeverity::Critical => "critical",
    }
}

fn event_severity_from_db(severity: String) -> StorageResult<EventSeverity> {
    match severity.as_str() {
        "debug" => Ok(EventSeverity::Debug),
        "info" => Ok(EventSeverity::Info),
        "warning" => Ok(EventSeverity::Warning),
        "error" => Ok(EventSeverity::Error),
        "critical" => Ok(EventSeverity::Critical),
        other => Err(StorageError::Backend(format!(
            "unknown event severity in database: {other}"
        ))),
    }
}

fn json_serializable<T: Serialize>(value: &T) -> StorageResult<Json<Value>> {
    serde_json::to_value(value)
        .map(Json)
        .map_err(|err| StorageError::Backend(format!("failed to serialize JSON value: {err}")))
}

fn command_status_to_db(status: &CommandStatus) -> &'static str {
    match status {
        CommandStatus::Pending => "pending",
        CommandStatus::Claimed => "claimed",
        CommandStatus::Executed => "executed",
        CommandStatus::Failed => "failed",
        CommandStatus::Cancelled => "cancelled",
    }
}

fn command_status_from_db(status: String) -> StorageResult<CommandStatus> {
    match status.as_str() {
        "pending" => Ok(CommandStatus::Pending),
        "claimed" => Ok(CommandStatus::Claimed),
        "executed" => Ok(CommandStatus::Executed),
        "failed" => Ok(CommandStatus::Failed),
        "cancelled" => Ok(CommandStatus::Cancelled),
        other => Err(StorageError::Backend(format!(
            "unknown command status in database: {other}"
        ))),
    }
}

fn approval_status_to_db(status: &ApprovalStatus) -> &'static str {
    match status {
        ApprovalStatus::NotRequired => "not_required",
        ApprovalStatus::Required => "required",
        ApprovalStatus::Approved => "approved",
        ApprovalStatus::Rejected => "rejected",
    }
}

fn approval_status_from_db(status: String) -> StorageResult<ApprovalStatus> {
    match status.as_str() {
        "not_required" => Ok(ApprovalStatus::NotRequired),
        "required" => Ok(ApprovalStatus::Required),
        "approved" => Ok(ApprovalStatus::Approved),
        "rejected" => Ok(ApprovalStatus::Rejected),
        other => Err(StorageError::Backend(format!(
            "unknown approval status in database: {other}"
        ))),
    }
}

fn command_lease_status_to_db(status: &CommandLeaseStatus) -> &'static str {
    match status {
        CommandLeaseStatus::Active => "active",
        CommandLeaseStatus::Expired => "expired",
        CommandLeaseStatus::Released => "released",
        CommandLeaseStatus::Completed => "completed",
        CommandLeaseStatus::Failed => "failed",
    }
}

fn command_lease_status_from_db(status: String) -> StorageResult<CommandLeaseStatus> {
    match status.as_str() {
        "active" => Ok(CommandLeaseStatus::Active),
        "expired" => Ok(CommandLeaseStatus::Expired),
        "released" => Ok(CommandLeaseStatus::Released),
        "completed" => Ok(CommandLeaseStatus::Completed),
        "failed" => Ok(CommandLeaseStatus::Failed),
        other => Err(StorageError::Backend(format!(
            "unknown command lease status in database: {other}"
        ))),
    }
}

fn rule_trigger_type_to_db(trigger_type: &RuleTriggerType) -> &'static str {
    match trigger_type {
        RuleTriggerType::ObservationCreated => "observation_created",
        RuleTriggerType::EventCreated => "event_created",
        RuleTriggerType::Manual => "manual",
    }
}

fn rule_trigger_type_from_db(trigger_type: String) -> StorageResult<RuleTriggerType> {
    match trigger_type.as_str() {
        "observation_created" => Ok(RuleTriggerType::ObservationCreated),
        "event_created" => Ok(RuleTriggerType::EventCreated),
        "manual" => Ok(RuleTriggerType::Manual),
        other => Err(StorageError::Backend(format!(
            "unknown rule trigger type in database: {other}"
        ))),
    }
}

fn row_to_command(row: Row) -> StorageResult<Command> {
    Ok(Command {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        target_entity_id: row.get("target_entity_id"),
        command_type: row.get("command_type"),
        payload: row.get("payload"),
        status: command_status_from_db(row.get::<_, String>("status"))?,
        requested_by: row.get("requested_by"),
        reason: row.get("reason"),
        claimed_by: row.get("claimed_by"),
        claimed_at: row.get("claimed_at"),
        completed_at: row.get("completed_at"),
        failure_reason: row.get("failure_reason"),
        approval_status: row
            .get::<_, Option<String>>("approval_status")
            .map(approval_status_from_db)
            .transpose()?,
        policy_decision: row
            .get::<_, Option<Json<Value>>>("policy_decision")
            .map(|Json(value)| value),
        retry_count: row.get::<_, i32>("retry_count") as u32,
        max_retries: row
            .get::<_, Option<i32>>("max_retries")
            .map(|value| value as u32),
        lease_expires_at: row.get("lease_expires_at"),
        last_claimed_by: row.get("last_claimed_by"),
        last_failure_reason: row.get("last_failure_reason"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_command_lease(row: Row) -> StorageResult<CommandLease> {
    Ok(CommandLease {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        command_id: row.get("command_id"),
        executor_id: row.get("executor_id"),
        lease_status: command_lease_status_from_db(row.get::<_, String>("lease_status"))?,
        claimed_at: row.get("claimed_at"),
        expires_at: row.get("expires_at"),
        released_at: row.get("released_at"),
        completed_at: row.get("completed_at"),
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
    })
}

fn row_to_action(row: Row) -> Action {
    Action {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        command_id: row.get("command_id"),
        executor_entity_id: row.get("executor_entity_id"),
        action_type: row.get("action_type"),
        status: row.get("status"),
        started_at: row.get("started_at"),
        finished_at: row.get("finished_at"),
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
    }
}

fn row_to_action_result(row: Row) -> ActionResult {
    ActionResult {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        command_id: row.get("command_id"),
        action_id: row.get("action_id"),
        status: row.get("status"),
        verified: row.get("verified"),
        result_payload: row.get("result_payload"),
        observed_at: row.get("observed_at"),
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
    }
}

fn row_to_rule(row: Row) -> StorageResult<Rule> {
    let Json(condition) = row.get::<_, Json<Value>>("condition");
    let Json(action) = row.get::<_, Json<Value>>("action");
    Ok(Rule {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        name: row.get("name"),
        description: row.get("description"),
        enabled: row.get("enabled"),
        trigger_type: rule_trigger_type_from_db(row.get::<_, String>("trigger_type"))?,
        target_entity_id: row.get("target_entity_id"),
        observed_property: row.get("observed_property"),
        event_type: row.get("event_type"),
        condition: serde_json::from_value(condition)
            .map_err(|err| StorageError::Backend(format!("invalid rule condition: {err}")))?,
        action: serde_json::from_value(action)
            .map_err(|err| StorageError::Backend(format!("invalid rule action: {err}")))?,
        metadata: row
            .get::<_, Option<Json<Value>>>("metadata")
            .map(|Json(value)| value),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn row_to_raw_message(row: Row) -> StorageResult<RawMessage> {
    let Json(headers) = row.get::<_, Json<Value>>("headers");
    Ok(RawMessage {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        source_type: raw_message_source_from_db(row.get::<_, String>("source_type"))?,
        source_ref: row.get("source_ref"),
        device_key: row.get("device_key"),
        decoder_hint: row.get("decoder_hint"),
        content_type: row.get("content_type"),
        producer_entity_id: row.get("producer_entity_id"),
        feature_of_interest_id: row.get("feature_of_interest_id"),
        payload_format: row.get("payload_format"),
        headers,
        payload: row.get("payload"),
        received_at: row.get("received_at"),
        normalization_status: normalization_status_from_db(
            row.get::<_, String>("normalization_status"),
        )?,
        normalization_error: row.get("normalization_error"),
    })
}

fn observation_value_to_columns(
    value: &ObservationValue,
) -> (
    Option<f64>,
    Option<String>,
    Option<bool>,
    Option<Json<Value>>,
) {
    match value {
        ObservationValue::Number { value } => (Some(*value), None, None, None),
        ObservationValue::Text { value } => (None, Some(value.clone()), None, None),
        ObservationValue::Bool { value } => (None, None, Some(*value), None),
        ObservationValue::Json { value } => (None, None, None, Some(Json(value.clone()))),
    }
}

fn row_to_observation(row: Row) -> StorageResult<Observation> {
    let Json(quality) = row.get::<_, Json<Value>>("quality");
    let Json(metadata) = row.get::<_, Json<Value>>("metadata");
    let value = if let Some(value) = row.get::<_, Option<f64>>("value_number") {
        ObservationValue::Number { value }
    } else if let Some(value) = row.get::<_, Option<String>>("value_string") {
        ObservationValue::Text { value }
    } else if let Some(value) = row.get::<_, Option<bool>>("value_bool") {
        ObservationValue::Bool { value }
    } else if let Some(Json(value)) = row.get::<_, Option<Json<Value>>>("value_json") {
        ObservationValue::Json { value }
    } else {
        return Err(StorageError::Backend(
            "observation row is missing a canonical value".to_string(),
        ));
    };

    Ok(Observation {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        producer_entity_id: row.get("producer_entity_id"),
        feature_of_interest_id: row.get("feature_of_interest_id"),
        observed_property: row.get("observed_property"),
        value,
        unit: row.get("unit"),
        observed_at: row.get("observed_at"),
        received_at: row.get("received_at"),
        protocol: row.get("protocol"),
        payload_format: row.get("payload_format"),
        raw_message_id: row.get("raw_message_id"),
        quality,
        metadata,
    })
}

fn row_to_event(row: Row) -> StorageResult<Event> {
    let severity = event_severity_from_db(row.get::<_, String>("severity"))?;
    let metadata = row
        .get::<_, Option<Json<Value>>>("metadata")
        .map(|Json(value)| value);
    Ok(Event {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        event_type: row.get("event_type"),
        severity,
        source_entity_id: row.get("source_entity_id"),
        target_entity_id: row.get("target_entity_id"),
        message: row.get("message"),
        occurred_at: row.get("occurred_at"),
        observed_at: row.get("observed_at"),
        correlation_id: row.get("correlation_id"),
        raw_message_id: row.get("raw_message_id"),
        observation_id: row.get("observation_id"),
        command_id: row.get("command_id"),
        action_id: row.get("action_id"),
        action_result_id: row.get("action_result_id"),
        metadata,
        created_at: row.get("created_at"),
    })
}

fn executor_status_to_db(status: &ExecutorAgentStatus) -> &'static str {
    match status {
        ExecutorAgentStatus::Online => "online",
        ExecutorAgentStatus::Offline => "offline",
        ExecutorAgentStatus::Degraded => "degraded",
    }
}

fn executor_status_from_db(status: String) -> StorageResult<ExecutorAgentStatus> {
    match status.as_str() {
        "online" => Ok(ExecutorAgentStatus::Online),
        "offline" => Ok(ExecutorAgentStatus::Offline),
        "degraded" => Ok(ExecutorAgentStatus::Degraded),
        other => Err(StorageError::Backend(format!(
            "unknown executor status in database: {other}"
        ))),
    }
}

fn edge_adapter_type_to_db(adapter_type: &EdgeAdapterType) -> &'static str {
    match adapter_type {
        EdgeAdapterType::Edge => "edge",
        EdgeAdapterType::Fog => "fog",
        EdgeAdapterType::Cloud => "cloud",
        EdgeAdapterType::Lab => "lab",
        EdgeAdapterType::Custom => "custom",
    }
}

fn edge_adapter_type_from_db(value: String) -> StorageResult<EdgeAdapterType> {
    match value.as_str() {
        "edge" => Ok(EdgeAdapterType::Edge),
        "fog" => Ok(EdgeAdapterType::Fog),
        "cloud" => Ok(EdgeAdapterType::Cloud),
        "lab" => Ok(EdgeAdapterType::Lab),
        "custom" => Ok(EdgeAdapterType::Custom),
        other => Err(StorageError::Backend(format!(
            "unknown edge adapter type in database: {other}"
        ))),
    }
}

fn edge_adapter_status_to_db(status: &EdgeAdapterStatus) -> &'static str {
    match status {
        EdgeAdapterStatus::Online => "online",
        EdgeAdapterStatus::Offline => "offline",
        EdgeAdapterStatus::Degraded => "degraded",
        EdgeAdapterStatus::Unknown => "unknown",
    }
}

fn edge_adapter_status_from_db(value: String) -> StorageResult<EdgeAdapterStatus> {
    match value.as_str() {
        "online" => Ok(EdgeAdapterStatus::Online),
        "offline" => Ok(EdgeAdapterStatus::Offline),
        "degraded" => Ok(EdgeAdapterStatus::Degraded),
        "unknown" => Ok(EdgeAdapterStatus::Unknown),
        other => Err(StorageError::Backend(format!(
            "unknown edge adapter status in database: {other}"
        ))),
    }
}

fn is_unique_violation(err: &::postgres::Error) -> bool {
    matches!(err.code(), Some(code) if *code == SqlState::UNIQUE_VIOLATION)
}

impl TenantStore for PostgresStorage {
    fn create_tenant(&self, tenant: Tenant) -> StorageResult<Tenant> {
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO tenants (id, slug, name, metadata, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING id, slug, name, metadata, created_at, updated_at
                    ",
                    &[
                        &tenant.id,
                        &tenant.slug,
                        &tenant.name,
                        &json_column(&tenant.metadata),
                        &tenant.created_at,
                        &tenant.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            Ok(row_to_tenant(row))
        })
    }

    fn get_tenant(&self, tenant_id: Uuid) -> StorageResult<Option<Tenant>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, slug, name, metadata, created_at, updated_at
                    FROM tenants
                    WHERE id = $1
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;
            Ok(row.map(row_to_tenant))
        })
    }

    fn get_tenant_by_slug(&self, slug: &str) -> StorageResult<Option<Tenant>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, slug, name, metadata, created_at, updated_at
                    FROM tenants
                    WHERE slug = $1
                    ",
                    &[&slug],
                )
                .map_err(map_postgres_error)?;
            Ok(row.map(row_to_tenant))
        })
    }
}

impl EntityStore for PostgresStorage {
    fn create_entity(&self, entity: Entity) -> StorageResult<Entity> {
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO entities (id, tenant_id, entity_key, entity_type, jsonld, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id, tenant_id, entity_key, entity_type, jsonld, created_at, updated_at
                    ",
                    &[
                        &entity.id,
                        &entity.tenant_id,
                        &entity.entity_key,
                        &entity.entity_type,
                        &json_column(&entity.jsonld),
                        &entity.created_at,
                        &entity.updated_at,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            Ok(row_to_entity(row))
        })
    }

    fn update_entity(&self, entity: Entity) -> StorageResult<Entity> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    UPDATE entities
                    SET entity_type = $4,
                        jsonld = $5,
                        updated_at = $6
                    WHERE tenant_id = $1 AND id = $2 AND entity_key = $3
                    RETURNING id, tenant_id, entity_key, entity_type, jsonld, created_at, updated_at
                    ",
                    &[
                        &entity.tenant_id,
                        &entity.id,
                        &entity.entity_key,
                        &entity.entity_type,
                        &json_column(&entity.jsonld),
                        &entity.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_entity).ok_or(StorageError::NotFound)
        })
    }

    fn get_entity(&self, tenant_id: Uuid, entity_id: Uuid) -> StorageResult<Option<Entity>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, entity_key, entity_type, jsonld, created_at, updated_at
                    FROM entities
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &entity_id],
                )
                .map_err(map_postgres_error)?;
            Ok(row.map(row_to_entity))
        })
    }

    fn get_entity_by_key(
        &self,
        tenant_id: Uuid,
        entity_key: &str,
    ) -> StorageResult<Option<Entity>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, entity_key, entity_type, jsonld, created_at, updated_at
                    FROM entities
                    WHERE tenant_id = $1 AND entity_key = $2
                    ",
                    &[&tenant_id, &entity_key],
                )
                .map_err(map_postgres_error)?;
            Ok(row.map(row_to_entity))
        })
    }

    fn list_entities(&self, tenant_id: Uuid) -> StorageResult<Vec<Entity>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, entity_key, entity_type, jsonld, created_at, updated_at
                    FROM entities
                    WHERE tenant_id = $1
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;
            let mut entities = rows.into_iter().map(row_to_entity).collect::<Vec<_>>();
            entities.sort_by(|left, right| left.entity_key.cmp(&right.entity_key));
            Ok(entities)
        })
    }
}

impl RelationshipStore for PostgresStorage {
    fn create_relationship(&self, relationship: Relationship) -> StorageResult<Relationship> {
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO entity_relationships (
                        id,
                        tenant_id,
                        source_entity_id,
                        relationship_type,
                        target_entity_id,
                        jsonld,
                        created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id, tenant_id, source_entity_id, relationship_type, target_entity_id, jsonld, created_at
                    ",
                    &[
                        &relationship.id,
                        &relationship.tenant_id,
                        &relationship.source_entity_id,
                        &relationship.relationship_type,
                        &relationship.target_entity_id,
                        &json_column(&relationship.jsonld),
                        &relationship.created_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            Ok(row_to_relationship(row))
        })
    }

    fn list_relationships(
        &self,
        tenant_id: Uuid,
        source_entity_id: Option<Uuid>,
        target_entity_id: Option<Uuid>,
    ) -> StorageResult<Vec<Relationship>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, source_entity_id, relationship_type, target_entity_id, jsonld, created_at
                    FROM entity_relationships
                    WHERE tenant_id = $1
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;

            let mut relationships = rows
                .into_iter()
                .map(row_to_relationship)
                .filter(|relationship| {
                    source_entity_id
                        .map(|id| relationship.source_entity_id == id)
                        .unwrap_or(true)
                })
                .filter(|relationship| {
                    target_entity_id
                        .map(|id| relationship.target_entity_id == id)
                        .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            relationships.sort_by(|left, right| left.created_at.cmp(&right.created_at));
            Ok(relationships)
        })
    }
}

impl RawMessageStore for PostgresStorage {
    fn store_raw_message(&self, raw_message: RawMessage) -> StorageResult<RawMessage> {
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO raw_messages (
                        id,
                        tenant_id,
                        source_type,
                        source_ref,
                        device_key,
                        decoder_hint,
                        content_type,
                        producer_entity_id,
                        feature_of_interest_id,
                        payload_format,
                        headers,
                        payload,
                        received_at,
                        normalization_status,
                        normalization_error
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    RETURNING id, tenant_id, source_type, source_ref, device_key, decoder_hint, content_type, producer_entity_id, feature_of_interest_id, payload_format, headers, payload, received_at, normalization_status, normalization_error
                    ",
                    &[
                        &raw_message.id,
                        &raw_message.tenant_id,
                        &raw_message_source_to_db(&raw_message.source_type),
                        &raw_message.source_ref,
                        &raw_message.device_key,
                        &raw_message.decoder_hint,
                        &raw_message.content_type,
                        &raw_message.producer_entity_id,
                        &raw_message.feature_of_interest_id,
                        &raw_message.payload_format,
                        &json_column(&raw_message.headers),
                        &raw_message.payload,
                        &raw_message.received_at,
                        &normalization_status_to_db(&raw_message.normalization_status),
                        &raw_message.normalization_error,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            row_to_raw_message(row)
        })
    }

    fn get_raw_message(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
    ) -> StorageResult<Option<RawMessage>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, source_type, source_ref, device_key, decoder_hint, content_type, producer_entity_id, feature_of_interest_id, payload_format, headers, payload, received_at, normalization_status, normalization_error
                    FROM raw_messages
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &raw_message_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_raw_message(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn list_raw_messages(&self, tenant_id: Uuid) -> StorageResult<Vec<RawMessage>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, source_type, source_ref, device_key, decoder_hint, content_type, producer_entity_id, feature_of_interest_id, payload_format, headers, payload, received_at, normalization_status, normalization_error
                    FROM raw_messages
                    WHERE tenant_id = $1
                    ORDER BY received_at DESC
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;
            rows.into_iter()
                .map(row_to_raw_message)
                .collect::<StorageResult<Vec<_>>>()
        })
    }

    fn query_raw_messages(
        &self,
        tenant_id: Uuid,
        producer_entity_id: Option<Uuid>,
        feature_of_interest_id: Option<Uuid>,
        payload_format: Option<&str>,
    ) -> StorageResult<Vec<RawMessage>> {
        self.with_client(|client| {
            let mut sql = String::from(
                "
                SELECT id, tenant_id, source_type, source_ref, device_key, decoder_hint, content_type, producer_entity_id, feature_of_interest_id, payload_format, headers, payload, received_at, normalization_status, normalization_error
                FROM raw_messages
                WHERE tenant_id = $1
                ",
            );
            let producer_entity_id = producer_entity_id;
            let feature_of_interest_id = feature_of_interest_id;
            let payload_format = payload_format.map(|value| value.to_string());
            let mut params: Vec<&(dyn ToSql + Sync)> = vec![&tenant_id];
            let mut next_index = 2;

            if let Some(producer_entity_id) = producer_entity_id.as_ref() {
                sql.push_str(&format!(" AND producer_entity_id = ${next_index}"));
                params.push(producer_entity_id);
                next_index += 1;
            }

            if let Some(feature_of_interest_id) = feature_of_interest_id.as_ref() {
                sql.push_str(&format!(" AND feature_of_interest_id = ${next_index}"));
                params.push(feature_of_interest_id);
                next_index += 1;
            }

            if let Some(payload_format) = payload_format.as_ref() {
                sql.push_str(&format!(" AND payload_format = ${next_index}"));
                params.push(payload_format);
            }

            sql.push_str(" ORDER BY received_at DESC");
            let rows = client.query(&sql, &params).map_err(map_postgres_error)?;
            rows.into_iter()
                .map(row_to_raw_message)
                .collect::<StorageResult<Vec<_>>>()
        })
    }

    fn mark_raw_message_normalized(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
    ) -> StorageResult<()> {
        self.with_client(|client| {
            let updated = client
                .execute(
                    "
                    UPDATE raw_messages
                    SET normalization_status = $3,
                        normalization_error = NULL
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[
                        &tenant_id,
                        &raw_message_id,
                        &normalization_status_to_db(&NormalizationStatus::Normalized),
                    ],
                )
                .map_err(map_postgres_error)?;
            if updated == 0 {
                return Err(StorageError::NotFound);
            }
            Ok(())
        })
    }

    fn mark_raw_message_failed(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
        error: &str,
    ) -> StorageResult<()> {
        self.with_client(|client| {
            let updated = client
                .execute(
                    "
                    UPDATE raw_messages
                    SET normalization_status = $3,
                        normalization_error = $4
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[
                        &tenant_id,
                        &raw_message_id,
                        &normalization_status_to_db(&NormalizationStatus::Failed),
                        &error,
                    ],
                )
                .map_err(map_postgres_error)?;
            if updated == 0 {
                return Err(StorageError::NotFound);
            }
            Ok(())
        })
    }
}

impl ObservationStore for PostgresStorage {
    fn store_observation(&self, observation: Observation) -> StorageResult<Observation> {
        self.with_client(|client| {
            let exists = client
                .query_opt(
                    "
                    SELECT 1
                    FROM observations
                    WHERE tenant_id = $1 AND id = $2
                    LIMIT 1
                    ",
                    &[&observation.tenant_id, &observation.id],
                )
                .map_err(map_postgres_error)?;
            if exists.is_some() {
                return Err(StorageError::Conflict);
            }

            let (value_number, value_string, value_bool, value_json) =
                observation_value_to_columns(&observation.value);
            let row = client
                .query_one(
                    "
                    INSERT INTO observations (
                        id,
                        tenant_id,
                        producer_entity_id,
                        feature_of_interest_id,
                        observed_property,
                        value_number,
                        value_string,
                        value_bool,
                        value_json,
                        unit,
                        observed_at,
                        received_at,
                        protocol,
                        payload_format,
                        raw_message_id,
                        quality,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                    RETURNING id, tenant_id, producer_entity_id, feature_of_interest_id, observed_property, value_number, value_string, value_bool, value_json, unit, observed_at, received_at, protocol, payload_format, raw_message_id, quality, metadata
                    ",
                    &[
                        &observation.id,
                        &observation.tenant_id,
                        &observation.producer_entity_id,
                        &observation.feature_of_interest_id,
                        &observation.observed_property,
                        &value_number,
                        &value_string,
                        &value_bool,
                        &value_json,
                        &observation.unit,
                        &observation.observed_at,
                        &observation.received_at,
                        &observation.protocol,
                        &observation.payload_format,
                        &observation.raw_message_id,
                        &json_column(&observation.quality),
                        &json_column(&observation.metadata),
                    ],
                )
                .map_err(map_postgres_error)?;
            row_to_observation(row)
        })
    }

    fn get_observation(
        &self,
        tenant_id: Uuid,
        observation_id: Uuid,
    ) -> StorageResult<Option<Observation>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, producer_entity_id, feature_of_interest_id, observed_property, value_number, value_string, value_bool, value_json, unit, observed_at, received_at, protocol, payload_format, raw_message_id, quality, metadata
                    FROM observations
                    WHERE tenant_id = $1 AND id = $2
                    ORDER BY observed_at DESC
                    LIMIT 1
                    ",
                    &[&tenant_id, &observation_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_observation(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn query_observations(
        &self,
        tenant_id: Uuid,
        feature_of_interest_id: Option<Uuid>,
        observed_property: Option<&str>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: u32,
    ) -> StorageResult<Vec<Observation>> {
        self.with_client(|client| {
            let mut sql = String::from(
                "
                SELECT id, tenant_id, producer_entity_id, feature_of_interest_id, observed_property, value_number, value_string, value_bool, value_json, unit, observed_at, received_at, protocol, payload_format, raw_message_id, quality, metadata
                FROM observations
                WHERE tenant_id = $1
                ",
            );
            let feature_of_interest_id = feature_of_interest_id;
            let observed_property = observed_property.map(|value| value.to_string());
            let from = from;
            let to = to;
            let mut params: Vec<&(dyn ToSql + Sync)> = vec![&tenant_id];
            let mut next_index = 2;

            if let Some(feature_of_interest_id) = feature_of_interest_id.as_ref() {
                sql.push_str(&format!(" AND feature_of_interest_id = ${next_index}"));
                params.push(feature_of_interest_id);
                next_index += 1;
            }

            if let Some(observed_property) = observed_property.as_ref() {
                sql.push_str(&format!(" AND observed_property = ${next_index}"));
                params.push(observed_property);
                next_index += 1;
            }

            if let Some(from) = from.as_ref() {
                sql.push_str(&format!(" AND observed_at >= ${next_index}"));
                params.push(from);
                next_index += 1;
            }

            if let Some(to) = to.as_ref() {
                sql.push_str(&format!(" AND observed_at <= ${next_index}"));
                params.push(to);
                next_index += 1;
            }

            let limit = limit as i64;
            sql.push_str(&format!(" ORDER BY observed_at DESC LIMIT ${next_index}"));
            params.push(&limit);

            let rows = client.query(&sql, &params).map_err(map_postgres_error)?;
            rows.into_iter()
                .map(row_to_observation)
                .collect::<StorageResult<Vec<_>>>()
        })
    }
}

impl EventStore for PostgresStorage {
    fn store_event(&self, event: Event) -> StorageResult<Event> {
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO events (
                        id,
                        tenant_id,
                        event_type,
                        severity,
                        source_entity_id,
                        target_entity_id,
                        message,
                        occurred_at,
                        observed_at,
                        correlation_id,
                        raw_message_id,
                        observation_id,
                        command_id,
                        action_id,
                        action_result_id,
                        metadata,
                        created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                    RETURNING id, tenant_id, event_type, severity, source_entity_id, target_entity_id, message, occurred_at, observed_at, correlation_id, raw_message_id, observation_id, command_id, action_id, action_result_id, metadata, created_at
                    ",
                    &[
                        &event.id,
                        &event.tenant_id,
                        &event.event_type,
                        &event_severity_to_db(&event.severity),
                        &event.source_entity_id,
                        &event.target_entity_id,
                        &event.message,
                        &event.occurred_at,
                        &event.observed_at,
                        &event.correlation_id,
                        &event.raw_message_id,
                        &event.observation_id,
                        &event.command_id,
                        &event.action_id,
                        &event.action_result_id,
                        &json_option_column(event.metadata.as_ref()),
                        &event.created_at,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            row_to_event(row)
        })
    }

    fn get_event(&self, tenant_id: Uuid, event_id: Uuid) -> StorageResult<Option<Event>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, event_type, severity, source_entity_id, target_entity_id, message, occurred_at, observed_at, correlation_id, raw_message_id, observation_id, command_id, action_id, action_result_id, metadata, created_at
                    FROM events
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &event_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_event(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn query_events(&self, tenant_id: Uuid, filter: EventFilter) -> StorageResult<Vec<Event>> {
        self.with_client(|client| {
            let mut sql = String::from(
                "
                SELECT id, tenant_id, event_type, severity, source_entity_id, target_entity_id, message, occurred_at, observed_at, correlation_id, raw_message_id, observation_id, command_id, action_id, action_result_id, metadata, created_at
                FROM events
                WHERE tenant_id = $1
                ",
            );
            let source_entity_id = filter.source_entity_id;
            let target_entity_id = filter.target_entity_id;
            let event_type = filter.event_type;
            let severity = filter.severity.as_ref().map(event_severity_to_db);
            let command_id = filter.command_id;
            let raw_message_id = filter.raw_message_id;
            let correlation_id = filter.correlation_id;
            let mut params: Vec<&(dyn ToSql + Sync)> = vec![&tenant_id];
            let mut next_index = 2;

            if let Some(source_entity_id) = source_entity_id.as_ref() {
                sql.push_str(&format!(" AND source_entity_id = ${next_index}"));
                params.push(source_entity_id);
                next_index += 1;
            }

            if let Some(target_entity_id) = target_entity_id.as_ref() {
                sql.push_str(&format!(" AND target_entity_id = ${next_index}"));
                params.push(target_entity_id);
                next_index += 1;
            }

            if let Some(event_type) = event_type.as_ref() {
                sql.push_str(&format!(" AND event_type = ${next_index}"));
                params.push(event_type);
                next_index += 1;
            }

            if let Some(severity) = severity.as_ref() {
                sql.push_str(&format!(" AND severity = ${next_index}"));
                params.push(severity);
                next_index += 1;
            }

            if let Some(command_id) = command_id.as_ref() {
                sql.push_str(&format!(" AND command_id = ${next_index}"));
                params.push(command_id);
                next_index += 1;
            }

            if let Some(raw_message_id) = raw_message_id.as_ref() {
                sql.push_str(&format!(" AND raw_message_id = ${next_index}"));
                params.push(raw_message_id);
                next_index += 1;
            }

            if let Some(correlation_id) = correlation_id.as_ref() {
                sql.push_str(&format!(" AND correlation_id = ${next_index}"));
                params.push(correlation_id);
            }

            sql.push_str(" ORDER BY occurred_at DESC");
            let rows = client.query(&sql, &params).map_err(map_postgres_error)?;
            rows.into_iter()
                .map(row_to_event)
                .collect::<StorageResult<Vec<_>>>()
        })
    }
}

impl CommandStore for PostgresStorage {
    fn store_command(&self, command: Command) -> StorageResult<Command> {
        self.with_client(|client| {
            let approval_status = command.approval_status.as_ref().map(approval_status_to_db);
            let policy_decision = json_option_column(command.policy_decision.as_ref());
            let retry_count = command.retry_count as i32;
            let max_retries = command.max_retries.map(|value| value as i32);
            let row = client
                .query_one(
                    "
                    INSERT INTO commands (
                        id,
                        tenant_id,
                        target_entity_id,
                        command_type,
                        payload,
                        status,
                        requested_by,
                        reason,
                        claimed_by,
                        claimed_at,
                        completed_at,
                        failure_reason,
                        approval_status,
                        policy_decision,
                        retry_count,
                        max_retries,
                        lease_expires_at,
                        last_claimed_by,
                        last_failure_reason,
                        created_at,
                        updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                    RETURNING id, tenant_id, target_entity_id, command_type, payload, status, requested_by, reason, claimed_by, claimed_at, completed_at, failure_reason, approval_status, policy_decision, retry_count, max_retries, lease_expires_at, last_claimed_by, last_failure_reason, created_at, updated_at
                    ",
                    &[
                        &command.id,
                        &command.tenant_id,
                        &command.target_entity_id,
                        &command.command_type,
                        &command.payload,
                        &command_status_to_db(&command.status),
                        &command.requested_by,
                        &command.reason,
                        &command.claimed_by,
                        &command.claimed_at,
                        &command.completed_at,
                        &command.failure_reason,
                        &approval_status,
                        &policy_decision,
                        &retry_count,
                        &max_retries,
                        &command.lease_expires_at,
                        &command.last_claimed_by,
                        &command.last_failure_reason,
                        &command.created_at,
                        &command.updated_at,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            row_to_command(row)
        })
    }

    fn update_command(&self, command: Command) -> StorageResult<Command> {
        self.with_client(|client| {
            let approval_status = command.approval_status.as_ref().map(approval_status_to_db);
            let policy_decision = json_option_column(command.policy_decision.as_ref());
            let retry_count = command.retry_count as i32;
            let max_retries = command.max_retries.map(|value| value as i32);
            let row = client
                .query_opt(
                    "
                    UPDATE commands
                    SET target_entity_id = $3,
                        command_type = $4,
                        payload = $5,
                        status = $6,
                        requested_by = $7,
                        reason = $8,
                        claimed_by = $9,
                        claimed_at = $10,
                        completed_at = $11,
                        failure_reason = $12,
                        approval_status = $13,
                        policy_decision = $14,
                        retry_count = $15,
                        max_retries = $16,
                        lease_expires_at = $17,
                        last_claimed_by = $18,
                        last_failure_reason = $19,
                        updated_at = $20
                    WHERE tenant_id = $1 AND id = $2
                    RETURNING id, tenant_id, target_entity_id, command_type, payload, status, requested_by, reason, claimed_by, claimed_at, completed_at, failure_reason, approval_status, policy_decision, retry_count, max_retries, lease_expires_at, last_claimed_by, last_failure_reason, created_at, updated_at
                    ",
                    &[
                        &command.tenant_id,
                        &command.id,
                        &command.target_entity_id,
                        &command.command_type,
                        &command.payload,
                        &command_status_to_db(&command.status),
                        &command.requested_by,
                        &command.reason,
                        &command.claimed_by,
                        &command.claimed_at,
                        &command.completed_at,
                        &command.failure_reason,
                        &approval_status,
                        &policy_decision,
                        &retry_count,
                        &max_retries,
                        &command.lease_expires_at,
                        &command.last_claimed_by,
                        &command.last_failure_reason,
                        &command.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_command).transpose()?.ok_or(StorageError::NotFound)
        })
    }

    fn get_command(&self, tenant_id: Uuid, command_id: Uuid) -> StorageResult<Option<Command>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, target_entity_id, command_type, payload, status, requested_by, reason, claimed_by, claimed_at, completed_at, failure_reason, approval_status, policy_decision, retry_count, max_retries, lease_expires_at, last_claimed_by, last_failure_reason, created_at, updated_at
                    FROM commands
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &command_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_command(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn query_commands(
        &self,
        tenant_id: Uuid,
        target_entity_id: Option<Uuid>,
        status: Option<CommandStatus>,
    ) -> StorageResult<Vec<Command>> {
        self.with_client(|client| {
            let mut sql = String::from(
                "
                SELECT id, tenant_id, target_entity_id, command_type, payload, status, requested_by, reason, claimed_by, claimed_at, completed_at, failure_reason, approval_status, policy_decision, retry_count, max_retries, lease_expires_at, last_claimed_by, last_failure_reason, created_at, updated_at
                FROM commands
                WHERE tenant_id = $1
                ",
            );
            let target_entity_id = target_entity_id;
            let status = status.map(|status| command_status_to_db(&status));
            let mut params: Vec<&(dyn ToSql + Sync)> = vec![&tenant_id];
            let mut next_index = 2;

            if let Some(target_entity_id) = target_entity_id.as_ref() {
                sql.push_str(&format!(" AND target_entity_id = ${next_index}"));
                params.push(target_entity_id);
                next_index += 1;
            }

            if let Some(status) = status.as_ref() {
                sql.push_str(&format!(" AND status = ${next_index}"));
                params.push(status);
            }

            sql.push_str(" ORDER BY created_at DESC");
            let rows = client.query(&sql, &params).map_err(map_postgres_error)?;
            rows.into_iter()
                .map(row_to_command)
                .collect::<StorageResult<Vec<_>>>()
        })
    }
}

impl CommandLeaseStore for PostgresStorage {
    fn store_command_lease(&self, lease: CommandLease) -> StorageResult<CommandLease> {
        self.with_client(|client| {
            let metadata = json_option_column(lease.metadata.as_ref());
            let row = client
                .query_one(
                    "
                    INSERT INTO command_leases (
                        id,
                        tenant_id,
                        command_id,
                        executor_id,
                        lease_status,
                        claimed_at,
                        expires_at,
                        released_at,
                        completed_at,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    RETURNING id, tenant_id, command_id, executor_id, lease_status, claimed_at, expires_at, released_at, completed_at, metadata
                    ",
                    &[
                        &lease.id,
                        &lease.tenant_id,
                        &lease.command_id,
                        &lease.executor_id,
                        &command_lease_status_to_db(&lease.lease_status),
                        &lease.claimed_at,
                        &lease.expires_at,
                        &lease.released_at,
                        &lease.completed_at,
                        &metadata,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            row_to_command_lease(row)
        })
    }

    fn update_command_lease(&self, lease: CommandLease) -> StorageResult<CommandLease> {
        self.with_client(|client| {
            let metadata = json_option_column(lease.metadata.as_ref());
            let row = client
                .query_opt(
                    "
                    UPDATE command_leases
                    SET command_id = $3,
                        executor_id = $4,
                        lease_status = $5,
                        claimed_at = $6,
                        expires_at = $7,
                        released_at = $8,
                        completed_at = $9,
                        metadata = $10
                    WHERE tenant_id = $1 AND id = $2
                    RETURNING id, tenant_id, command_id, executor_id, lease_status, claimed_at, expires_at, released_at, completed_at, metadata
                    ",
                    &[
                        &lease.tenant_id,
                        &lease.id,
                        &lease.command_id,
                        &lease.executor_id,
                        &command_lease_status_to_db(&lease.lease_status),
                        &lease.claimed_at,
                        &lease.expires_at,
                        &lease.released_at,
                        &lease.completed_at,
                        &metadata,
                    ],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_command_lease).transpose()?.ok_or(StorageError::NotFound)
        })
    }

    fn get_command_lease(
        &self,
        tenant_id: Uuid,
        lease_id: Uuid,
    ) -> StorageResult<Option<CommandLease>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, command_id, executor_id, lease_status, claimed_at, expires_at, released_at, completed_at, metadata
                    FROM command_leases
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &lease_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_command_lease(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn get_active_command_lease(
        &self,
        tenant_id: Uuid,
        command_id: Uuid,
    ) -> StorageResult<Option<CommandLease>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, command_id, executor_id, lease_status, claimed_at, expires_at, released_at, completed_at, metadata
                    FROM command_leases
                    WHERE tenant_id = $1 AND command_id = $2 AND lease_status = 'active'
                    ORDER BY claimed_at DESC
                    LIMIT 1
                    ",
                    &[&tenant_id, &command_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_command_lease(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn get_latest_command_lease(
        &self,
        tenant_id: Uuid,
        command_id: Uuid,
    ) -> StorageResult<Option<CommandLease>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, command_id, executor_id, lease_status, claimed_at, expires_at, released_at, completed_at, metadata
                    FROM command_leases
                    WHERE tenant_id = $1 AND command_id = $2
                    ORDER BY claimed_at DESC
                    LIMIT 1
                    ",
                    &[&tenant_id, &command_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_command_lease(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn list_active_command_leases(&self, tenant_id: Uuid) -> StorageResult<Vec<CommandLease>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, command_id, executor_id, lease_status, claimed_at, expires_at, released_at, completed_at, metadata
                    FROM command_leases
                    WHERE tenant_id = $1 AND lease_status = 'active'
                    ORDER BY expires_at ASC
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;
            rows.into_iter()
                .map(row_to_command_lease)
                .collect::<StorageResult<Vec<_>>>()
        })
    }
}

impl ActionStore for PostgresStorage {
    fn store_action(&self, action: Action) -> StorageResult<Action> {
        self.with_client(|client| {
            let metadata = json_option_column(action.metadata.as_ref());
            let row = client
                .query_one(
                    "
                    INSERT INTO actions (
                        id,
                        tenant_id,
                        command_id,
                        executor_entity_id,
                        action_type,
                        status,
                        started_at,
                        finished_at,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id, tenant_id, command_id, executor_entity_id, action_type, status, started_at, finished_at, metadata
                    ",
                    &[
                        &action.id,
                        &action.tenant_id,
                        &action.command_id,
                        &action.executor_entity_id,
                        &action.action_type,
                        &action.status,
                        &action.started_at,
                        &action.finished_at,
                        &metadata,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            Ok(row_to_action(row))
        })
    }

    fn get_action(&self, tenant_id: Uuid, action_id: Uuid) -> StorageResult<Option<Action>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, command_id, executor_entity_id, action_type, status, started_at, finished_at, metadata
                    FROM actions
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &action_id],
                )
                .map_err(map_postgres_error)?;
            Ok(row.map(row_to_action))
        })
    }

    fn query_actions(
        &self,
        tenant_id: Uuid,
        command_id: Option<Uuid>,
    ) -> StorageResult<Vec<Action>> {
        self.with_client(|client| {
            let mut sql = String::from(
                "
                SELECT id, tenant_id, command_id, executor_entity_id, action_type, status, started_at, finished_at, metadata
                FROM actions
                WHERE tenant_id = $1
                ",
            );
            let command_id = command_id;
            let mut params: Vec<&(dyn ToSql + Sync)> = vec![&tenant_id];
            if let Some(command_id) = command_id.as_ref() {
                sql.push_str(" AND command_id = $2");
                params.push(command_id);
            }
            sql.push_str(" ORDER BY started_at ASC NULLS FIRST, id ASC");
            let rows = client.query(&sql, &params).map_err(map_postgres_error)?;
            Ok(rows.into_iter().map(row_to_action).collect())
        })
    }
}

impl ActionResultStore for PostgresStorage {
    fn store_action_result(&self, result: ActionResult) -> StorageResult<ActionResult> {
        self.with_client(|client| {
            let metadata = json_option_column(result.metadata.as_ref());
            let row = client
                .query_one(
                    "
                    INSERT INTO action_results (
                        id,
                        tenant_id,
                        command_id,
                        action_id,
                        status,
                        verified,
                        result_payload,
                        observed_at,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id, tenant_id, command_id, action_id, status, verified, result_payload, observed_at, metadata
                    ",
                    &[
                        &result.id,
                        &result.tenant_id,
                        &result.command_id,
                        &result.action_id,
                        &result.status,
                        &result.verified,
                        &result.result_payload,
                        &result.observed_at,
                        &metadata,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            Ok(row_to_action_result(row))
        })
    }

    fn query_action_results(
        &self,
        tenant_id: Uuid,
        action_id: Option<Uuid>,
        command_id: Option<Uuid>,
    ) -> StorageResult<Vec<ActionResult>> {
        self.with_client(|client| {
            let mut sql = String::from(
                "
                SELECT id, tenant_id, command_id, action_id, status, verified, result_payload, observed_at, metadata
                FROM action_results
                WHERE tenant_id = $1
                ",
            );
            let action_id = action_id;
            let command_id = command_id;
            let mut params: Vec<&(dyn ToSql + Sync)> = vec![&tenant_id];
            let mut next_index = 2;

            if let Some(action_id) = action_id.as_ref() {
                sql.push_str(&format!(" AND action_id = ${next_index}"));
                params.push(action_id);
                next_index += 1;
            }

            if let Some(command_id) = command_id.as_ref() {
                sql.push_str(&format!(" AND command_id = ${next_index}"));
                params.push(command_id);
            }

            sql.push_str(" ORDER BY observed_at DESC");
            let rows = client.query(&sql, &params).map_err(map_postgres_error)?;
            Ok(rows.into_iter().map(row_to_action_result).collect())
        })
    }
}

impl RuleStore for PostgresStorage {
    fn store_rule(&self, rule: Rule) -> StorageResult<Rule> {
        self.with_client(|client| {
            let condition = json_serializable(&rule.condition)?;
            let action = json_serializable(&rule.action)?;
            let metadata = json_option_column(rule.metadata.as_ref());
            let row = client
                .query_one(
                    "
                    INSERT INTO rules (
                        id,
                        tenant_id,
                        name,
                        description,
                        enabled,
                        trigger_type,
                        target_entity_id,
                        observed_property,
                        event_type,
                        condition,
                        action,
                        metadata,
                        created_at,
                        updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    RETURNING id, tenant_id, name, description, enabled, trigger_type, target_entity_id, observed_property, event_type, condition, action, metadata, created_at, updated_at
                    ",
                    &[
                        &rule.id,
                        &rule.tenant_id,
                        &rule.name,
                        &rule.description,
                        &rule.enabled,
                        &rule_trigger_type_to_db(&rule.trigger_type),
                        &rule.target_entity_id,
                        &rule.observed_property,
                        &rule.event_type,
                        &condition,
                        &action,
                        &metadata,
                        &rule.created_at,
                        &rule.updated_at,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            row_to_rule(row)
        })
    }

    fn update_rule(&self, rule: Rule) -> StorageResult<Rule> {
        self.with_client(|client| {
            let condition = json_serializable(&rule.condition)?;
            let action = json_serializable(&rule.action)?;
            let metadata = json_option_column(rule.metadata.as_ref());
            let row = client
                .query_opt(
                    "
                    UPDATE rules
                    SET name = $3,
                        description = $4,
                        enabled = $5,
                        trigger_type = $6,
                        target_entity_id = $7,
                        observed_property = $8,
                        event_type = $9,
                        condition = $10,
                        action = $11,
                        metadata = $12,
                        updated_at = $13
                    WHERE tenant_id = $1 AND id = $2
                    RETURNING id, tenant_id, name, description, enabled, trigger_type, target_entity_id, observed_property, event_type, condition, action, metadata, created_at, updated_at
                    ",
                    &[
                        &rule.tenant_id,
                        &rule.id,
                        &rule.name,
                        &rule.description,
                        &rule.enabled,
                        &rule_trigger_type_to_db(&rule.trigger_type),
                        &rule.target_entity_id,
                        &rule.observed_property,
                        &rule.event_type,
                        &condition,
                        &action,
                        &metadata,
                        &rule.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_rule).transpose()?.ok_or(StorageError::NotFound)
        })
    }

    fn get_rule(&self, tenant_id: Uuid, rule_id: Uuid) -> StorageResult<Option<Rule>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, name, description, enabled, trigger_type, target_entity_id, observed_property, event_type, condition, action, metadata, created_at, updated_at
                    FROM rules
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &rule_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_rule(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn list_rules(&self, tenant_id: Uuid) -> StorageResult<Vec<Rule>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, name, description, enabled, trigger_type, target_entity_id, observed_property, event_type, condition, action, metadata, created_at, updated_at
                    FROM rules
                    WHERE tenant_id = $1
                    ORDER BY created_at ASC
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;
            rows.into_iter()
                .map(row_to_rule)
                .collect::<StorageResult<Vec<_>>>()
        })
    }
}

impl PayloadProfileStore for PostgresStorage {
    fn put_payload_profile(
        &self,
        tenant_id: Uuid,
        profile: PayloadProfile,
    ) -> StorageResult<PayloadProfile> {
        if profile.entity_id == Uuid::nil() {
            return Err(StorageError::InvalidInput(
                "entity_id must not be nil".to_string(),
            ));
        }
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO payload_profiles (
                        tenant_id,
                        entity_id,
                        payload_format,
                        protocol,
                        content_type,
                        attribute_mapping,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (tenant_id, entity_id) DO UPDATE SET
                        payload_format = EXCLUDED.payload_format,
                        protocol = EXCLUDED.protocol,
                        content_type = EXCLUDED.content_type,
                        attribute_mapping = EXCLUDED.attribute_mapping,
                        metadata = EXCLUDED.metadata
                    RETURNING tenant_id, entity_id, payload_format, protocol, content_type, attribute_mapping, metadata
                    ",
                    &[
                        &tenant_id,
                        &profile.entity_id,
                        &profile.payload_format,
                        &profile.protocol,
                        &profile.content_type,
                        &json_option_column(profile.attribute_mapping.as_ref()),
                        &json_option_column(profile.metadata.as_ref()),
                    ],
                )
                .map_err(map_postgres_error)?;
            Ok(row_to_payload_profile(row))
        })
    }

    fn get_payload_profile(
        &self,
        tenant_id: Uuid,
        entity_id: Uuid,
    ) -> StorageResult<Option<PayloadProfile>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT tenant_id, entity_id, payload_format, protocol, content_type, attribute_mapping, metadata
                    FROM payload_profiles
                    WHERE tenant_id = $1 AND entity_id = $2
                    ",
                    &[&tenant_id, &entity_id],
                )
                .map_err(map_postgres_error)?;
            Ok(row.map(row_to_payload_profile))
        })
    }
}

impl IngestionConnectorStore for PostgresStorage {
    fn create_ingestion_connector(
        &self,
        connector: IngestionConnector,
    ) -> StorageResult<IngestionConnector> {
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO ingestion_connectors (
                        id,
                        tenant_id,
                        connector_key,
                        connector_type,
                        connector_profile,
                        enabled,
                        display_name,
                        protocol,
                        endpoint,
                        broker_url,
                        client_id,
                        topic_filter,
                        http_path,
                        payload_format,
                        content_type,
                        secret_ref_id,
                        default_producer_entity_id,
                        default_feature_of_interest_id,
                        metadata,
                        created_at,
                        updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
                    )
                    RETURNING id, tenant_id, connector_key, connector_type, connector_profile,
                        enabled, display_name, protocol, endpoint, broker_url, client_id,
                        topic_filter, http_path, payload_format, content_type,
                        secret_ref_id, default_producer_entity_id, default_feature_of_interest_id, metadata,
                        created_at, updated_at
                    ",
                    &[
                        &connector.id,
                        &connector.tenant_id,
                        &connector.connector_key,
                        &ingestion_connector_type_to_db(&connector.connector_type),
                        &connector_profile_to_db(&connector.connector_profile),
                        &connector.enabled,
                        &connector.display_name,
                        &connector.protocol,
                        &connector.endpoint,
                        &connector.broker_url,
                        &connector.client_id,
                        &connector.topic_filter,
                        &connector.http_path,
                        &connector.payload_format,
                        &connector.content_type,
                        &connector.secret_ref_id,
                        &connector.default_producer_entity_id,
                        &connector.default_feature_of_interest_id,
                        &json_option_column(connector.metadata.as_ref()),
                        &connector.created_at,
                        &connector.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            row_to_ingestion_connector(row)
        })
    }

    fn get_ingestion_connector(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
    ) -> StorageResult<Option<IngestionConnector>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, connector_key, connector_type, connector_profile,
                        enabled, display_name, protocol, endpoint, broker_url, client_id,
                        topic_filter, http_path, payload_format, content_type,
                        secret_ref_id, default_producer_entity_id, default_feature_of_interest_id, metadata,
                        created_at, updated_at
                    FROM ingestion_connectors
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &connector_id],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_ingestion_connector).transpose()
        })
    }

    fn list_ingestion_connectors(&self, tenant_id: Uuid) -> StorageResult<Vec<IngestionConnector>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, connector_key, connector_type, connector_profile,
                        enabled, display_name, protocol, endpoint, broker_url, client_id,
                        topic_filter, http_path, payload_format, content_type,
                        secret_ref_id, default_producer_entity_id, default_feature_of_interest_id, metadata,
                        created_at, updated_at
                    FROM ingestion_connectors
                    WHERE tenant_id = $1
                    ORDER BY connector_key ASC
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;
            rows.into_iter()
                .map(row_to_ingestion_connector)
                .collect::<StorageResult<Vec<_>>>()
        })
    }

    fn update_ingestion_connector(
        &self,
        connector: IngestionConnector,
    ) -> StorageResult<IngestionConnector> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    UPDATE ingestion_connectors SET
                        connector_key = $3,
                        connector_type = $4,
                        connector_profile = $5,
                        enabled = $6,
                        display_name = $7,
                        protocol = $8,
                        endpoint = $9,
                        broker_url = $10,
                        client_id = $11,
                        topic_filter = $12,
                        http_path = $13,
                        payload_format = $14,
                        content_type = $15,
                        secret_ref_id = $16,
                        default_producer_entity_id = $17,
                        default_feature_of_interest_id = $18,
                        metadata = $19,
                        created_at = $20,
                        updated_at = $21
                    WHERE tenant_id = $1 AND id = $2
                    RETURNING id, tenant_id, connector_key, connector_type, connector_profile,
                        enabled, display_name, protocol, endpoint, broker_url, client_id,
                        topic_filter, http_path, payload_format, content_type,
                        secret_ref_id, default_producer_entity_id, default_feature_of_interest_id, metadata,
                        created_at, updated_at
                    ",
                    &[
                        &connector.tenant_id,
                        &connector.id,
                        &connector.connector_key,
                        &ingestion_connector_type_to_db(&connector.connector_type),
                        &connector_profile_to_db(&connector.connector_profile),
                        &connector.enabled,
                        &connector.display_name,
                        &connector.protocol,
                        &connector.endpoint,
                        &connector.broker_url,
                        &connector.client_id,
                        &connector.topic_filter,
                        &connector.http_path,
                        &connector.payload_format,
                        &connector.content_type,
                        &connector.secret_ref_id,
                        &connector.default_producer_entity_id,
                        &connector.default_feature_of_interest_id,
                        &json_option_column(connector.metadata.as_ref()),
                        &connector.created_at,
                        &connector.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_ingestion_connector)
                .transpose()?
                .ok_or(StorageError::NotFound)
        })
    }
}

impl ConnectorSecretStore for PostgresStorage {
    fn create_connector_secret(&self, secret: ConnectorSecret) -> StorageResult<ConnectorSecret> {
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO connector_secrets (
                        id, tenant_id, secret_key, secret_type, username, secret_value, metadata,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    RETURNING id, tenant_id, secret_key, secret_type, username, secret_value,
                        metadata, created_at, updated_at
                    ",
                    &[
                        &secret.id,
                        &secret.tenant_id,
                        &secret.secret_key,
                        &connector_secret_type_to_db(&secret.secret_type),
                        &secret.username,
                        &secret.secret_value,
                        &json_option_column(secret.metadata.as_ref()),
                        &secret.created_at,
                        &secret.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            row_to_connector_secret(row)
        })
    }

    fn get_connector_secret(
        &self,
        tenant_id: Uuid,
        secret_id: Uuid,
    ) -> StorageResult<Option<ConnectorSecret>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, secret_key, secret_type, username, secret_value,
                        metadata, created_at, updated_at
                    FROM connector_secrets
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &secret_id],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_connector_secret).transpose()
        })
    }

    fn list_connector_secrets(&self, tenant_id: Uuid) -> StorageResult<Vec<ConnectorSecret>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, secret_key, secret_type, username, secret_value,
                        metadata, created_at, updated_at
                    FROM connector_secrets
                    WHERE tenant_id = $1
                    ORDER BY secret_key ASC
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;
            rows.into_iter()
                .map(row_to_connector_secret)
                .collect::<StorageResult<Vec<_>>>()
        })
    }

    fn delete_connector_secret(&self, tenant_id: Uuid, secret_id: Uuid) -> StorageResult<()> {
        self.with_client(|client| {
            let deleted = client
                .execute(
                    "DELETE FROM connector_secrets WHERE tenant_id = $1 AND id = $2",
                    &[&tenant_id, &secret_id],
                )
                .map_err(map_postgres_error)?;
            if deleted == 0 {
                return Err(StorageError::NotFound);
            }
            Ok(())
        })
    }
}

impl TtnDeviceMappingStore for PostgresStorage {
    fn create_ttn_device_mapping(
        &self,
        mapping: TtnDeviceMapping,
    ) -> StorageResult<TtnDeviceMapping> {
        self.with_client(|client| {
            validate_ttn_device_mapping_conflict_postgres(client, &mapping)?;
            let row = client
                .query_one(
                    "
                    INSERT INTO ttn_device_mappings (
                        id, tenant_id, connector_id, ttn_application_id, ttn_device_id,
                        producer_entity_id, feature_of_interest_id, enabled, metadata,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    RETURNING id, tenant_id, connector_id, ttn_application_id, ttn_device_id,
                        producer_entity_id, feature_of_interest_id, enabled, metadata,
                        created_at, updated_at
                    ",
                    &[
                        &mapping.id,
                        &mapping.tenant_id,
                        &mapping.connector_id,
                        &mapping.ttn_application_id,
                        &mapping.ttn_device_id,
                        &mapping.producer_entity_id,
                        &mapping.feature_of_interest_id,
                        &mapping.enabled,
                        &json_option_column(mapping.metadata.as_ref()),
                        &mapping.created_at,
                        &mapping.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            Ok(row_to_ttn_device_mapping(row))
        })
    }

    fn get_ttn_device_mapping(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
        mapping_id: Uuid,
    ) -> StorageResult<Option<TtnDeviceMapping>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, connector_id, ttn_application_id, ttn_device_id,
                        producer_entity_id, feature_of_interest_id, enabled, metadata,
                        created_at, updated_at
                    FROM ttn_device_mappings
                    WHERE tenant_id = $1 AND connector_id = $2 AND id = $3
                    ",
                    &[&tenant_id, &connector_id, &mapping_id],
                )
                .map_err(map_postgres_error)?;
            Ok(row.map(row_to_ttn_device_mapping))
        })
    }

    fn list_ttn_device_mappings(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
    ) -> StorageResult<Vec<TtnDeviceMapping>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, connector_id, ttn_application_id, ttn_device_id,
                        producer_entity_id, feature_of_interest_id, enabled, metadata,
                        created_at, updated_at
                    FROM ttn_device_mappings
                    WHERE tenant_id = $1 AND connector_id = $2
                    ORDER BY ttn_device_id ASC, ttn_application_id ASC NULLS LAST
                    ",
                    &[&tenant_id, &connector_id],
                )
                .map_err(map_postgres_error)?;
            Ok(rows.into_iter().map(row_to_ttn_device_mapping).collect())
        })
    }

    fn update_ttn_device_mapping(
        &self,
        mapping: TtnDeviceMapping,
    ) -> StorageResult<TtnDeviceMapping> {
        self.with_client(|client| {
            validate_ttn_device_mapping_conflict_postgres(client, &mapping)?;
            let row = client
                .query_opt(
                    "
                    UPDATE ttn_device_mappings SET
                        ttn_application_id = $4,
                        ttn_device_id = $5,
                        producer_entity_id = $6,
                        feature_of_interest_id = $7,
                        enabled = $8,
                        metadata = $9,
                        created_at = $10,
                        updated_at = $11
                    WHERE tenant_id = $1 AND connector_id = $2 AND id = $3
                    RETURNING id, tenant_id, connector_id, ttn_application_id, ttn_device_id,
                        producer_entity_id, feature_of_interest_id, enabled, metadata,
                        created_at, updated_at
                    ",
                    &[
                        &mapping.tenant_id,
                        &mapping.connector_id,
                        &mapping.id,
                        &mapping.ttn_application_id,
                        &mapping.ttn_device_id,
                        &mapping.producer_entity_id,
                        &mapping.feature_of_interest_id,
                        &mapping.enabled,
                        &json_option_column(mapping.metadata.as_ref()),
                        &mapping.created_at,
                        &mapping.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_ttn_device_mapping)
                .ok_or(StorageError::NotFound)
        })
    }

    fn delete_ttn_device_mapping(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
        mapping_id: Uuid,
    ) -> StorageResult<()> {
        self.with_client(|client| {
            let deleted = client
                .execute(
                    "
                    DELETE FROM ttn_device_mappings
                    WHERE tenant_id = $1 AND connector_id = $2 AND id = $3
                    ",
                    &[&tenant_id, &connector_id, &mapping_id],
                )
                .map_err(map_postgres_error)?;
            if deleted == 0 {
                return Err(StorageError::NotFound);
            }
            Ok(())
        })
    }

    fn find_ttn_device_mapping(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
        ttn_application_id: Option<&str>,
        ttn_device_id: &str,
    ) -> StorageResult<Option<TtnDeviceMapping>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, connector_id, ttn_application_id, ttn_device_id,
                        producer_entity_id, feature_of_interest_id, enabled, metadata,
                        created_at, updated_at
                    FROM ttn_device_mappings
                    WHERE tenant_id = $1
                        AND connector_id = $2
                        AND ttn_device_id = $3
                        AND enabled = TRUE
                        AND (ttn_application_id = $4 OR ttn_application_id IS NULL)
                    ORDER BY CASE WHEN ttn_application_id = $4 THEN 0 ELSE 1 END
                    LIMIT 1
                    ",
                    &[
                        &tenant_id,
                        &connector_id,
                        &ttn_device_id,
                        &ttn_application_id,
                    ],
                )
                .map_err(map_postgres_error)?;
            Ok(row.map(row_to_ttn_device_mapping))
        })
    }
}

impl CapabilityStore for PostgresStorage {
    fn put_capabilities(
        &self,
        tenant_id: Uuid,
        entity_id: Uuid,
        capabilities: Vec<Capability>,
    ) -> StorageResult<Vec<Capability>> {
        self.with_client(|client| {
            let mut tx = client.transaction().map_err(map_postgres_error)?;
            tx.execute(
                "DELETE FROM capabilities WHERE tenant_id = $1 AND entity_id = $2",
                &[&tenant_id, &entity_id],
            )
            .map_err(map_postgres_error)?;

            for capability in &capabilities {
                if capability.entity_id != entity_id {
                    return Err(StorageError::InvalidInput(
                        "capability entity_id does not match requested entity".to_string(),
                    ));
                }
                tx.execute(
                    "
                    INSERT INTO capabilities (
                        tenant_id,
                        entity_id,
                        capability_name,
                        command_type,
                        protocol,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ",
                    &[
                        &tenant_id,
                        &entity_id,
                        &capability.capability_name,
                        &capability.command_type,
                        &capability.protocol,
                        &json_option_column(capability.metadata.as_ref()),
                    ],
                )
                .map_err(map_postgres_error)?;
            }

            tx.commit().map_err(map_postgres_error)?;
            Ok(capabilities)
        })
    }

    fn list_capabilities(
        &self,
        tenant_id: Uuid,
        entity_id: Uuid,
    ) -> StorageResult<Vec<Capability>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT tenant_id, entity_id, capability_name, command_type, protocol, metadata
                    FROM capabilities
                    WHERE tenant_id = $1 AND entity_id = $2
                    ",
                    &[&tenant_id, &entity_id],
                )
                .map_err(map_postgres_error)?;
            let mut capabilities = rows.into_iter().map(row_to_capability).collect::<Vec<_>>();
            capabilities.sort_by(|left, right| left.capability_name.cmp(&right.capability_name));
            Ok(capabilities)
        })
    }
}

impl PolicyStore for PostgresStorage {
    fn put_policies(&self, tenant_id: Uuid, policies: Vec<Policy>) -> StorageResult<Vec<Policy>> {
        self.with_client(|client| {
            let mut tx = client.transaction().map_err(map_postgres_error)?;
            tx.execute("DELETE FROM policies WHERE tenant_id = $1", &[&tenant_id])
                .map_err(map_postgres_error)?;

            for policy in &policies {
                if policy.tenant_id != tenant_id {
                    return Err(StorageError::InvalidInput(
                        "policy tenant_id does not match requested tenant".to_string(),
                    ));
                }
                tx.execute(
                    "
                    INSERT INTO policies (
                        id,
                        tenant_id,
                        target_entity_id,
                        command_type,
                        requires_approval,
                        auto_execute_allowed,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ",
                    &[
                        &policy.id,
                        &tenant_id,
                        &policy.target_entity_id,
                        &policy.command_type,
                        &policy.requires_approval,
                        &policy.auto_execute_allowed,
                        &json_option_column(policy.metadata.as_ref()),
                    ],
                )
                .map_err(map_postgres_error)?;
            }

            tx.commit().map_err(map_postgres_error)?;
            Ok(policies)
        })
    }

    fn query_policies(
        &self,
        tenant_id: Uuid,
        target_entity_id: Option<Uuid>,
        command_type: Option<&str>,
    ) -> StorageResult<Vec<Policy>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, target_entity_id, command_type, requires_approval, auto_execute_allowed, metadata
                    FROM policies
                    WHERE tenant_id = $1
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;

            let mut policies = rows
                .into_iter()
                .map(|row| {
                    let metadata = row
                        .get::<_, Option<Json<Value>>>("metadata")
                        .map(|Json(value)| value);
                    Policy {
                        id: row.get("id"),
                        tenant_id: row.get("tenant_id"),
                        target_entity_id: row.get("target_entity_id"),
                        command_type: row.get("command_type"),
                        requires_approval: row.get("requires_approval"),
                        auto_execute_allowed: row.get("auto_execute_allowed"),
                        metadata,
                    }
                })
                .filter(|policy| {
                    target_entity_id
                        .map(|id| policy.target_entity_id == Some(id))
                        .unwrap_or(true)
                })
                .filter(|policy| {
                    command_type
                        .map(|command_type| policy.command_type.as_deref() == Some(command_type))
                        .unwrap_or(true)
                })
                .collect::<Vec<_>>();

            policies.sort_by_key(|policy| {
                (
                    policy.target_entity_id.is_none(),
                    policy.command_type.is_none(),
                    policy.id,
                )
            });
            Ok(policies)
        })
    }
}

impl ExecutorStore for PostgresStorage {
    fn create_executor(&self, executor: ExecutorAgent) -> StorageResult<ExecutorAgent> {
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO executor_agents (
                        id,
                        tenant_id,
                        agent_key,
                        agent_type,
                        display_name,
                        status,
                        last_seen_at,
                        metadata,
                        created_at,
                        updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    RETURNING id, tenant_id, agent_key, agent_type, display_name, status, last_seen_at, metadata, created_at, updated_at
                    ",
                    &[
                        &executor.id,
                        &executor.tenant_id,
                        &executor.agent_key,
                        &executor.agent_type,
                        &executor.display_name,
                        &executor_status_to_db(&executor.status),
                        &executor.last_seen_at,
                        &json_option_column(executor.metadata.as_ref()),
                        &executor.created_at,
                        &executor.updated_at,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            row_to_executor(row)
        })
    }

    fn update_executor(&self, executor: ExecutorAgent) -> StorageResult<ExecutorAgent> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    UPDATE executor_agents
                    SET agent_key = $3,
                        agent_type = $4,
                        display_name = $5,
                        status = $6,
                        last_seen_at = $7,
                        metadata = $8,
                        updated_at = $9
                    WHERE tenant_id = $1 AND id = $2
                    RETURNING id, tenant_id, agent_key, agent_type, display_name, status, last_seen_at, metadata, created_at, updated_at
                    ",
                    &[
                        &executor.tenant_id,
                        &executor.id,
                        &executor.agent_key,
                        &executor.agent_type,
                        &executor.display_name,
                        &executor_status_to_db(&executor.status),
                        &executor.last_seen_at,
                        &json_option_column(executor.metadata.as_ref()),
                        &executor.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_executor).ok_or(StorageError::NotFound)
        })?
    }

    fn get_executor(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
    ) -> StorageResult<Option<ExecutorAgent>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, agent_key, agent_type, display_name, status, last_seen_at, metadata, created_at, updated_at
                    FROM executor_agents
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &executor_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_executor(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn list_executors(&self, tenant_id: Uuid) -> StorageResult<Vec<ExecutorAgent>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, agent_key, agent_type, display_name, status, last_seen_at, metadata, created_at, updated_at
                    FROM executor_agents
                    WHERE tenant_id = $1
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;
            let mut executors = rows
                .into_iter()
                .map(row_to_executor)
                .collect::<StorageResult<Vec<_>>>()?;
            executors.sort_by(|left, right| left.agent_key.cmp(&right.agent_key));
            Ok(executors)
        })
    }

    fn put_executor_capabilities(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
        capabilities: Vec<ExecutorCapability>,
    ) -> StorageResult<Vec<ExecutorCapability>> {
        self.with_client(|client| {
            let mut tx = client.transaction().map_err(map_postgres_error)?;
            tx.execute(
                "DELETE FROM executor_capabilities WHERE tenant_id = $1 AND agent_id = $2",
                &[&tenant_id, &executor_id],
            )
            .map_err(map_postgres_error)?;

            for capability in &capabilities {
                if capability.agent_id != executor_id {
                    return Err(StorageError::InvalidInput(
                        "executor capability agent_id does not match requested executor"
                            .to_string(),
                    ));
                }
                tx.execute(
                    "
                    INSERT INTO executor_capabilities (
                        tenant_id,
                        agent_id,
                        command_type,
                        protocol,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5)
                    ",
                    &[
                        &tenant_id,
                        &executor_id,
                        &capability.command_type,
                        &capability.protocol,
                        &json_option_column(capability.metadata.as_ref()),
                    ],
                )
                .map_err(map_postgres_error)?;
            }

            tx.commit().map_err(map_postgres_error)?;
            Ok(capabilities)
        })
    }

    fn list_executor_capabilities(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
    ) -> StorageResult<Vec<ExecutorCapability>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT tenant_id, agent_id, command_type, protocol, metadata
                    FROM executor_capabilities
                    WHERE tenant_id = $1 AND agent_id = $2
                    ",
                    &[&tenant_id, &executor_id],
                )
                .map_err(map_postgres_error)?;
            let mut capabilities = rows
                .into_iter()
                .map(row_to_executor_capability)
                .collect::<Vec<_>>();
            capabilities.sort_by(|left, right| left.command_type.cmp(&right.command_type));
            Ok(capabilities)
        })
    }

    fn put_executor_scopes(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
        scopes: Vec<ExecutorScope>,
    ) -> StorageResult<Vec<ExecutorScope>> {
        self.with_client(|client| {
            let mut tx = client.transaction().map_err(map_postgres_error)?;
            tx.execute(
                "DELETE FROM executor_scopes WHERE tenant_id = $1 AND agent_id = $2",
                &[&tenant_id, &executor_id],
            )
            .map_err(map_postgres_error)?;

            for scope in &scopes {
                if scope.agent_id != executor_id {
                    return Err(StorageError::InvalidInput(
                        "executor scope agent_id does not match requested executor".to_string(),
                    ));
                }
                tx.execute(
                    "
                    INSERT INTO executor_scopes (
                        tenant_id,
                        agent_id,
                        target_entity_id,
                        entity_type,
                        relationship_type,
                        metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    ",
                    &[
                        &tenant_id,
                        &executor_id,
                        &scope.target_entity_id,
                        &scope.entity_type,
                        &scope.relationship_type,
                        &json_option_column(scope.metadata.as_ref()),
                    ],
                )
                .map_err(map_postgres_error)?;
            }

            tx.commit().map_err(map_postgres_error)?;
            Ok(scopes)
        })
    }

    fn list_executor_scopes(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
    ) -> StorageResult<Vec<ExecutorScope>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT tenant_id, agent_id, target_entity_id, entity_type, relationship_type, metadata
                    FROM executor_scopes
                    WHERE tenant_id = $1 AND agent_id = $2
                    ",
                    &[&tenant_id, &executor_id],
                )
                .map_err(map_postgres_error)?;
            let mut scopes = rows.into_iter().map(row_to_executor_scope).collect::<Vec<_>>();
            scopes.sort_by(|left, right| {
                left.target_entity_id
                    .cmp(&right.target_entity_id)
                    .then_with(|| left.entity_type.cmp(&right.entity_type))
                    .then_with(|| left.relationship_type.cmp(&right.relationship_type))
            });
            Ok(scopes)
        })
    }
}

impl EdgeAdapterStore for PostgresStorage {
    fn create_edge_adapter(&self, adapter: EdgeAdapter) -> StorageResult<EdgeAdapter> {
        self.with_client(|client| {
            let row = client
                .query_one(
                    "
                    INSERT INTO edge_adapters (
                        id,
                        tenant_id,
                        adapter_key,
                        display_name,
                        adapter_type,
                        status,
                        version,
                        host_id,
                        site_id,
                        environment,
                        last_seen_at,
                        metadata,
                        created_at,
                        updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    RETURNING id, tenant_id, adapter_key, display_name, adapter_type, status, version, host_id, site_id, environment, last_seen_at, metadata, created_at, updated_at
                    ",
                    &[
                        &adapter.id,
                        &adapter.tenant_id,
                        &adapter.adapter_key,
                        &adapter.display_name,
                        &edge_adapter_type_to_db(&adapter.adapter_type),
                        &edge_adapter_status_to_db(&adapter.status),
                        &adapter.version,
                        &adapter.host_id,
                        &adapter.site_id,
                        &adapter.environment,
                        &adapter.last_seen_at,
                        &json_option_column(adapter.metadata.as_ref()),
                        &adapter.created_at,
                        &adapter.updated_at,
                    ],
                )
                .map_err(|err| if is_unique_violation(&err) { StorageError::Conflict } else { map_postgres_error(err) })?;
            row_to_edge_adapter(row)
        })
    }

    fn update_edge_adapter(&self, adapter: EdgeAdapter) -> StorageResult<EdgeAdapter> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    UPDATE edge_adapters
                    SET adapter_key = $3,
                        display_name = $4,
                        adapter_type = $5,
                        status = $6,
                        version = $7,
                        host_id = $8,
                        site_id = $9,
                        environment = $10,
                        last_seen_at = $11,
                        metadata = $12,
                        updated_at = $13
                    WHERE tenant_id = $1 AND id = $2
                    RETURNING id, tenant_id, adapter_key, display_name, adapter_type, status, version, host_id, site_id, environment, last_seen_at, metadata, created_at, updated_at
                    ",
                    &[
                        &adapter.tenant_id,
                        &adapter.id,
                        &adapter.adapter_key,
                        &adapter.display_name,
                        &edge_adapter_type_to_db(&adapter.adapter_type),
                        &edge_adapter_status_to_db(&adapter.status),
                        &adapter.version,
                        &adapter.host_id,
                        &adapter.site_id,
                        &adapter.environment,
                        &adapter.last_seen_at,
                        &json_option_column(adapter.metadata.as_ref()),
                        &adapter.updated_at,
                    ],
                )
                .map_err(map_postgres_error)?;
            row.map(row_to_edge_adapter).ok_or(StorageError::NotFound)
        })?
    }

    fn get_edge_adapter(
        &self,
        tenant_id: Uuid,
        adapter_id: Uuid,
    ) -> StorageResult<Option<EdgeAdapter>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, adapter_key, display_name, adapter_type, status, version, host_id, site_id, environment, last_seen_at, metadata, created_at, updated_at
                    FROM edge_adapters
                    WHERE tenant_id = $1 AND id = $2
                    ",
                    &[&tenant_id, &adapter_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_edge_adapter(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn get_edge_adapter_by_key(
        &self,
        tenant_id: Uuid,
        adapter_key: &str,
    ) -> StorageResult<Option<EdgeAdapter>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT id, tenant_id, adapter_key, display_name, adapter_type, status, version, host_id, site_id, environment, last_seen_at, metadata, created_at, updated_at
                    FROM edge_adapters
                    WHERE tenant_id = $1 AND adapter_key = $2
                    ",
                    &[&tenant_id, &adapter_key],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_edge_adapter(row).map(Some),
                None => Ok(None),
            }
        })
    }

    fn list_edge_adapters(&self, tenant_id: Uuid) -> StorageResult<Vec<EdgeAdapter>> {
        self.with_client(|client| {
            let rows = client
                .query(
                    "
                    SELECT id, tenant_id, adapter_key, display_name, adapter_type, status, version, host_id, site_id, environment, last_seen_at, metadata, created_at, updated_at
                    FROM edge_adapters
                    WHERE tenant_id = $1
                    ",
                    &[&tenant_id],
                )
                .map_err(map_postgres_error)?;
            let mut adapters = rows
                .into_iter()
                .map(row_to_edge_adapter)
                .collect::<StorageResult<Vec<_>>>()?;
            adapters.sort_by(|left, right| left.adapter_key.cmp(&right.adapter_key));
            Ok(adapters)
        })
    }

    fn put_edge_adapter_status(
        &self,
        tenant_id: Uuid,
        status: EdgeAdapterStatusReport,
    ) -> StorageResult<EdgeAdapterStatusReport> {
        self.with_client(|client| {
            let mut tx = client.transaction().map_err(map_postgres_error)?;
            let adapter_exists = tx
                .query_opt(
                    "SELECT id FROM edge_adapters WHERE tenant_id = $1 AND id = $2",
                    &[&tenant_id, &status.adapter_id],
                )
                .map_err(map_postgres_error)?
                .is_some();
            if !adapter_exists {
                return Err(StorageError::NotFound);
            }
            tx.execute(
                "
                INSERT INTO edge_adapter_statuses (
                    adapter_id,
                    tenant_id,
                    status,
                    observed_at,
                    uptime_seconds,
                    active_connectors,
                    active_plugins,
                    dlq_depth,
                    dlq_oldest_record_at,
                    last_publish_success_at,
                    last_publish_failure_at,
                    last_error,
                    metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (adapter_id) DO UPDATE SET
                    tenant_id = EXCLUDED.tenant_id,
                    status = EXCLUDED.status,
                    observed_at = EXCLUDED.observed_at,
                    uptime_seconds = EXCLUDED.uptime_seconds,
                    active_connectors = EXCLUDED.active_connectors,
                    active_plugins = EXCLUDED.active_plugins,
                    dlq_depth = EXCLUDED.dlq_depth,
                    dlq_oldest_record_at = EXCLUDED.dlq_oldest_record_at,
                    last_publish_success_at = EXCLUDED.last_publish_success_at,
                    last_publish_failure_at = EXCLUDED.last_publish_failure_at,
                    last_error = EXCLUDED.last_error,
                    metadata = EXCLUDED.metadata
                ",
                &[
                    &status.adapter_id,
                    &tenant_id,
                    &edge_adapter_status_to_db(&status.status),
                    &status.observed_at,
                    &status.uptime_seconds.map(|value| value as i64),
                    &status.active_connectors.map(|value| value as i32),
                    &status.active_plugins.map(|value| value as i32),
                    &status.dlq_depth.map(|value| value as i64),
                    &status.dlq_oldest_record_at,
                    &status.last_publish_success_at,
                    &status.last_publish_failure_at,
                    &status.last_error,
                    &json_option_column(status.metadata.as_ref()),
                ],
            )
            .map_err(map_postgres_error)?;
            tx.commit().map_err(map_postgres_error)?;
            Ok(status)
        })
    }

    fn get_edge_adapter_status(
        &self,
        tenant_id: Uuid,
        adapter_id: Uuid,
    ) -> StorageResult<Option<EdgeAdapterStatusReport>> {
        self.with_client(|client| {
            let row = client
                .query_opt(
                    "
                    SELECT adapter_id, tenant_id, status, observed_at, uptime_seconds, active_connectors, active_plugins, dlq_depth, dlq_oldest_record_at, last_publish_success_at, last_publish_failure_at, last_error, metadata
                    FROM edge_adapter_statuses
                    WHERE tenant_id = $1 AND adapter_id = $2
                    ",
                    &[&tenant_id, &adapter_id],
                )
                .map_err(map_postgres_error)?;
            match row {
                Some(row) => row_to_edge_adapter_status(row).map(Some),
                None => Ok(None),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;
    use std::sync::{Mutex, OnceLock};

    static POSTGRES_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn postgres_test_storage() -> Option<PostgresStorage> {
        let url = match std::env::var("AIONCORE_TEST_DATABASE_URL") {
            Ok(value) => value,
            Err(_) => {
                eprintln!(
                    "skipping PostgreSQL storage tests; set AIONCORE_TEST_DATABASE_URL to enable them"
                );
                return None;
            }
        };

        let _guard = POSTGRES_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("postgres test lock poisoned");

        let storage = PostgresStorage::connect(PostgresStorageConfig::new(url))
            .expect("failed to connect to PostgreSQL test database");
        storage
            .run_embedded_migrations()
            .expect("failed to run embedded migrations");
        Some(storage)
    }

    fn unique_suffix() -> String {
        format!("{}", Uuid::new_v4()).replace('-', "")
    }

    fn postgres_test_client() -> Client {
        let url = std::env::var("AIONCORE_TEST_DATABASE_URL")
            .expect("AIONCORE_TEST_DATABASE_URL must be set for PostgreSQL tests");
        let config: PgConfig = url.parse().expect("invalid PostgreSQL test URL");
        config
            .connect(NoTls)
            .expect("failed to connect to PostgreSQL test database")
    }

    fn build_tenant(suffix: &str) -> Tenant {
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        Tenant {
            id: Uuid::new_v4(),
            slug: format!("tenant-{suffix}"),
            name: format!("Tenant {suffix}"),
            metadata: serde_json::json!({"suite": "postgres"}),
            created_at: now,
            updated_at: now,
        }
    }

    fn build_entity(tenant_id: Uuid, suffix: &str, entity_type: &str) -> Entity {
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        Entity {
            id: Uuid::new_v4(),
            tenant_id,
            entity_key: format!("entity-{suffix}"),
            entity_type: entity_type.to_string(),
            jsonld: serde_json::json!({
                "@context": {"aion": "https://aioncore.org/ns#"},
                "@id": format!("urn:aion:test:{suffix}"),
                "@type": entity_type,
            }),
            created_at: now,
            updated_at: now,
        }
    }

    fn build_connector(
        tenant_id: Uuid,
        suffix: &str,
        connector_key: &str,
        connector_type: IngestionConnectorType,
        connector_profile: ConnectorProfile,
        payload_format: &str,
    ) -> IngestionConnector {
        IngestionConnector::new(
            tenant_id,
            format!("{connector_key}-{suffix}"),
            connector_type,
            connector_profile,
            false,
            Some(format!("Connector {suffix}")),
            Some("http".to_string()),
            Some(format!("endpoint-{suffix}")),
            Some("mqtt://127.0.0.1:1883".to_string()),
            Some(format!("client-{suffix}")),
            Some("aioncore/+/+/data".to_string()),
            Some(format!("/ingestion/{suffix}")),
            Some(payload_format.to_string()),
            Some("application/json".to_string()),
            None,
            None,
            Some(json!({"suite": "postgres", "suffix": suffix})),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap(),
        )
        .expect("valid ingestion connector")
    }

    fn build_connector_secret(tenant_id: Uuid, suffix: &str) -> ConnectorSecret {
        ConnectorSecret::new(
            tenant_id,
            format!("broker-secret-{suffix}"),
            ConnectorSecretType::MqttBasicAuth,
            Some(format!("mqtt-user-{suffix}")),
            format!("secret-value-{suffix}"),
            Some(json!({"suite": "postgres", "suffix": suffix})),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap(),
        )
        .expect("valid connector secret")
    }

    fn build_ttn_device_mapping(
        tenant_id: Uuid,
        connector_id: Uuid,
        producer_entity_id: Uuid,
        feature_of_interest_id: Option<Uuid>,
        suffix: &str,
        ttn_application_id: Option<&str>,
    ) -> TtnDeviceMapping {
        TtnDeviceMapping::new(
            tenant_id,
            connector_id,
            ttn_application_id.map(ToOwned::to_owned),
            format!("soil-node-{suffix}"),
            producer_entity_id,
            feature_of_interest_id,
            true,
            Some(json!({"suite": "postgres", "suffix": suffix})),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap(),
        )
        .expect("valid TTN device mapping")
    }

    fn build_relationship(
        tenant_id: Uuid,
        source_entity_id: Uuid,
        target_entity_id: Uuid,
        suffix: &str,
    ) -> Relationship {
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        Relationship {
            id: Uuid::new_v4(),
            tenant_id,
            source_entity_id,
            relationship_type: format!("aion:relatedTo:{suffix}"),
            target_entity_id,
            jsonld: serde_json::json!({"@type": "aion:Relationship"}),
            created_at: now,
        }
    }

    fn build_observation(
        tenant_id: Uuid,
        producer_entity_id: Uuid,
        feature_of_interest_id: Uuid,
        observed_property: &str,
        value: ObservationValue,
        unit: Option<&str>,
        observed_at: chrono::DateTime<Utc>,
        received_at: chrono::DateTime<Utc>,
        protocol: &str,
        payload_format: &str,
        raw_message_id: Option<Uuid>,
    ) -> Observation {
        Observation::new(
            tenant_id,
            producer_entity_id,
            feature_of_interest_id,
            observed_property,
            value,
            unit.map(|value| value.to_string()),
            observed_at,
            received_at,
            protocol,
            payload_format,
            raw_message_id,
            json!({"quality": "good"}),
            json!({"source": "postgres"}),
        )
        .expect("valid observation")
    }

    fn build_event(
        tenant_id: Uuid,
        event_type: &str,
        severity: EventSeverity,
        source_entity_id: Option<Uuid>,
        target_entity_id: Option<Uuid>,
        message: Option<&str>,
        occurred_at: chrono::DateTime<Utc>,
        observed_at: Option<chrono::DateTime<Utc>>,
        correlation_id: Option<&str>,
        raw_message_id: Option<Uuid>,
        command_id: Option<Uuid>,
    ) -> Event {
        Event::new(
            tenant_id,
            event_type,
            severity,
            source_entity_id,
            target_entity_id,
            message.map(|value| value.to_string()),
            occurred_at,
            observed_at,
            correlation_id.map(|value| value.to_string()),
            raw_message_id,
            None,
            command_id,
            None,
            None,
            Some(json!({"source": "postgres"})),
            occurred_at,
        )
        .expect("valid event")
    }

    fn seed_command_row(
        tenant_id: Uuid,
        command_id: Uuid,
        target_entity_id: Uuid,
        command_type: &str,
    ) {
        let mut client = postgres_test_client();
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let payload = json!({"target_state": "on"});
        client
            .execute(
                "
                INSERT INTO commands (
                    id,
                    tenant_id,
                    target_entity_id,
                    command_type,
                    payload,
                    status,
                    created_at,
                    updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (id) DO NOTHING
                ",
                &[
                    &command_id,
                    &tenant_id,
                    &target_entity_id,
                    &command_type,
                    &payload,
                    &"pending",
                    &now,
                    &now,
                ],
            )
            .expect("seed command row");
    }

    #[test]
    fn postgres_tests_skip_cleanly_without_env() {
        if std::env::var("AIONCORE_TEST_DATABASE_URL").is_ok() {
            return;
        }

        assert!(postgres_test_storage().is_none());
    }

    #[test]
    fn postgres_edge_adapter_parity() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let adapter = EdgeAdapter::new(
            tenant.id,
            format!("edge-adapter-{suffix}"),
            EdgeAdapterType::Fog,
            Some(format!("Edge adapter {suffix}")),
            EdgeAdapterStatus::Online,
            Some("1.0.0".to_string()),
            Some("host-01".to_string()),
            Some("site-01".to_string()),
            Some("fog".to_string()),
            Some(json!({"suite": "postgres", "suffix": suffix})),
            now,
        )
        .expect("valid edge adapter");
        let status = EdgeAdapterStatusReport {
            adapter_id: adapter.id,
            status: EdgeAdapterStatus::Degraded,
            observed_at: now,
            uptime_seconds: Some(3600),
            active_connectors: Some(2),
            active_plugins: Some(1),
            dlq_depth: Some(4),
            dlq_oldest_record_at: Some(now),
            last_publish_success_at: Some(now),
            last_publish_failure_at: None,
            last_error: Some("upstream unavailable".to_string()),
            metadata: Some(json!({"source": "postgres-test"})),
        };

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store
                .create_tenant(tenant.clone())
                .expect("failed to create tenant");
        }

        for store in [
            &in_memory as &dyn EdgeAdapterStore,
            &pg as &dyn EdgeAdapterStore,
        ] {
            let created = store
                .create_edge_adapter(adapter.clone())
                .expect("create edge adapter");
            assert_eq!(created, adapter);
            assert_eq!(
                store
                    .get_edge_adapter(tenant.id, adapter.id)
                    .expect("get edge adapter")
                    .expect("missing edge adapter"),
                adapter
            );
            assert_eq!(
                store
                    .get_edge_adapter_by_key(tenant.id, &adapter.adapter_key)
                    .expect("get edge adapter by key")
                    .expect("missing edge adapter by key"),
                adapter
            );

            let updated_status = store
                .put_edge_adapter_status(tenant.id, status.clone())
                .expect("put edge adapter status");
            assert_eq!(updated_status, status);
            assert_eq!(
                store
                    .get_edge_adapter_status(tenant.id, adapter.id)
                    .expect("get edge adapter status")
                    .expect("missing edge adapter status"),
                status
            );
        }
    }

    #[test]
    fn postgres_parity_entities() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let entity_a = build_entity(tenant.id, &format!("{suffix}-a"), "aion:Sensor");
        let entity_b = build_entity(tenant.id, &format!("{suffix}-b"), "aion:Device");

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store
                .create_tenant(tenant.clone())
                .expect("failed to create tenant");
        }

        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            assert_eq!(
                store
                    .create_entity(entity_a.clone())
                    .expect("create entity"),
                entity_a
            );
            assert_eq!(
                store
                    .create_entity(entity_b.clone())
                    .expect("create entity"),
                entity_b
            );
            assert_eq!(
                store
                    .get_entity(tenant.id, entity_a.id)
                    .expect("get entity")
                    .expect("missing entity"),
                entity_a
            );
            assert_eq!(
                store
                    .get_entity_by_key(tenant.id, &entity_b.entity_key)
                    .expect("get entity by key")
                    .expect("missing entity"),
                entity_b
            );
            let mut entities = store.list_entities(tenant.id).expect("list entities");
            entities.sort_by(|left, right| left.entity_key.cmp(&right.entity_key));
            assert_eq!(entities, vec![entity_a.clone(), entity_b.clone()]);

            let mut updated = entity_b.clone();
            updated.entity_type = "aion:OperationalDevice".to_string();
            updated.jsonld = serde_json::json!({
                "@context": {"aion": "https://aioncore.org/ns#"},
                "@id": format!("urn:aion:test:{}", updated.entity_key),
                "@type": "aion:OperationalDevice",
                "name": "Updated"
            });
            updated.updated_at = chrono::Utc::now();
            let stored = store.update_entity(updated.clone()).expect("update entity");
            assert_eq!(stored.id, entity_b.id);
            assert_eq!(stored.entity_key, entity_b.entity_key);
            assert_eq!(stored.entity_type, "aion:OperationalDevice");
        }
    }

    #[test]
    fn postgres_parity_relationships() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let source = build_entity(tenant.id, &format!("{suffix}-source"), "aion:Sensor");
        let target = build_entity(tenant.id, &format!("{suffix}-target"), "aion:Device");
        let relationship = build_relationship(tenant.id, source.id, target.id, &suffix);

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store
                .create_entity(source.clone())
                .expect("create source entity");
            store
                .create_entity(target.clone())
                .expect("create target entity");
        }

        for store in [
            &in_memory as &dyn RelationshipStore,
            &pg as &dyn RelationshipStore,
        ] {
            assert_eq!(
                store
                    .create_relationship(relationship.clone())
                    .expect("create relationship"),
                relationship
            );
            let mut relationships = store
                .list_relationships(tenant.id, Some(source.id), Some(target.id))
                .expect("list relationships");
            relationships.sort_by(|left, right| left.created_at.cmp(&right.created_at));
            assert_eq!(relationships, vec![relationship.clone()]);
        }
    }

    #[test]
    fn postgres_parity_payload_profile_and_capabilities() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let entity = build_entity(tenant.id, &format!("{suffix}-entity"), "aion:Sensor");
        let profile = PayloadProfile::new(
            entity.id,
            "senml-json",
            Some("http".to_string()),
            Some("application/senml+json".to_string()),
            Some(serde_json::json!({"value": "$.v"})),
            Some(serde_json::json!({"suite": "postgres"})),
        )
        .expect("valid payload profile");
        let capabilities = vec![
            Capability::new(
                entity.id,
                "ReadTemperature",
                "ReadTemperature",
                Some("mqtt".to_string()),
                Some(serde_json::json!({"priority": 1})),
            )
            .expect("valid capability"),
            Capability::new(
                entity.id,
                "ReadHumidity",
                "ReadHumidity",
                None,
                Some(serde_json::json!({"priority": 2})),
            )
            .expect("valid capability"),
        ];

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store.create_entity(entity.clone()).expect("create entity");
        }

        for store in [
            &in_memory as &dyn PayloadProfileStore,
            &pg as &dyn PayloadProfileStore,
        ] {
            assert_eq!(
                store
                    .put_payload_profile(tenant.id, profile.clone())
                    .expect("put payload profile"),
                profile
            );
            assert_eq!(
                store
                    .get_payload_profile(tenant.id, entity.id)
                    .expect("get payload profile")
                    .expect("missing payload profile"),
                profile
            );
        }

        for store in [
            &in_memory as &dyn CapabilityStore,
            &pg as &dyn CapabilityStore,
        ] {
            assert_eq!(
                store
                    .put_capabilities(tenant.id, entity.id, capabilities.clone())
                    .expect("put capabilities"),
                capabilities
            );
            let mut listed = store
                .list_capabilities(tenant.id, entity.id)
                .expect("list capabilities");
            listed.sort_by(|left, right| left.capability_name.cmp(&right.capability_name));
            assert_eq!(listed, capabilities);
        }
    }

    #[test]
    fn postgres_parity_ingestion_connectors() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let http_connector = build_connector(
            tenant.id,
            &suffix,
            "http",
            IngestionConnectorType::Http,
            ConnectorProfile::Custom,
            "senml-json",
        );
        let generic_mqtt_connector = build_connector(
            tenant.id,
            &suffix,
            "generic-mqtt",
            IngestionConnectorType::Mqtt,
            ConnectorProfile::GenericMqtt,
            "canonical-json",
        );
        let ttn_connector = build_connector(
            tenant.id,
            &suffix,
            "ttn",
            IngestionConnectorType::Mqtt,
            ConnectorProfile::TtnV3,
            "ttn-uplink-json",
        );

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }

        for store in [
            &in_memory as &dyn IngestionConnectorStore,
            &pg as &dyn IngestionConnectorStore,
        ] {
            for connector in [
                http_connector.clone(),
                generic_mqtt_connector.clone(),
                ttn_connector.clone(),
            ] {
                assert_eq!(
                    store
                        .create_ingestion_connector(connector.clone())
                        .expect("create connector"),
                    connector
                );
                assert_eq!(
                    store
                        .get_ingestion_connector(tenant.id, connector.id)
                        .expect("get connector")
                        .expect("missing connector"),
                    connector
                );
            }

            let listed = store
                .list_ingestion_connectors(tenant.id)
                .expect("list connectors");
            assert_eq!(listed.len(), 3);
            assert!(listed
                .iter()
                .any(|connector| connector.connector_profile == ConnectorProfile::TtnV3));
            assert!(listed
                .iter()
                .any(|connector| connector.connector_profile == ConnectorProfile::GenericMqtt));
            assert!(listed
                .iter()
                .any(|connector| connector.connector_type == IngestionConnectorType::Http));

            let mut enabled = http_connector.clone();
            enabled.set_enabled(true, Utc.with_ymd_and_hms(2026, 4, 27, 12, 1, 0).unwrap());
            enabled.display_name = Some("Updated HTTP connector".to_string());
            enabled.endpoint = Some("/updated".to_string());
            enabled.payload_format = Some("canonical-json".to_string());
            enabled.metadata = Some(json!({"updated": true}));
            assert_eq!(
                store
                    .update_ingestion_connector(enabled.clone())
                    .expect("enable connector"),
                enabled
            );
            let updated_connector = store
                .get_ingestion_connector(tenant.id, http_connector.id)
                .expect("get updated connector")
                .expect("missing updated connector");
            assert!(updated_connector.enabled);
            assert_eq!(
                updated_connector.display_name.as_deref(),
                Some("Updated HTTP connector")
            );
            assert_eq!(updated_connector.endpoint.as_deref(), Some("/updated"));
            assert_eq!(
                updated_connector.payload_format.as_deref(),
                Some("canonical-json")
            );
            assert_eq!(updated_connector.metadata, Some(json!({"updated": true})));

            let mut disabled = enabled.clone();
            disabled.set_enabled(false, Utc.with_ymd_and_hms(2026, 4, 27, 12, 2, 0).unwrap());
            assert_eq!(
                store
                    .update_ingestion_connector(disabled.clone())
                    .expect("disable connector"),
                disabled
            );
            assert!(
                !store
                    .get_ingestion_connector(tenant.id, http_connector.id)
                    .expect("get disabled connector")
                    .expect("missing disabled connector")
                    .enabled
            );
        }
    }

    #[test]
    fn postgres_parity_connector_secrets() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }

        for store in [
            &in_memory as &dyn ConnectorSecretStore,
            &pg as &dyn ConnectorSecretStore,
        ] {
            let secret = build_connector_secret(tenant.id, &suffix);
            let secret = store
                .create_connector_secret(secret)
                .expect("create connector secret");
            assert_eq!(
                store
                    .get_connector_secret(tenant.id, secret.id)
                    .expect("get connector secret")
                    .expect("missing connector secret"),
                secret
            );
            assert_eq!(
                store
                    .list_connector_secrets(tenant.id)
                    .expect("list connector secrets")
                    .len(),
                1
            );
            assert!(!format!("{secret:?}").contains(&secret.secret_value));
            store
                .delete_connector_secret(tenant.id, secret.id)
                .expect("delete connector secret");
            assert!(store
                .get_connector_secret(tenant.id, secret.id)
                .expect("get deleted connector secret")
                .is_none());
        }
    }

    #[test]
    fn postgres_parity_ttn_device_mappings() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let producer = build_entity(tenant.id, &format!("{suffix}-producer"), "aion:Sensor");
        let feature = build_entity(tenant.id, &format!("{suffix}-feature"), "aion:Field");
        let connector = build_connector(
            tenant.id,
            &suffix,
            "ttn-mapping",
            IngestionConnectorType::Mqtt,
            ConnectorProfile::TtnV3,
            "ttn-uplink-json",
        );

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store
                .create_entity(producer.clone())
                .expect("create producer");
            store
                .create_entity(feature.clone())
                .expect("create feature");
        }
        for store in [
            &in_memory as &dyn IngestionConnectorStore,
            &pg as &dyn IngestionConnectorStore,
        ] {
            store
                .create_ingestion_connector(connector.clone())
                .expect("create connector");
        }

        for store in [
            &in_memory as &dyn TtnDeviceMappingStore,
            &pg as &dyn TtnDeviceMappingStore,
        ] {
            let generic = build_ttn_device_mapping(
                tenant.id,
                connector.id,
                producer.id,
                Some(feature.id),
                &suffix,
                None,
            );
            let app_specific = build_ttn_device_mapping(
                tenant.id,
                connector.id,
                producer.id,
                Some(feature.id),
                &format!("{suffix}-app"),
                Some("farm-app"),
            );
            let generic = store
                .create_ttn_device_mapping(generic)
                .expect("create generic mapping");
            let app_specific = store
                .create_ttn_device_mapping(app_specific)
                .expect("create application mapping");

            assert_eq!(
                store
                    .get_ttn_device_mapping(tenant.id, connector.id, generic.id)
                    .expect("get mapping")
                    .expect("missing mapping"),
                generic
            );
            assert_eq!(
                store
                    .list_ttn_device_mappings(tenant.id, connector.id)
                    .expect("list mappings")
                    .len(),
                2
            );
            assert_eq!(
                store
                    .find_ttn_device_mapping(
                        tenant.id,
                        connector.id,
                        Some("farm-app"),
                        &app_specific.ttn_device_id
                    )
                    .expect("find app mapping")
                    .expect("missing app mapping")
                    .id,
                app_specific.id
            );

            let mut disabled = app_specific.clone();
            disabled.set_enabled(false, Utc.with_ymd_and_hms(2026, 4, 27, 12, 1, 0).unwrap());
            store
                .update_ttn_device_mapping(disabled)
                .expect("disable mapping");
            assert!(store
                .find_ttn_device_mapping(
                    tenant.id,
                    connector.id,
                    Some("farm-app"),
                    &app_specific.ttn_device_id
                )
                .expect("find disabled mapping")
                .is_none());
            assert_eq!(
                store
                    .find_ttn_device_mapping(
                        tenant.id,
                        connector.id,
                        Some("other-app"),
                        &generic.ttn_device_id
                    )
                    .expect("find generic mapping")
                    .expect("missing generic mapping")
                    .id,
                generic.id
            );
            assert!(matches!(
                store.create_ttn_device_mapping(build_ttn_device_mapping(
                    tenant.id,
                    connector.id,
                    producer.id,
                    Some(feature.id),
                    &suffix,
                    None,
                )),
                Err(StorageError::ConflictWithMessage(_))
            ));
            store
                .delete_ttn_device_mapping(tenant.id, connector.id, generic.id)
                .expect("delete generic mapping");
            assert!(store
                .get_ttn_device_mapping(tenant.id, connector.id, generic.id)
                .expect("get deleted mapping")
                .is_none());
        }
    }

    #[test]
    fn postgres_parity_policies_and_executors() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let entity = build_entity(tenant.id, &format!("{suffix}-entity"), "aion:Pump");
        let executor = ExecutorAgent::new(
            tenant.id,
            format!("agent-{suffix}"),
            "edge",
            Some("Edge Agent".to_string()),
            ExecutorAgentStatus::Online,
            Some(serde_json::json!({"suite": "postgres"})),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap(),
        )
        .expect("valid executor");
        let capabilities = vec![ExecutorCapability::new(
            executor.id,
            "StartPump",
            Some("local".to_string()),
            Some(serde_json::json!({"scope": "primary"})),
        )
        .expect("valid executor capability")];
        let scopes = vec![
            ExecutorScope::new(
                executor.id,
                Some(entity.id),
                Some("aion:Pump".to_string()),
                None,
                Some(serde_json::json!({"zone": "north"})),
            ),
            ExecutorScope::new(
                executor.id,
                None,
                None,
                Some("aion:locatedIn".to_string()),
                Some(serde_json::json!({"zone": "north"})),
            ),
        ];
        let policies = vec![
            Policy::new(
                tenant.id,
                Some(entity.id),
                Some("StartPump".to_string()),
                true,
                false,
                Some(serde_json::json!({"reason": "approval required"})),
            )
            .expect("valid policy"),
            Policy::new(
                tenant.id,
                None,
                Some("StopPump".to_string()),
                false,
                true,
                Some(serde_json::json!({"reason": "default policy"})),
            )
            .expect("valid policy"),
        ];

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store.create_entity(entity.clone()).expect("create entity");
        }

        for store in [&in_memory as &dyn ExecutorStore, &pg as &dyn ExecutorStore] {
            assert_eq!(
                store
                    .create_executor(executor.clone())
                    .expect("create executor"),
                executor
            );
            assert_eq!(
                store
                    .get_executor(tenant.id, executor.id)
                    .expect("get executor")
                    .expect("missing executor"),
                executor
            );
            let executors = store.list_executors(tenant.id).expect("list executors");
            assert_eq!(executors, vec![executor.clone()]);

            assert_eq!(
                store
                    .put_executor_capabilities(tenant.id, executor.id, capabilities.clone())
                    .expect("put executor capabilities"),
                capabilities
            );
            let listed_capabilities = store
                .list_executor_capabilities(tenant.id, executor.id)
                .expect("list executor capabilities");
            assert_eq!(listed_capabilities, capabilities);

            assert_eq!(
                store
                    .put_executor_scopes(tenant.id, executor.id, scopes.clone())
                    .expect("put executor scopes"),
                scopes
            );
            let mut listed_scopes = store
                .list_executor_scopes(tenant.id, executor.id)
                .expect("list executor scopes");
            listed_scopes.sort_by(|left, right| {
                left.target_entity_id
                    .cmp(&right.target_entity_id)
                    .then_with(|| left.entity_type.cmp(&right.entity_type))
                    .then_with(|| left.relationship_type.cmp(&right.relationship_type))
            });
            let mut expected_scopes = scopes.clone();
            expected_scopes.sort_by(|left, right| {
                left.target_entity_id
                    .cmp(&right.target_entity_id)
                    .then_with(|| left.entity_type.cmp(&right.entity_type))
                    .then_with(|| left.relationship_type.cmp(&right.relationship_type))
            });
            assert_eq!(listed_scopes, expected_scopes);
        }

        for store in [&in_memory as &dyn PolicyStore, &pg as &dyn PolicyStore] {
            assert_eq!(
                store
                    .put_policies(tenant.id, policies.clone())
                    .expect("put policies"),
                policies
            );
            let mut listed = store
                .query_policies(tenant.id, Some(entity.id), Some("StartPump"))
                .expect("query policies");
            listed.sort_by_key(|policy| {
                (
                    policy.target_entity_id.is_none(),
                    policy.command_type.is_none(),
                    policy.id,
                )
            });
            let expected = policies
                .iter()
                .filter(|policy| policy.matches(entity.id, "StartPump"))
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(listed, expected);
        }
    }

    #[test]
    fn postgres_parity_raw_messages() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let producer_a = build_entity(tenant.id, &format!("{suffix}-producer-a"), "aion:Sensor");
        let producer_b = build_entity(tenant.id, &format!("{suffix}-producer-b"), "aion:Sensor");
        let feature_a = build_entity(tenant.id, &format!("{suffix}-feature-a"), "aion:Zone");
        let feature_b = build_entity(tenant.id, &format!("{suffix}-feature-b"), "aion:Zone");

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store
                .create_entity(producer_a.clone())
                .expect("create producer a");
            store
                .create_entity(producer_b.clone())
                .expect("create producer b");
            store
                .create_entity(feature_a.clone())
                .expect("create feature a");
            store
                .create_entity(feature_b.clone())
                .expect("create feature b");
        }

        let received_a = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let received_b = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 1).unwrap();
        let received_c = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 2).unwrap();
        let raw_a = RawMessage::new(
            tenant.id,
            aion_raw_message::RawMessageSource::Http,
            Some("/ingest/http/a".to_string()),
            Some("device-a".to_string()),
            Some("senml-json".to_string()),
            Some("application/senml+json".to_string()),
            Some(producer_a.id),
            Some(feature_a.id),
            Some("senml-json".to_string()),
            json!({"source": "a"}),
            br#"{"temperature":21.4}"#.to_vec(),
            received_a,
        )
        .expect("raw a");
        let raw_b = RawMessage::new(
            tenant.id,
            aion_raw_message::RawMessageSource::Http,
            Some("/ingest/http/b".to_string()),
            Some("device-b".to_string()),
            Some("ultralight".to_string()),
            Some("text/plain".to_string()),
            Some(producer_b.id),
            Some(feature_a.id),
            Some("ultralight".to_string()),
            json!({"source": "b"}),
            br#"t|21.5"#.to_vec(),
            received_b,
        )
        .expect("raw b");
        let raw_c = RawMessage::new(
            tenant.id,
            aion_raw_message::RawMessageSource::Mqtt,
            Some("aion/demo/device-c".to_string()),
            Some("device-c".to_string()),
            Some("json_mapping".to_string()),
            Some("application/json".to_string()),
            Some(producer_a.id),
            Some(feature_b.id),
            Some("json_mapping".to_string()),
            json!({"source": "c"}),
            br#"{"temperature":21.6}"#.to_vec(),
            received_c,
        )
        .expect("raw c");

        for store in [
            &in_memory as &dyn RawMessageStore,
            &pg as &dyn RawMessageStore,
        ] {
            assert_eq!(store.store_raw_message(raw_a.clone()).unwrap(), raw_a);
            assert_eq!(store.store_raw_message(raw_b.clone()).unwrap(), raw_b);
            assert_eq!(store.store_raw_message(raw_c.clone()).unwrap(), raw_c);
            assert_eq!(
                store.get_raw_message(tenant.id, raw_b.id).unwrap().unwrap(),
                raw_b
            );

            let listed = store.list_raw_messages(tenant.id).unwrap();
            assert_eq!(listed, vec![raw_c.clone(), raw_b.clone(), raw_a.clone()]);

            let by_producer = store
                .query_raw_messages(tenant.id, Some(producer_a.id), None, None)
                .unwrap();
            assert_eq!(by_producer, vec![raw_c.clone(), raw_a.clone()]);

            let by_feature = store
                .query_raw_messages(tenant.id, None, Some(feature_a.id), None)
                .unwrap();
            assert_eq!(by_feature, vec![raw_b.clone(), raw_a.clone()]);

            let by_format = store
                .query_raw_messages(tenant.id, None, None, Some("ultralight"))
                .unwrap();
            assert_eq!(by_format, vec![raw_b.clone()]);
        }
    }

    #[test]
    fn postgres_parity_observations() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let producer = build_entity(tenant.id, &format!("{suffix}-producer"), "aion:Sensor");
        let feature = build_entity(tenant.id, &format!("{suffix}-feature"), "aion:Zone");
        let other_feature = build_entity(tenant.id, &format!("{suffix}-feature-2"), "aion:Zone");

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store
                .create_entity(producer.clone())
                .expect("create producer");
            store
                .create_entity(feature.clone())
                .expect("create feature");
            store
                .create_entity(other_feature.clone())
                .expect("create other feature");
        }

        let raw = RawMessage::new(
            tenant.id,
            aion_raw_message::RawMessageSource::Http,
            Some("/ingest/http".to_string()),
            Some("device-01".to_string()),
            Some("senml-json".to_string()),
            Some("application/senml+json".to_string()),
            Some(producer.id),
            Some(feature.id),
            Some("senml-json".to_string()),
            json!({"source": "observation"}),
            br#"{"temperature":21.4}"#.to_vec(),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap(),
        )
        .expect("raw message");

        for store in [
            &in_memory as &dyn RawMessageStore,
            &pg as &dyn RawMessageStore,
        ] {
            store.store_raw_message(raw.clone()).expect("store raw");
        }

        let observation = build_observation(
            tenant.id,
            producer.id,
            feature.id,
            "temperature",
            ObservationValue::Number { value: 21.4 },
            Some("Cel"),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 1).unwrap(),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 2).unwrap(),
            "http",
            "senml-json",
            Some(raw.id),
        );
        let other_observation = build_observation(
            tenant.id,
            producer.id,
            other_feature.id,
            "humidity",
            ObservationValue::Text {
                value: "54.2".to_string(),
            },
            None,
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 3).unwrap(),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 4).unwrap(),
            "http",
            "senml-json",
            Some(raw.id),
        );

        for store in [
            &in_memory as &dyn ObservationStore,
            &pg as &dyn ObservationStore,
        ] {
            assert_eq!(
                store.store_observation(observation.clone()).unwrap(),
                observation
            );
            assert_eq!(
                store.store_observation(other_observation.clone()).unwrap(),
                other_observation
            );
            assert_eq!(
                store
                    .get_observation(tenant.id, observation.id)
                    .unwrap()
                    .unwrap(),
                observation
            );
            let by_feature = store
                .query_observations(tenant.id, Some(feature.id), None, None, None, 10)
                .unwrap();
            assert_eq!(by_feature, vec![observation.clone()]);
            assert_eq!(by_feature[0].raw_message_id, Some(raw.id));
        }
    }

    #[test]
    fn postgres_parity_events() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let source = build_entity(tenant.id, &format!("{suffix}-source"), "aion:Sensor");
        let target = build_entity(tenant.id, &format!("{suffix}-target"), "aion:Pump");
        let raw = RawMessage::new(
            tenant.id,
            aion_raw_message::RawMessageSource::Http,
            Some("/ingest/http".to_string()),
            Some("device-evt".to_string()),
            Some("json_mapping".to_string()),
            Some("application/json".to_string()),
            Some(source.id),
            Some(target.id),
            Some("json_mapping".to_string()),
            json!({"source": "events"}),
            br#"{"state":"on"}"#.to_vec(),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap(),
        )
        .expect("raw message");
        let command_id = Uuid::new_v4();

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store.create_entity(source.clone()).expect("create source");
            store.create_entity(target.clone()).expect("create target");
        }
        for store in [
            &in_memory as &dyn RawMessageStore,
            &pg as &dyn RawMessageStore,
        ] {
            store.store_raw_message(raw.clone()).expect("store raw");
        }

        seed_command_row(tenant.id, command_id, target.id, "StartPump");

        let event = build_event(
            tenant.id,
            "aion:CommandCreated",
            EventSeverity::Info,
            Some(source.id),
            Some(target.id),
            Some("command created"),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 1).unwrap(),
            Some(Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 1).unwrap()),
            Some("corr-001"),
            Some(raw.id),
            Some(command_id),
        );

        for store in [&in_memory as &dyn EventStore, &pg as &dyn EventStore] {
            assert_eq!(store.store_event(event.clone()).unwrap(), event);
            assert_eq!(
                store.get_event(tenant.id, event.id).unwrap().unwrap(),
                event
            );

            let by_type = store
                .query_events(
                    tenant.id,
                    EventFilter {
                        event_type: Some("aion:CommandCreated".to_string()),
                        severity: Some(EventSeverity::Info),
                        command_id: Some(command_id),
                        raw_message_id: Some(raw.id),
                        correlation_id: Some("corr-001".to_string()),
                        ..EventFilter::default()
                    },
                )
                .unwrap();
            assert_eq!(by_type, vec![event.clone()]);
        }
    }

    fn build_command(
        tenant_id: Uuid,
        target_entity_id: Uuid,
        command_type: &str,
        approval_status: Option<ApprovalStatus>,
        now: chrono::DateTime<Utc>,
    ) -> Command {
        let mut command = Command::new(
            tenant_id,
            target_entity_id,
            command_type,
            json!({"target_state": "on"}),
            Some("operator".to_string()),
            Some("rule generated".to_string()),
            approval_status,
            Some(json!({"source": "postgres"})),
            now,
        )
        .expect("valid command");
        command.created_at = now;
        command.updated_at = now;
        command
    }

    #[test]
    fn postgres_parity_commands_actions_and_results() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let target = build_entity(tenant.id, &format!("{suffix}-target"), "aion:Pump");
        let executor_entity =
            build_entity(tenant.id, &format!("{suffix}-executor"), "aion:Controller");

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store.create_entity(target.clone()).expect("create target");
            store
                .create_entity(executor_entity.clone())
                .expect("create executor entity");
        }

        let base = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let mut approved_command = build_command(
            tenant.id,
            target.id,
            "StartPump",
            Some(ApprovalStatus::Required),
            base,
        );
        let mut claimed_command = build_command(
            tenant.id,
            target.id,
            "ClaimPump",
            Some(ApprovalStatus::NotRequired),
            base + chrono::Duration::seconds(1),
        );
        let mut executed_command = build_command(
            tenant.id,
            target.id,
            "ExecutePump",
            Some(ApprovalStatus::NotRequired),
            base + chrono::Duration::seconds(2),
        );
        let mut failed_command = build_command(
            tenant.id,
            target.id,
            "FailPump",
            Some(ApprovalStatus::NotRequired),
            base + chrono::Duration::seconds(3),
        );
        let mut cancelled_command = build_command(
            tenant.id,
            target.id,
            "CancelPump",
            Some(ApprovalStatus::NotRequired),
            base + chrono::Duration::seconds(4),
        );
        let mut retry_command = build_command(
            tenant.id,
            target.id,
            "RetryPump",
            Some(ApprovalStatus::NotRequired),
            base + chrono::Duration::seconds(5),
        );
        retry_command.max_retries = Some(3);

        for store in [&in_memory as &dyn CommandStore, &pg as &dyn CommandStore] {
            assert_eq!(
                store.store_command(approved_command.clone()).unwrap(),
                approved_command
            );
            assert_eq!(
                store.store_command(claimed_command.clone()).unwrap(),
                claimed_command
            );
            assert_eq!(
                store.store_command(executed_command.clone()).unwrap(),
                executed_command
            );
            assert_eq!(
                store.store_command(failed_command.clone()).unwrap(),
                failed_command
            );
            assert_eq!(
                store.store_command(cancelled_command.clone()).unwrap(),
                cancelled_command
            );
            assert_eq!(
                store.store_command(retry_command.clone()).unwrap(),
                retry_command
            );

            let listed = store
                .query_commands(tenant.id, Some(target.id), None)
                .expect("query commands by target");
            assert_eq!(
                listed.iter().map(|command| command.id).collect::<Vec<_>>(),
                vec![
                    retry_command.id,
                    cancelled_command.id,
                    failed_command.id,
                    executed_command.id,
                    claimed_command.id,
                    approved_command.id,
                ]
            );

            let mut updated = approved_command.clone();
            updated
                .approve(base + chrono::Duration::seconds(10))
                .unwrap();
            assert_eq!(store.update_command(updated.clone()).unwrap(), updated);
            approved_command = updated;

            let mut updated = claimed_command.clone();
            updated
                .claim("edge-agent-01", base + chrono::Duration::seconds(11))
                .unwrap();
            updated.set_lease_expires_at(
                Some(base + chrono::Duration::seconds(71)),
                base + chrono::Duration::seconds(11),
            );
            assert_eq!(store.update_command(updated.clone()).unwrap(), updated);
            claimed_command = updated;

            let mut updated = executed_command.clone();
            updated
                .claim("edge-agent-01", base + chrono::Duration::seconds(12))
                .unwrap();
            updated.set_lease_expires_at(
                Some(base + chrono::Duration::seconds(72)),
                base + chrono::Duration::seconds(12),
            );
            updated
                .mark_executed(base + chrono::Duration::seconds(13))
                .unwrap();
            assert_eq!(store.update_command(updated.clone()).unwrap(), updated);
            executed_command = updated;

            let mut updated = failed_command.clone();
            updated
                .claim("edge-agent-01", base + chrono::Duration::seconds(14))
                .unwrap();
            updated.set_lease_expires_at(
                Some(base + chrono::Duration::seconds(74)),
                base + chrono::Duration::seconds(14),
            );
            updated
                .mark_failed("pump jammed", base + chrono::Duration::seconds(15))
                .unwrap();
            assert_eq!(store.update_command(updated.clone()).unwrap(), updated);
            failed_command = updated;

            let mut updated = cancelled_command.clone();
            updated
                .cancel(base + chrono::Duration::seconds(16))
                .unwrap();
            assert_eq!(store.update_command(updated.clone()).unwrap(), updated);
            cancelled_command = updated;

            let mut updated = retry_command.clone();
            updated
                .claim("edge-agent-01", base + chrono::Duration::seconds(17))
                .unwrap();
            updated.set_lease_expires_at(
                Some(base + chrono::Duration::seconds(77)),
                base + chrono::Duration::seconds(17),
            );
            updated.schedule_retry(base + chrono::Duration::seconds(18));
            updated.set_lease_expires_at(
                Some(base + chrono::Duration::seconds(78)),
                base + chrono::Duration::seconds(18),
            );
            assert_eq!(store.update_command(updated.clone()).unwrap(), updated);
            retry_command = updated;

            assert_eq!(
                store
                    .get_command(tenant.id, approved_command.id)
                    .unwrap()
                    .unwrap(),
                approved_command
            );
            assert_eq!(
                store
                    .get_command(tenant.id, claimed_command.id)
                    .unwrap()
                    .unwrap(),
                claimed_command
            );
            assert_eq!(
                store
                    .get_command(tenant.id, executed_command.id)
                    .unwrap()
                    .unwrap(),
                executed_command
            );
            assert_eq!(
                store
                    .get_command(tenant.id, failed_command.id)
                    .unwrap()
                    .unwrap(),
                failed_command
            );
            assert_eq!(
                store
                    .get_command(tenant.id, cancelled_command.id)
                    .unwrap()
                    .unwrap(),
                cancelled_command
            );
            assert_eq!(
                store
                    .get_command(tenant.id, retry_command.id)
                    .unwrap()
                    .unwrap(),
                retry_command
            );

            assert_eq!(
                store
                    .query_commands(tenant.id, Some(target.id), Some(CommandStatus::Pending))
                    .unwrap()
                    .iter()
                    .map(|command| command.id)
                    .collect::<Vec<_>>(),
                vec![retry_command.id, approved_command.id]
            );
            assert_eq!(
                store
                    .query_commands(tenant.id, Some(target.id), Some(CommandStatus::Claimed))
                    .unwrap()
                    .iter()
                    .map(|command| command.id)
                    .collect::<Vec<_>>(),
                vec![claimed_command.id]
            );
            assert_eq!(
                store
                    .query_commands(tenant.id, Some(target.id), Some(CommandStatus::Executed))
                    .unwrap()
                    .iter()
                    .map(|command| command.id)
                    .collect::<Vec<_>>(),
                vec![executed_command.id]
            );
            assert_eq!(
                store
                    .query_commands(tenant.id, Some(target.id), Some(CommandStatus::Failed))
                    .unwrap()
                    .iter()
                    .map(|command| command.id)
                    .collect::<Vec<_>>(),
                vec![failed_command.id]
            );
            assert_eq!(
                store
                    .query_commands(tenant.id, Some(target.id), Some(CommandStatus::Cancelled))
                    .unwrap()
                    .iter()
                    .map(|command| command.id)
                    .collect::<Vec<_>>(),
                vec![cancelled_command.id]
            );

            let action = Action::new(
                tenant.id,
                executed_command.id,
                Some(executor_entity.id),
                "StartPump",
                "started",
                Some(base + chrono::Duration::seconds(12)),
                None,
                Some(json!({"source": "postgres"})),
            )
            .expect("valid action");
            for store in [&in_memory as &dyn ActionStore, &pg as &dyn ActionStore] {
                assert_eq!(store.store_action(action.clone()).unwrap(), action);
                assert_eq!(
                    store.get_action(tenant.id, action.id).unwrap().unwrap(),
                    action
                );
                assert_eq!(
                    store
                        .query_actions(tenant.id, Some(executed_command.id))
                        .unwrap(),
                    vec![action.clone()]
                );
            }

            let result = ActionResult::new(
                tenant.id,
                executed_command.id,
                action.id,
                "succeeded",
                true,
                json!({"pump_state": "running"}),
                base + chrono::Duration::seconds(13),
                Some(json!({"source": "postgres"})),
            )
            .expect("valid action result");
            for store in [
                &in_memory as &dyn ActionResultStore,
                &pg as &dyn ActionResultStore,
            ] {
                assert_eq!(store.store_action_result(result.clone()).unwrap(), result);
                assert_eq!(
                    store
                        .query_action_results(tenant.id, Some(action.id), None)
                        .unwrap(),
                    vec![result.clone()]
                );
                assert_eq!(
                    store
                        .query_action_results(tenant.id, None, Some(executed_command.id))
                        .unwrap(),
                    vec![result.clone()]
                );
            }
        }
    }

    #[test]
    fn postgres_parity_command_leases() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let command_target = build_entity(tenant.id, &format!("{suffix}-target"), "aion:Pump");
        let executor_entity =
            build_entity(tenant.id, &format!("{suffix}-executor"), "aion:Controller");

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store
                .create_entity(command_target.clone())
                .expect("create target");
            store
                .create_entity(executor_entity.clone())
                .expect("create executor entity");
        }

        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let command = build_command(
            tenant.id,
            command_target.id,
            "StartPump",
            Some(ApprovalStatus::Approved),
            now,
        );
        let lease = CommandLease::new(
            tenant.id,
            command.id,
            executor_entity.id,
            now,
            now + chrono::Duration::seconds(60),
            Some(json!({"source": "postgres"})),
        )
        .expect("valid lease");

        for store in [
            &in_memory as &dyn CommandLeaseStore,
            &pg as &dyn CommandLeaseStore,
        ] {
            assert_eq!(store.store_command_lease(lease.clone()).unwrap(), lease);
            assert_eq!(
                store
                    .get_command_lease(tenant.id, lease.id)
                    .unwrap()
                    .unwrap(),
                lease
            );
            assert_eq!(
                store
                    .get_active_command_lease(tenant.id, command.id)
                    .unwrap()
                    .unwrap(),
                lease
            );
            assert_eq!(
                store.list_active_command_leases(tenant.id).unwrap(),
                vec![lease.clone()]
            );

            let mut refreshed = lease.clone();
            refreshed
                .refresh(
                    now + chrono::Duration::seconds(90),
                    now + chrono::Duration::seconds(15),
                )
                .expect("refresh lease");
            assert_eq!(
                store.update_command_lease(refreshed.clone()).unwrap(),
                refreshed
            );
            assert_eq!(
                store
                    .get_latest_command_lease(tenant.id, command.id)
                    .unwrap()
                    .unwrap(),
                refreshed
            );

            let mut released = refreshed.clone();
            released.mark_released(now + chrono::Duration::seconds(20));
            assert_eq!(
                store.update_command_lease(released.clone()).unwrap(),
                released
            );
            assert!(store
                .get_active_command_lease(tenant.id, command.id)
                .unwrap()
                .is_none());
            assert!(store
                .list_active_command_leases(tenant.id)
                .unwrap()
                .is_empty());
            assert_eq!(
                store
                    .get_latest_command_lease(tenant.id, command.id)
                    .unwrap()
                    .unwrap(),
                released
            );
        }
    }

    #[test]
    fn postgres_parity_rules() {
        let Some(pg) = postgres_test_storage() else {
            return;
        };
        let in_memory = InMemoryStorage::new();
        let suffix = unique_suffix();
        let tenant = build_tenant(&suffix);
        let target = build_entity(tenant.id, &format!("{suffix}-target"), "aion:Pump");

        for store in [&in_memory as &dyn TenantStore, &pg as &dyn TenantStore] {
            store.create_tenant(tenant.clone()).expect("create tenant");
        }
        for store in [&in_memory as &dyn EntityStore, &pg as &dyn EntityStore] {
            store.create_entity(target.clone()).expect("create target");
        }

        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let mut observation_rule = Rule::new(
            tenant.id,
            "Low water",
            Some("trigger irrigation".to_string()),
            true,
            RuleTriggerType::ObservationCreated,
            Some(target.id),
            Some("WaterTankLevel".to_string()),
            None,
            aion_rule::RuleCondition {
                comparison: aion_rule::RuleComparison::LessThan,
                value: json!(20),
            },
            aion_rule::RuleAction::CreateCommand {
                target_entity_id: target.id,
                command_type: "StartPump".to_string(),
                payload: json!({"target_state": "on"}),
                requested_by: Some("rule-engine".to_string()),
                reason: Some("water level low".to_string()),
                metadata: Some(json!({"suite": "postgres"})),
            },
            Some(json!({"source": "postgres"})),
            now,
        )
        .expect("valid observation rule");
        let mut event_rule = Rule::new(
            tenant.id,
            "Pump fault",
            None,
            true,
            RuleTriggerType::EventCreated,
            Some(target.id),
            None,
            Some("aion:PumpFault".to_string()),
            aion_rule::RuleCondition {
                comparison: aion_rule::RuleComparison::Equals,
                value: json!("critical"),
            },
            aion_rule::RuleAction::CreateEvent {
                event_type: "aion:PumpInspectionRequested".to_string(),
                severity: EventSeverity::Warning,
                source_entity_id: Some(target.id),
                target_entity_id: Some(target.id),
                message: Some("inspect pump".to_string()),
                metadata: Some(json!({"suite": "postgres"})),
            },
            Some(json!({"source": "postgres"})),
            now + chrono::Duration::seconds(1),
        )
        .expect("valid event rule");
        observation_rule.created_at = now;
        observation_rule.updated_at = now;
        event_rule.created_at = now + chrono::Duration::seconds(1);
        event_rule.updated_at = now + chrono::Duration::seconds(1);

        for store in [&in_memory as &dyn RuleStore, &pg as &dyn RuleStore] {
            assert_eq!(
                store.store_rule(observation_rule.clone()).unwrap(),
                observation_rule
            );
            assert_eq!(store.store_rule(event_rule.clone()).unwrap(), event_rule);
            assert_eq!(
                store
                    .get_rule(tenant.id, observation_rule.id)
                    .unwrap()
                    .unwrap(),
                observation_rule
            );
            assert_eq!(
                store.get_rule(tenant.id, event_rule.id).unwrap().unwrap(),
                event_rule
            );

            let listed = store.list_rules(tenant.id).unwrap();
            assert_eq!(
                listed.iter().map(|rule| rule.id).collect::<Vec<_>>(),
                vec![observation_rule.id, event_rule.id]
            );

            let mut disabled = observation_rule.clone();
            disabled.set_enabled(false, now + chrono::Duration::seconds(5));
            assert_eq!(store.update_rule(disabled.clone()).unwrap(), disabled);
            assert!(
                !store
                    .get_rule(tenant.id, observation_rule.id)
                    .unwrap()
                    .unwrap()
                    .enabled
            );
        }
    }
}
