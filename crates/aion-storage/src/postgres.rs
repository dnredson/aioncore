use super::*;
use ::postgres::error::SqlState;
use ::postgres::types::{Json, ToSql};
use ::postgres::{Client, Config as PgConfig, NoTls, Row};
use aion_action::ExecutorAgentStatus;
use aion_event::EventSeverity;
use aion_observation::ObservationValue;
use aion_raw_message::{NormalizationStatus, RawMessageSource};
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
}
