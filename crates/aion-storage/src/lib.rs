use aion_action::{
    Action, ActionResult, Capability, Command, CommandLease, CommandLeaseStatus, CommandStatus,
    EdgeAdapter, EdgeAdapterStatusReport, ExecutorAgent, ExecutorCapability, ExecutorScope, Policy,
};
use aion_dlq::{DlqFailureStage, DlqRecord, DlqStatus};
use aion_entity::Entity;
use aion_event::{Event, EventSeverity};
use aion_flow::Flow;
use aion_observation::Observation;
use aion_raw_message::RawMessage;
use aion_relationship::Relationship;
use aion_rule::Rule;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, RwLock},
};
use uuid::Uuid;

mod postgres;

pub use postgres::{PostgresStorage, PostgresStorageConfig};

pub const MIGRATION_0001_CREATE_TENANTS: &str =
    include_str!("../../../migrations/0001_create_tenants.sql");
pub const MIGRATION_0002_CREATE_ENTITIES: &str =
    include_str!("../../../migrations/0002_create_entities.sql");
pub const MIGRATION_0003_CREATE_ENTITY_RELATIONSHIPS: &str =
    include_str!("../../../migrations/0003_create_entity_relationships.sql");
pub const MIGRATION_0004_CREATE_RAW_MESSAGES: &str =
    include_str!("../../../migrations/0004_create_raw_messages.sql");
pub const MIGRATION_0005_CREATE_OBSERVATIONS: &str =
    include_str!("../../../migrations/0005_create_observations.sql");
pub const MIGRATION_0006_CREATE_RUNTIME_PERSISTENCE_TABLES: &str =
    include_str!("../../../migrations/0006_create_runtime_persistence_tables.sql");
pub const MIGRATION_0007_CREATE_INGESTION_CONNECTORS: &str =
    include_str!("../../../migrations/0007_create_ingestion_connectors.sql");
pub const MIGRATION_0008_CREATE_CONNECTOR_SECRETS: &str =
    include_str!("../../../migrations/0008_create_connector_secrets.sql");
pub const MIGRATION_0009_CREATE_TTN_DEVICE_MAPPINGS: &str =
    include_str!("../../../migrations/0009_create_ttn_device_mappings.sql");
pub const MIGRATION_0010_HARDEN_TTN_DEVICE_MAPPING_UNIQUENESS: &str =
    include_str!("../../../migrations/0010_harden_ttn_device_mapping_uniqueness.sql");
pub const MIGRATION_0011_CREATE_EDGE_ADAPTERS: &str =
    include_str!("../../../migrations/0011_create_edge_adapters.sql");
pub const MIGRATION_0012_CREATE_API_TOKENS: &str =
    include_str!("../../../migrations/0012_create_api_tokens.sql");
pub const MIGRATION_0013_CREATE_FLOWS: &str =
    include_str!("../../../migrations/0013_create_flows.sql");
pub const MIGRATION_0014_CREATE_DLQ_RECORDS: &str =
    include_str!("../../../migrations/0014_create_dlq_records.sql");
pub const MIGRATION_0015_ADD_RAW_MESSAGE_IDEMPOTENCY: &str =
    include_str!("../../../migrations/0015_add_raw_message_idempotency.sql");

pub const ORDERED_MIGRATIONS: &[(&str, &str)] = &[
    ("0001_create_tenants.sql", MIGRATION_0001_CREATE_TENANTS),
    ("0002_create_entities.sql", MIGRATION_0002_CREATE_ENTITIES),
    (
        "0003_create_entity_relationships.sql",
        MIGRATION_0003_CREATE_ENTITY_RELATIONSHIPS,
    ),
    (
        "0004_create_raw_messages.sql",
        MIGRATION_0004_CREATE_RAW_MESSAGES,
    ),
    (
        "0005_create_observations.sql",
        MIGRATION_0005_CREATE_OBSERVATIONS,
    ),
    (
        "0006_create_runtime_persistence_tables.sql",
        MIGRATION_0006_CREATE_RUNTIME_PERSISTENCE_TABLES,
    ),
    (
        "0007_create_ingestion_connectors.sql",
        MIGRATION_0007_CREATE_INGESTION_CONNECTORS,
    ),
    (
        "0008_create_connector_secrets.sql",
        MIGRATION_0008_CREATE_CONNECTOR_SECRETS,
    ),
    (
        "0009_create_ttn_device_mappings.sql",
        MIGRATION_0009_CREATE_TTN_DEVICE_MAPPINGS,
    ),
    (
        "0010_harden_ttn_device_mapping_uniqueness.sql",
        MIGRATION_0010_HARDEN_TTN_DEVICE_MAPPING_UNIQUENESS,
    ),
    (
        "0011_create_edge_adapters.sql",
        MIGRATION_0011_CREATE_EDGE_ADAPTERS,
    ),
    (
        "0012_create_api_tokens.sql",
        MIGRATION_0012_CREATE_API_TOKENS,
    ),
    ("0013_create_flows.sql", MIGRATION_0013_CREATE_FLOWS),
    (
        "0014_create_dlq_records.sql",
        MIGRATION_0014_CREATE_DLQ_RECORDS,
    ),
    (
        "0015_add_raw_message_idempotency.sql",
        MIGRATION_0015_ADD_RAW_MESSAGE_IDEMPOTENCY,
    ),
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tenant {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PayloadProfile {
    pub entity_id: Uuid,
    pub payload_format: String,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub attribute_mapping: Option<Value>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IngestionConnectorType {
    Http,
    Mqtt,
    Future,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConnectorProfile {
    GenericAionMqtt,
    GenericMqtt,
    TtnV3,
    Custom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorSecretType {
    MqttBasicAuth,
    Token,
    ApiKey,
    Custom,
}

#[derive(Clone, PartialEq, Deserialize)]
pub struct ConnectorSecret {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub secret_key: String,
    pub secret_type: ConnectorSecretType,
    pub username: Option<String>,
    pub secret_value: String,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl fmt::Debug for ConnectorSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorSecret")
            .field("id", &self.id)
            .field("tenant_id", &self.tenant_id)
            .field("secret_key", &self.secret_key)
            .field("secret_type", &self.secret_type)
            .field("username", &self.username)
            .field("secret_value", &"***REDACTED***")
            .field("metadata", &self.metadata)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

impl ConnectorSecret {
    pub fn new(
        tenant_id: Uuid,
        secret_key: impl Into<String>,
        secret_type: ConnectorSecretType,
        username: Option<String>,
        secret_value: impl Into<String>,
        metadata: Option<Value>,
        now: DateTime<Utc>,
    ) -> StorageResult<Self> {
        let secret_key = secret_key.into();
        if secret_key.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "secret_key must not be empty".to_string(),
            ));
        }
        let secret_value = secret_value.into();
        if secret_value.is_empty() {
            return Err(StorageError::InvalidInput(
                "secret_value must not be empty".to_string(),
            ));
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            secret_key,
            secret_type,
            username,
            secret_value,
            metadata,
            created_at: now,
            updated_at: now,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiTokenPrincipalType {
    User,
    Device,
    Adapter,
    Executor,
    Connector,
    Service,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiToken {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub token_name: String,
    pub token_prefix: String,
    pub token_hash: String,
    pub principal_type: ApiTokenPrincipalType,
    pub principal_id: Option<String>,
    pub scopes: Vec<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ApiToken {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        token_name: impl Into<String>,
        token_prefix: impl Into<String>,
        token_hash: impl Into<String>,
        principal_type: ApiTokenPrincipalType,
        principal_id: Option<String>,
        scopes: Vec<String>,
        expires_at: Option<DateTime<Utc>>,
        metadata: Option<Value>,
        now: DateTime<Utc>,
    ) -> StorageResult<Self> {
        let token_name = token_name.into();
        if token_name.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "token_name must not be empty".to_string(),
            ));
        }

        let token_prefix = token_prefix.into();
        if token_prefix.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "token_prefix must not be empty".to_string(),
            ));
        }

        let token_hash = token_hash.into();
        if token_hash.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "token_hash must not be empty".to_string(),
            ));
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            token_name,
            token_prefix,
            token_hash,
            principal_type,
            principal_id: principal_id
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            scopes,
            expires_at,
            revoked_at: None,
            last_used_at: None,
            metadata,
            created_at: now,
            updated_at: now,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestionConnector {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: ConnectorProfile,
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl IngestionConnector {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        connector_key: impl Into<String>,
        connector_type: IngestionConnectorType,
        connector_profile: ConnectorProfile,
        enabled: bool,
        display_name: Option<String>,
        protocol: Option<String>,
        endpoint: Option<String>,
        broker_url: Option<String>,
        client_id: Option<String>,
        topic_filter: Option<String>,
        http_path: Option<String>,
        payload_format: Option<String>,
        content_type: Option<String>,
        default_producer_entity_id: Option<Uuid>,
        default_feature_of_interest_id: Option<Uuid>,
        metadata: Option<Value>,
        now: DateTime<Utc>,
    ) -> StorageResult<Self> {
        let connector_key = connector_key.into();
        if connector_key.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "connector_key must not be empty".to_string(),
            ));
        }

        Ok(Self {
            id: Uuid::new_v4(),
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
            secret_ref_id: None,
            default_producer_entity_id,
            default_feature_of_interest_id,
            metadata,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn set_enabled(&mut self, enabled: bool, now: DateTime<Utc>) {
        self.enabled = enabled;
        self.updated_at = now;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TtnDeviceMapping {
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

impl TtnDeviceMapping {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        connector_id: Uuid,
        ttn_application_id: Option<String>,
        ttn_device_id: impl Into<String>,
        producer_entity_id: Uuid,
        feature_of_interest_id: Option<Uuid>,
        enabled: bool,
        metadata: Option<Value>,
        now: DateTime<Utc>,
    ) -> StorageResult<Self> {
        let ttn_device_id = ttn_device_id.into();
        if ttn_device_id.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "ttn_device_id must not be empty".to_string(),
            ));
        }
        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            connector_id,
            ttn_application_id: ttn_application_id.and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }),
            ttn_device_id,
            producer_entity_id,
            feature_of_interest_id,
            enabled,
            metadata,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn update_fields(
        &mut self,
        ttn_application_id: Option<Option<String>>,
        ttn_device_id: Option<String>,
        producer_entity_id: Option<Uuid>,
        feature_of_interest_id: Option<Option<Uuid>>,
        enabled: Option<bool>,
        metadata: Option<Option<Value>>,
        now: DateTime<Utc>,
    ) -> StorageResult<()> {
        if let Some(application_id) = ttn_application_id {
            self.ttn_application_id = application_id.and_then(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            });
        }
        if let Some(device_id) = ttn_device_id {
            if device_id.trim().is_empty() {
                return Err(StorageError::InvalidInput(
                    "ttn_device_id must not be empty".to_string(),
                ));
            }
            self.ttn_device_id = device_id;
        }
        if let Some(producer_entity_id) = producer_entity_id {
            self.producer_entity_id = producer_entity_id;
        }
        if let Some(feature_of_interest_id) = feature_of_interest_id {
            self.feature_of_interest_id = feature_of_interest_id;
        }
        if let Some(enabled) = enabled {
            self.enabled = enabled;
        }
        if let Some(metadata) = metadata {
            self.metadata = metadata;
        }
        self.updated_at = now;
        Ok(())
    }

    pub fn set_enabled(&mut self, enabled: bool, now: DateTime<Utc>) {
        self.enabled = enabled;
        self.updated_at = now;
    }
}

fn validate_ttn_device_mapping_conflict<'a>(
    mappings: impl Iterator<Item = &'a TtnDeviceMapping>,
    candidate: &TtnDeviceMapping,
) -> StorageResult<()> {
    if !candidate.enabled {
        return Ok(());
    }

    for mapping in mappings {
        if mapping.id == candidate.id
            || mapping.tenant_id != candidate.tenant_id
            || mapping.connector_id != candidate.connector_id
            || !mapping.enabled
            || mapping.ttn_device_id != candidate.ttn_device_id
        {
            continue;
        }

        if mapping.ttn_application_id == candidate.ttn_application_id {
            let scope = candidate
                .ttn_application_id
                .as_deref()
                .map(|application_id| format!("application '{application_id}'"))
                .unwrap_or_else(|| "fallback device".to_string());
            return Err(StorageError::ConflictWithMessage(format!(
                "enabled TTN mapping conflict for connector {}, device '{}', {scope}",
                candidate.connector_id, candidate.ttn_device_id
            )));
        }
    }

    Ok(())
}

impl PayloadProfile {
    pub fn new(
        entity_id: Uuid,
        payload_format: impl Into<String>,
        protocol: Option<String>,
        content_type: Option<String>,
        attribute_mapping: Option<Value>,
        metadata: Option<Value>,
    ) -> StorageResult<Self> {
        let payload_format = payload_format.into();
        if payload_format.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "payload_format must not be empty".to_string(),
            ));
        }

        Ok(Self {
            entity_id,
            payload_format,
            protocol,
            content_type,
            attribute_mapping,
            metadata,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    NotFound,
    Conflict,
    ConflictWithMessage(String),
    InvalidInput(String),
    Backend(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("record was not found"),
            Self::Conflict => f.write_str("record conflicts with existing data"),
            Self::ConflictWithMessage(message) => f.write_str(message),
            Self::InvalidInput(message) => write!(f, "invalid input: {message}"),
            Self::Backend(message) => write!(f, "storage backend error: {message}"),
        }
    }
}

impl std::error::Error for StorageError {}

pub type StorageResult<T> = Result<T, StorageError>;

pub trait TenantStore {
    fn create_tenant(&self, tenant: Tenant) -> StorageResult<Tenant>;
    fn get_tenant(&self, tenant_id: Uuid) -> StorageResult<Option<Tenant>>;
    fn get_tenant_by_slug(&self, slug: &str) -> StorageResult<Option<Tenant>>;
}

pub trait EntityStore {
    fn create_entity(&self, entity: Entity) -> StorageResult<Entity>;
    fn update_entity(&self, entity: Entity) -> StorageResult<Entity>;
    fn get_entity(&self, tenant_id: Uuid, entity_id: Uuid) -> StorageResult<Option<Entity>>;
    fn get_entity_any_tenant(&self, entity_id: Uuid) -> StorageResult<Option<Entity>>;
    fn get_entity_by_key(&self, tenant_id: Uuid, entity_key: &str)
        -> StorageResult<Option<Entity>>;
    fn list_entities(&self, tenant_id: Uuid) -> StorageResult<Vec<Entity>>;
    fn list_all_entities(&self) -> StorageResult<Vec<Entity>>;
}

pub trait RelationshipStore {
    fn create_relationship(&self, relationship: Relationship) -> StorageResult<Relationship>;
    fn list_relationships(
        &self,
        tenant_id: Uuid,
        source_entity_id: Option<Uuid>,
        target_entity_id: Option<Uuid>,
    ) -> StorageResult<Vec<Relationship>>;
}

pub trait RawMessageStore {
    fn store_raw_message(&self, raw_message: RawMessage) -> StorageResult<RawMessage>;
    fn find_raw_message_by_idempotency_key(
        &self,
        tenant_id: Uuid,
        idempotency_key: &str,
    ) -> StorageResult<Option<RawMessage>>;
    fn get_raw_message(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
    ) -> StorageResult<Option<RawMessage>>;
    fn get_raw_message_any_tenant(&self, raw_message_id: Uuid)
        -> StorageResult<Option<RawMessage>>;
    fn list_raw_messages(&self, tenant_id: Uuid) -> StorageResult<Vec<RawMessage>>;
    fn list_all_raw_messages(&self) -> StorageResult<Vec<RawMessage>>;
    fn query_raw_messages(
        &self,
        tenant_id: Uuid,
        producer_entity_id: Option<Uuid>,
        feature_of_interest_id: Option<Uuid>,
        payload_format: Option<&str>,
    ) -> StorageResult<Vec<RawMessage>>;
    fn mark_raw_message_normalized(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
    ) -> StorageResult<()>;
    fn mark_raw_message_failed(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
        error: &str,
    ) -> StorageResult<()>;
}

pub trait ObservationStore {
    fn store_observation(&self, observation: Observation) -> StorageResult<Observation>;
    fn get_observation(
        &self,
        tenant_id: Uuid,
        observation_id: Uuid,
    ) -> StorageResult<Option<Observation>>;
    fn get_observation_any_tenant(
        &self,
        observation_id: Uuid,
    ) -> StorageResult<Option<Observation>>;
    fn query_observations(
        &self,
        tenant_id: Uuid,
        feature_of_interest_id: Option<Uuid>,
        observed_property: Option<&str>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: u32,
    ) -> StorageResult<Vec<Observation>>;
    fn query_observations_chronological(
        &self,
        tenant_id: Uuid,
        feature_of_interest_id: Option<Uuid>,
        observed_property: Option<&str>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: u32,
    ) -> StorageResult<Vec<Observation>>;
    fn list_all_observations(&self) -> StorageResult<Vec<Observation>>;
}

pub trait PayloadProfileStore {
    fn put_payload_profile(
        &self,
        tenant_id: Uuid,
        profile: PayloadProfile,
    ) -> StorageResult<PayloadProfile>;
    fn get_payload_profile(
        &self,
        tenant_id: Uuid,
        entity_id: Uuid,
    ) -> StorageResult<Option<PayloadProfile>>;
}

pub trait IngestionConnectorStore {
    fn create_ingestion_connector(
        &self,
        connector: IngestionConnector,
    ) -> StorageResult<IngestionConnector>;
    fn get_ingestion_connector(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
    ) -> StorageResult<Option<IngestionConnector>>;
    fn list_ingestion_connectors(&self, tenant_id: Uuid) -> StorageResult<Vec<IngestionConnector>>;
    fn list_all_ingestion_connectors(&self) -> StorageResult<Vec<IngestionConnector>>;
    fn update_ingestion_connector(
        &self,
        connector: IngestionConnector,
    ) -> StorageResult<IngestionConnector>;
}

pub trait ConnectorSecretStore {
    fn create_connector_secret(&self, secret: ConnectorSecret) -> StorageResult<ConnectorSecret>;
    fn get_connector_secret(
        &self,
        tenant_id: Uuid,
        secret_id: Uuid,
    ) -> StorageResult<Option<ConnectorSecret>>;
    fn list_connector_secrets(&self, tenant_id: Uuid) -> StorageResult<Vec<ConnectorSecret>>;
    fn delete_connector_secret(&self, tenant_id: Uuid, secret_id: Uuid) -> StorageResult<()>;
}

pub trait ApiTokenStore {
    fn create_api_token(&self, token: ApiToken) -> StorageResult<ApiToken>;
    fn get_api_token(&self, tenant_id: Uuid, token_id: Uuid) -> StorageResult<Option<ApiToken>>;
    fn list_api_tokens(&self, tenant_id: Uuid) -> StorageResult<Vec<ApiToken>>;
    fn find_api_token_by_prefix(
        &self,
        tenant_id: Uuid,
        token_prefix: &str,
    ) -> StorageResult<Option<ApiToken>>;
    fn find_api_token_by_prefix_any_tenant(
        &self,
        token_prefix: &str,
    ) -> StorageResult<Option<ApiToken>>;
    fn update_api_token_last_used_at(
        &self,
        tenant_id: Uuid,
        token_id: Uuid,
        last_used_at: DateTime<Utc>,
    ) -> StorageResult<ApiToken>;
    fn revoke_api_token(
        &self,
        tenant_id: Uuid,
        token_id: Uuid,
        revoked_at: DateTime<Utc>,
    ) -> StorageResult<ApiToken>;
}

pub trait TtnDeviceMappingStore {
    fn create_ttn_device_mapping(
        &self,
        mapping: TtnDeviceMapping,
    ) -> StorageResult<TtnDeviceMapping>;
    fn get_ttn_device_mapping(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
        mapping_id: Uuid,
    ) -> StorageResult<Option<TtnDeviceMapping>>;
    fn list_ttn_device_mappings(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
    ) -> StorageResult<Vec<TtnDeviceMapping>>;
    fn update_ttn_device_mapping(
        &self,
        mapping: TtnDeviceMapping,
    ) -> StorageResult<TtnDeviceMapping>;
    fn delete_ttn_device_mapping(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
        mapping_id: Uuid,
    ) -> StorageResult<()>;
    fn find_ttn_device_mapping(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
        ttn_application_id: Option<&str>,
        ttn_device_id: &str,
    ) -> StorageResult<Option<TtnDeviceMapping>>;
}

pub trait CapabilityStore {
    fn put_capabilities(
        &self,
        tenant_id: Uuid,
        entity_id: Uuid,
        capabilities: Vec<Capability>,
    ) -> StorageResult<Vec<Capability>>;
    fn list_capabilities(&self, tenant_id: Uuid, entity_id: Uuid)
        -> StorageResult<Vec<Capability>>;
}

pub trait ExecutorStore {
    fn create_executor(&self, executor: ExecutorAgent) -> StorageResult<ExecutorAgent>;
    fn update_executor(&self, executor: ExecutorAgent) -> StorageResult<ExecutorAgent>;
    fn get_executor(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
    ) -> StorageResult<Option<ExecutorAgent>>;
    fn get_executor_any_tenant(&self, executor_id: Uuid) -> StorageResult<Option<ExecutorAgent>>;
    fn list_executors(&self, tenant_id: Uuid) -> StorageResult<Vec<ExecutorAgent>>;
    fn list_all_executors(&self) -> StorageResult<Vec<ExecutorAgent>>;
    fn put_executor_capabilities(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
        capabilities: Vec<ExecutorCapability>,
    ) -> StorageResult<Vec<ExecutorCapability>>;
    fn list_executor_capabilities(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
    ) -> StorageResult<Vec<ExecutorCapability>>;
    fn put_executor_scopes(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
        scopes: Vec<ExecutorScope>,
    ) -> StorageResult<Vec<ExecutorScope>>;
    fn list_executor_scopes(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
    ) -> StorageResult<Vec<ExecutorScope>>;
}

pub trait EdgeAdapterStore {
    fn create_edge_adapter(&self, adapter: EdgeAdapter) -> StorageResult<EdgeAdapter>;
    fn update_edge_adapter(&self, adapter: EdgeAdapter) -> StorageResult<EdgeAdapter>;
    fn get_edge_adapter(
        &self,
        tenant_id: Uuid,
        adapter_id: Uuid,
    ) -> StorageResult<Option<EdgeAdapter>>;
    fn get_edge_adapter_by_key(
        &self,
        tenant_id: Uuid,
        adapter_key: &str,
    ) -> StorageResult<Option<EdgeAdapter>>;
    fn list_edge_adapters(&self, tenant_id: Uuid) -> StorageResult<Vec<EdgeAdapter>>;
    fn put_edge_adapter_status(
        &self,
        tenant_id: Uuid,
        status: EdgeAdapterStatusReport,
    ) -> StorageResult<EdgeAdapterStatusReport>;
    fn get_edge_adapter_status(
        &self,
        tenant_id: Uuid,
        adapter_id: Uuid,
    ) -> StorageResult<Option<EdgeAdapterStatusReport>>;
}

pub trait CommandStore {
    fn store_command(&self, command: Command) -> StorageResult<Command>;
    fn update_command(&self, command: Command) -> StorageResult<Command>;
    fn get_command(&self, tenant_id: Uuid, command_id: Uuid) -> StorageResult<Option<Command>>;
    fn get_command_any_tenant(&self, command_id: Uuid) -> StorageResult<Option<Command>>;
    fn query_commands(
        &self,
        tenant_id: Uuid,
        target_entity_id: Option<Uuid>,
        status: Option<CommandStatus>,
    ) -> StorageResult<Vec<Command>>;
    fn list_all_commands(&self) -> StorageResult<Vec<Command>>;
}

pub trait CommandLeaseStore {
    fn store_command_lease(&self, lease: CommandLease) -> StorageResult<CommandLease>;
    fn update_command_lease(&self, lease: CommandLease) -> StorageResult<CommandLease>;
    fn get_command_lease(
        &self,
        tenant_id: Uuid,
        lease_id: Uuid,
    ) -> StorageResult<Option<CommandLease>>;
    fn get_active_command_lease(
        &self,
        tenant_id: Uuid,
        command_id: Uuid,
    ) -> StorageResult<Option<CommandLease>>;
    fn get_latest_command_lease(
        &self,
        tenant_id: Uuid,
        command_id: Uuid,
    ) -> StorageResult<Option<CommandLease>>;
    fn list_active_command_leases(&self, tenant_id: Uuid) -> StorageResult<Vec<CommandLease>>;
}

pub trait PolicyStore {
    fn put_policies(&self, tenant_id: Uuid, policies: Vec<Policy>) -> StorageResult<Vec<Policy>>;
    fn query_policies(
        &self,
        tenant_id: Uuid,
        target_entity_id: Option<Uuid>,
        command_type: Option<&str>,
    ) -> StorageResult<Vec<Policy>>;
    fn list_all_policies(&self) -> StorageResult<Vec<Policy>>;
}

pub trait ActionStore {
    fn store_action(&self, action: Action) -> StorageResult<Action>;
    fn get_action(&self, tenant_id: Uuid, action_id: Uuid) -> StorageResult<Option<Action>>;
    fn get_action_any_tenant(&self, action_id: Uuid) -> StorageResult<Option<Action>>;
    fn query_actions(
        &self,
        tenant_id: Uuid,
        command_id: Option<Uuid>,
    ) -> StorageResult<Vec<Action>>;
    fn list_all_actions(&self) -> StorageResult<Vec<Action>>;
}

pub trait ActionResultStore {
    fn store_action_result(&self, result: ActionResult) -> StorageResult<ActionResult>;
    fn query_action_results(
        &self,
        tenant_id: Uuid,
        action_id: Option<Uuid>,
        command_id: Option<Uuid>,
    ) -> StorageResult<Vec<ActionResult>>;
    fn list_all_action_results(&self) -> StorageResult<Vec<ActionResult>>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventFilter {
    pub source_entity_id: Option<Uuid>,
    pub target_entity_id: Option<Uuid>,
    pub event_type: Option<String>,
    pub severity: Option<EventSeverity>,
    pub command_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub correlation_id: Option<String>,
}

pub trait EventStore {
    fn store_event(&self, event: Event) -> StorageResult<Event>;
    fn get_event(&self, tenant_id: Uuid, event_id: Uuid) -> StorageResult<Option<Event>>;
    fn get_event_any_tenant(&self, event_id: Uuid) -> StorageResult<Option<Event>>;
    fn query_events(&self, tenant_id: Uuid, filter: EventFilter) -> StorageResult<Vec<Event>>;
    fn list_all_events(&self) -> StorageResult<Vec<Event>>;
}

pub trait RuleStore {
    fn store_rule(&self, rule: Rule) -> StorageResult<Rule>;
    fn update_rule(&self, rule: Rule) -> StorageResult<Rule>;
    fn get_rule(&self, tenant_id: Uuid, rule_id: Uuid) -> StorageResult<Option<Rule>>;
    fn get_rule_any_tenant(&self, rule_id: Uuid) -> StorageResult<Option<Rule>>;
    fn list_rules(&self, tenant_id: Uuid) -> StorageResult<Vec<Rule>>;
    fn list_all_rules(&self) -> StorageResult<Vec<Rule>>;
}

pub trait FlowStore {
    fn create_flow(&self, flow: Flow) -> StorageResult<Flow>;
    fn update_flow(&self, flow: Flow) -> StorageResult<Flow>;
    fn get_flow(&self, tenant_id: Uuid, flow_id: Uuid) -> StorageResult<Option<Flow>>;
    fn get_flow_any_tenant(&self, flow_id: Uuid) -> StorageResult<Option<Flow>>;
    fn list_flows(&self, tenant_id: Uuid) -> StorageResult<Vec<Flow>>;
    fn list_all_flows(&self) -> StorageResult<Vec<Flow>>;
    fn delete_flow(&self, tenant_id: Uuid, flow_id: Uuid) -> StorageResult<()>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DlqRecordFilter {
    pub status: Option<DlqStatus>,
    pub failure_stage: Option<DlqFailureStage>,
    pub source_system: Option<String>,
    pub connector_id: Option<Uuid>,
    pub flow_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub idempotency_key: Option<String>,
    pub external_flowfile_uuid: Option<String>,
    pub sync_session_id: Option<String>,
    pub limit: u32,
}

pub trait DlqStore {
    fn create_dlq_record(&self, record: DlqRecord) -> StorageResult<DlqRecord>;
    fn list_dlq_records(
        &self,
        tenant_id: Uuid,
        filter: DlqRecordFilter,
    ) -> StorageResult<Vec<DlqRecord>>;
    fn list_all_dlq_records(&self, filter: DlqRecordFilter) -> StorageResult<Vec<DlqRecord>>;
    fn get_dlq_record(&self, tenant_id: Uuid, record_id: Uuid) -> StorageResult<Option<DlqRecord>>;
    fn get_dlq_record_any_tenant(&self, record_id: Uuid) -> StorageResult<Option<DlqRecord>>;
    fn update_dlq_record_status(
        &self,
        tenant_id: Uuid,
        record_id: Uuid,
        status: DlqStatus,
        now: DateTime<Utc>,
    ) -> StorageResult<DlqRecord>;
}

pub trait ControlPlaneStore:
    TenantStore
    + EntityStore
    + RelationshipStore
    + PayloadProfileStore
    + IngestionConnectorStore
    + ConnectorSecretStore
    + ApiTokenStore
    + TtnDeviceMappingStore
    + EdgeAdapterStore
    + CapabilityStore
    + PolicyStore
    + CommandStore
    + CommandLeaseStore
    + ActionStore
    + ActionResultStore
    + RuleStore
    + FlowStore
    + DlqStore
    + ExecutorStore
{
}

impl<T> ControlPlaneStore for T where
    T: TenantStore
        + EntityStore
        + RelationshipStore
        + PayloadProfileStore
        + IngestionConnectorStore
        + ConnectorSecretStore
        + ApiTokenStore
        + TtnDeviceMappingStore
        + EdgeAdapterStore
        + CapabilityStore
        + PolicyStore
        + CommandStore
        + CommandLeaseStore
        + ActionStore
        + ActionResultStore
        + RuleStore
        + FlowStore
        + DlqStore
        + ExecutorStore
{
}

pub trait TelemetryStore: ObservationStore + RawMessageStore + EventStore {}

impl<T> TelemetryStore for T where T: ObservationStore + RawMessageStore + EventStore {}

pub trait AiContextStore:
    EntityStore
    + RelationshipStore
    + ObservationStore
    + EventStore
    + CommandStore
    + ActionStore
    + ActionResultStore
{
}

impl<T> AiContextStore for T where
    T: EntityStore
        + RelationshipStore
        + ObservationStore
        + EventStore
        + CommandStore
        + ActionStore
        + ActionResultStore
{
}

pub trait StorageBackend:
    ControlPlaneStore + TelemetryStore + AiContextStore + fmt::Debug + Send + Sync
{
    fn check_readiness(&self) -> StorageResult<()>;
}

impl StorageBackend for InMemoryStorage {
    fn check_readiness(&self) -> StorageResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryStorage {
    inner: Arc<RwLock<InMemoryState>>,
}

#[derive(Debug, Default)]
struct InMemoryState {
    tenants: HashMap<Uuid, Tenant>,
    tenant_slug_index: HashMap<String, Uuid>,
    entities: HashMap<Uuid, Entity>,
    entity_key_index: HashMap<(Uuid, String), Uuid>,
    relationships: HashMap<Uuid, Relationship>,
    raw_messages: HashMap<Uuid, RawMessage>,
    raw_message_idempotency_index: HashMap<(Uuid, String), Uuid>,
    observations: HashMap<Uuid, Observation>,
    payload_profiles: HashMap<(Uuid, Uuid), PayloadProfile>,
    ingestion_connectors: HashMap<Uuid, IngestionConnector>,
    ingestion_connector_key_index: HashMap<(Uuid, String), Uuid>,
    flows: HashMap<Uuid, Flow>,
    flow_key_index: HashMap<(Uuid, String), Uuid>,
    dlq_records: HashMap<Uuid, DlqRecord>,
    connector_secrets: HashMap<Uuid, ConnectorSecret>,
    connector_secret_key_index: HashMap<(Uuid, String), Uuid>,
    api_tokens: HashMap<Uuid, ApiToken>,
    api_token_prefix_index: HashMap<(Uuid, String), Uuid>,
    ttn_device_mappings: HashMap<Uuid, TtnDeviceMapping>,
    capabilities: HashMap<(Uuid, Uuid), Vec<Capability>>,
    executors: HashMap<Uuid, ExecutorAgent>,
    executor_key_index: HashMap<(Uuid, String), Uuid>,
    executor_capabilities: HashMap<(Uuid, Uuid), Vec<ExecutorCapability>>,
    executor_scopes: HashMap<(Uuid, Uuid), Vec<ExecutorScope>>,
    edge_adapters: HashMap<Uuid, EdgeAdapter>,
    edge_adapter_key_index: HashMap<(Uuid, String), Uuid>,
    edge_adapter_statuses: HashMap<Uuid, EdgeAdapterStatusReport>,
    commands: HashMap<Uuid, Command>,
    command_leases: HashMap<Uuid, CommandLease>,
    policies: HashMap<Uuid, Policy>,
    actions: HashMap<Uuid, Action>,
    action_results: HashMap<Uuid, ActionResult>,
    events: HashMap<Uuid, Event>,
    rules: HashMap<Uuid, Rule>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    fn read_state(&self) -> StorageResult<std::sync::RwLockReadGuard<'_, InMemoryState>> {
        self.inner
            .read()
            .map_err(|_| StorageError::Backend("in-memory storage lock was poisoned".to_string()))
    }

    fn write_state(&self) -> StorageResult<std::sync::RwLockWriteGuard<'_, InMemoryState>> {
        self.inner
            .write()
            .map_err(|_| StorageError::Backend("in-memory storage lock was poisoned".to_string()))
    }
}

impl TenantStore for InMemoryStorage {
    fn create_tenant(&self, tenant: Tenant) -> StorageResult<Tenant> {
        let mut state = self.write_state()?;

        if state.tenants.contains_key(&tenant.id)
            || state.tenant_slug_index.contains_key(&tenant.slug)
        {
            return Err(StorageError::Conflict);
        }

        state
            .tenant_slug_index
            .insert(tenant.slug.clone(), tenant.id);
        state.tenants.insert(tenant.id, tenant.clone());
        Ok(tenant)
    }

    fn get_tenant(&self, tenant_id: Uuid) -> StorageResult<Option<Tenant>> {
        Ok(self.read_state()?.tenants.get(&tenant_id).cloned())
    }

    fn get_tenant_by_slug(&self, slug: &str) -> StorageResult<Option<Tenant>> {
        let state = self.read_state()?;
        Ok(state
            .tenant_slug_index
            .get(slug)
            .and_then(|tenant_id| state.tenants.get(tenant_id))
            .cloned())
    }
}

impl EntityStore for InMemoryStorage {
    fn create_entity(&self, entity: Entity) -> StorageResult<Entity> {
        let mut state = self.write_state()?;
        let index_key = (entity.tenant_id, entity.entity_key.clone());

        if state.entities.contains_key(&entity.id)
            || state.entity_key_index.contains_key(&index_key)
        {
            return Err(StorageError::Conflict);
        }

        state.entity_key_index.insert(index_key, entity.id);
        state.entities.insert(entity.id, entity.clone());
        Ok(entity)
    }

    fn update_entity(&self, entity: Entity) -> StorageResult<Entity> {
        let mut state = self.write_state()?;
        let index_key = (entity.tenant_id, entity.entity_key.clone());

        match state.entity_key_index.get(&index_key).copied() {
            Some(existing_id) if existing_id == entity.id => {}
            Some(_) => return Err(StorageError::Conflict),
            None => return Err(StorageError::NotFound),
        }

        if !state.entities.contains_key(&entity.id) {
            return Err(StorageError::NotFound);
        }

        state.entities.insert(entity.id, entity.clone());
        Ok(entity)
    }

    fn get_entity(&self, tenant_id: Uuid, entity_id: Uuid) -> StorageResult<Option<Entity>> {
        Ok(self
            .read_state()?
            .entities
            .get(&entity_id)
            .filter(|entity| entity.tenant_id == tenant_id)
            .cloned())
    }

    fn get_entity_any_tenant(&self, entity_id: Uuid) -> StorageResult<Option<Entity>> {
        Ok(self.read_state()?.entities.get(&entity_id).cloned())
    }

    fn get_entity_by_key(
        &self,
        tenant_id: Uuid,
        entity_key: &str,
    ) -> StorageResult<Option<Entity>> {
        let state = self.read_state()?;
        Ok(state
            .entity_key_index
            .get(&(tenant_id, entity_key.to_string()))
            .and_then(|entity_id| state.entities.get(entity_id))
            .cloned())
    }

    fn list_entities(&self, tenant_id: Uuid) -> StorageResult<Vec<Entity>> {
        let mut entities = self
            .read_state()?
            .entities
            .values()
            .filter(|entity| entity.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();

        entities.sort_by(|left, right| left.entity_key.cmp(&right.entity_key));
        Ok(entities)
    }

    fn list_all_entities(&self) -> StorageResult<Vec<Entity>> {
        let mut entities = self
            .read_state()?
            .entities
            .values()
            .cloned()
            .collect::<Vec<_>>();

        entities.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then_with(|| left.entity_key.cmp(&right.entity_key))
        });
        Ok(entities)
    }
}

impl RelationshipStore for InMemoryStorage {
    fn create_relationship(&self, relationship: Relationship) -> StorageResult<Relationship> {
        let mut state = self.write_state()?;

        if state.relationships.contains_key(&relationship.id) {
            return Err(StorageError::Conflict);
        }

        state
            .relationships
            .insert(relationship.id, relationship.clone());
        Ok(relationship)
    }

    fn list_relationships(
        &self,
        tenant_id: Uuid,
        source_entity_id: Option<Uuid>,
        target_entity_id: Option<Uuid>,
    ) -> StorageResult<Vec<Relationship>> {
        let mut relationships = self
            .read_state()?
            .relationships
            .values()
            .filter(|relationship| relationship.tenant_id == tenant_id)
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
            .cloned()
            .collect::<Vec<_>>();

        relationships.sort_by_key(|relationship| relationship.created_at);
        Ok(relationships)
    }
}

impl RawMessageStore for InMemoryStorage {
    fn store_raw_message(&self, raw_message: RawMessage) -> StorageResult<RawMessage> {
        let mut state = self.write_state()?;

        if state.raw_messages.contains_key(&raw_message.id) {
            return Err(StorageError::Conflict);
        }
        if let Some(idempotency_key) = raw_message.idempotency_key.as_ref() {
            let index_key = (raw_message.tenant_id, idempotency_key.clone());
            if state.raw_message_idempotency_index.contains_key(&index_key) {
                return Err(StorageError::Conflict);
            }
            state
                .raw_message_idempotency_index
                .insert(index_key, raw_message.id);
        }

        state
            .raw_messages
            .insert(raw_message.id, raw_message.clone());
        Ok(raw_message)
    }

    fn find_raw_message_by_idempotency_key(
        &self,
        tenant_id: Uuid,
        idempotency_key: &str,
    ) -> StorageResult<Option<RawMessage>> {
        let state = self.read_state()?;
        let Some(raw_message_id) = state
            .raw_message_idempotency_index
            .get(&(tenant_id, idempotency_key.to_string()))
        else {
            return Ok(None);
        };

        Ok(state.raw_messages.get(raw_message_id).cloned())
    }

    fn get_raw_message(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
    ) -> StorageResult<Option<RawMessage>> {
        Ok(self
            .read_state()?
            .raw_messages
            .get(&raw_message_id)
            .filter(|raw_message| raw_message.tenant_id == tenant_id)
            .cloned())
    }

    fn get_raw_message_any_tenant(
        &self,
        raw_message_id: Uuid,
    ) -> StorageResult<Option<RawMessage>> {
        Ok(self
            .read_state()?
            .raw_messages
            .get(&raw_message_id)
            .cloned())
    }

    fn list_raw_messages(&self, tenant_id: Uuid) -> StorageResult<Vec<RawMessage>> {
        let mut raw_messages = self
            .read_state()?
            .raw_messages
            .values()
            .filter(|raw_message| raw_message.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();

        raw_messages.sort_by(|left, right| right.received_at.cmp(&left.received_at));
        Ok(raw_messages)
    }

    fn list_all_raw_messages(&self) -> StorageResult<Vec<RawMessage>> {
        let mut raw_messages = self
            .read_state()?
            .raw_messages
            .values()
            .cloned()
            .collect::<Vec<_>>();

        raw_messages.sort_by(|left, right| {
            right
                .received_at
                .cmp(&left.received_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        Ok(raw_messages)
    }

    fn query_raw_messages(
        &self,
        tenant_id: Uuid,
        producer_entity_id: Option<Uuid>,
        feature_of_interest_id: Option<Uuid>,
        payload_format: Option<&str>,
    ) -> StorageResult<Vec<RawMessage>> {
        let mut raw_messages = self
            .read_state()?
            .raw_messages
            .values()
            .filter(|raw_message| raw_message.tenant_id == tenant_id)
            .filter(|raw_message| {
                producer_entity_id
                    .map(|id| raw_message.producer_entity_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|raw_message| {
                feature_of_interest_id
                    .map(|id| raw_message.feature_of_interest_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|raw_message| {
                payload_format
                    .map(|format| raw_message.payload_format.as_deref() == Some(format))
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();

        raw_messages.sort_by(|left, right| right.received_at.cmp(&left.received_at));
        Ok(raw_messages)
    }

    fn mark_raw_message_normalized(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
    ) -> StorageResult<()> {
        let mut state = self.write_state()?;
        let raw_message = state
            .raw_messages
            .get_mut(&raw_message_id)
            .filter(|raw_message| raw_message.tenant_id == tenant_id)
            .ok_or(StorageError::NotFound)?;

        raw_message.mark_normalized();
        Ok(())
    }

    fn mark_raw_message_failed(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
        error: &str,
    ) -> StorageResult<()> {
        let mut state = self.write_state()?;
        let raw_message = state
            .raw_messages
            .get_mut(&raw_message_id)
            .filter(|raw_message| raw_message.tenant_id == tenant_id)
            .ok_or(StorageError::NotFound)?;

        raw_message.mark_failed(error);
        Ok(())
    }
}

impl ObservationStore for InMemoryStorage {
    fn store_observation(&self, observation: Observation) -> StorageResult<Observation> {
        let mut state = self.write_state()?;

        if state.observations.contains_key(&observation.id) {
            return Err(StorageError::Conflict);
        }

        state
            .observations
            .insert(observation.id, observation.clone());
        Ok(observation)
    }

    fn get_observation(
        &self,
        tenant_id: Uuid,
        observation_id: Uuid,
    ) -> StorageResult<Option<Observation>> {
        Ok(self
            .read_state()?
            .observations
            .get(&observation_id)
            .filter(|observation| observation.tenant_id == tenant_id)
            .cloned())
    }

    fn get_observation_any_tenant(
        &self,
        observation_id: Uuid,
    ) -> StorageResult<Option<Observation>> {
        Ok(self
            .read_state()?
            .observations
            .get(&observation_id)
            .cloned())
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
        let mut observations = self
            .read_state()?
            .observations
            .values()
            .filter(|observation| observation.tenant_id == tenant_id)
            .filter(|observation| {
                feature_of_interest_id
                    .map(|id| observation.feature_of_interest_id == id)
                    .unwrap_or(true)
            })
            .filter(|observation| {
                observed_property
                    .map(|property| observation.observed_property == property)
                    .unwrap_or(true)
            })
            .filter(|observation| {
                from.map(|from| observation.observed_at >= from)
                    .unwrap_or(true)
            })
            .filter(|observation| to.map(|to| observation.observed_at <= to).unwrap_or(true))
            .cloned()
            .collect::<Vec<_>>();

        observations.sort_by(|left, right| right.observed_at.cmp(&left.observed_at));
        observations.truncate(limit as usize);
        Ok(observations)
    }

    fn query_observations_chronological(
        &self,
        tenant_id: Uuid,
        feature_of_interest_id: Option<Uuid>,
        observed_property: Option<&str>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: u32,
    ) -> StorageResult<Vec<Observation>> {
        let mut observations = self
            .read_state()?
            .observations
            .values()
            .filter(|observation| observation.tenant_id == tenant_id)
            .filter(|observation| {
                feature_of_interest_id
                    .map(|id| observation.feature_of_interest_id == id)
                    .unwrap_or(true)
            })
            .filter(|observation| {
                observed_property
                    .map(|property| observation.observed_property == property)
                    .unwrap_or(true)
            })
            .filter(|observation| {
                from.map(|from| observation.observed_at >= from)
                    .unwrap_or(true)
            })
            .filter(|observation| to.map(|to| observation.observed_at <= to).unwrap_or(true))
            .cloned()
            .collect::<Vec<_>>();
        observations.sort_by(|left, right| {
            left.observed_at
                .cmp(&right.observed_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        observations.truncate(limit as usize);
        Ok(observations)
    }

    fn list_all_observations(&self) -> StorageResult<Vec<Observation>> {
        let mut observations = self
            .read_state()?
            .observations
            .values()
            .cloned()
            .collect::<Vec<_>>();

        observations.sort_by(|left, right| {
            right
                .observed_at
                .cmp(&left.observed_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        Ok(observations)
    }
}

impl PayloadProfileStore for InMemoryStorage {
    fn put_payload_profile(
        &self,
        tenant_id: Uuid,
        profile: PayloadProfile,
    ) -> StorageResult<PayloadProfile> {
        let mut state = self.write_state()?;
        state
            .payload_profiles
            .insert((tenant_id, profile.entity_id), profile.clone());
        Ok(profile)
    }

    fn get_payload_profile(
        &self,
        tenant_id: Uuid,
        entity_id: Uuid,
    ) -> StorageResult<Option<PayloadProfile>> {
        Ok(self
            .read_state()?
            .payload_profiles
            .get(&(tenant_id, entity_id))
            .cloned())
    }
}

impl IngestionConnectorStore for InMemoryStorage {
    fn create_ingestion_connector(
        &self,
        connector: IngestionConnector,
    ) -> StorageResult<IngestionConnector> {
        let mut state = self.write_state()?;
        let index_key = (connector.tenant_id, connector.connector_key.clone());
        if state.ingestion_connectors.contains_key(&connector.id)
            || state.ingestion_connector_key_index.contains_key(&index_key)
        {
            return Err(StorageError::Conflict);
        }

        state
            .ingestion_connector_key_index
            .insert(index_key, connector.id);
        state
            .ingestion_connectors
            .insert(connector.id, connector.clone());
        Ok(connector)
    }

    fn get_ingestion_connector(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
    ) -> StorageResult<Option<IngestionConnector>> {
        Ok(self
            .read_state()?
            .ingestion_connectors
            .get(&connector_id)
            .filter(|connector| connector.tenant_id == tenant_id)
            .cloned())
    }

    fn list_ingestion_connectors(&self, tenant_id: Uuid) -> StorageResult<Vec<IngestionConnector>> {
        let mut connectors = self
            .read_state()?
            .ingestion_connectors
            .values()
            .filter(|connector| connector.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();

        connectors.sort_by(|left, right| left.connector_key.cmp(&right.connector_key));
        Ok(connectors)
    }

    fn list_all_ingestion_connectors(&self) -> StorageResult<Vec<IngestionConnector>> {
        let mut connectors = self
            .read_state()?
            .ingestion_connectors
            .values()
            .cloned()
            .collect::<Vec<_>>();

        connectors.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then_with(|| left.connector_key.cmp(&right.connector_key))
        });
        Ok(connectors)
    }

    fn update_ingestion_connector(
        &self,
        connector: IngestionConnector,
    ) -> StorageResult<IngestionConnector> {
        let mut state = self.write_state()?;
        let stored = state
            .ingestion_connectors
            .get_mut(&connector.id)
            .filter(|stored| stored.tenant_id == connector.tenant_id)
            .ok_or(StorageError::NotFound)?;

        *stored = connector.clone();
        Ok(connector)
    }
}

impl ConnectorSecretStore for InMemoryStorage {
    fn create_connector_secret(&self, secret: ConnectorSecret) -> StorageResult<ConnectorSecret> {
        let mut state = self.write_state()?;
        let index_key = (secret.tenant_id, secret.secret_key.clone());
        if state.connector_secrets.contains_key(&secret.id)
            || state.connector_secret_key_index.contains_key(&index_key)
        {
            return Err(StorageError::Conflict);
        }

        state
            .connector_secret_key_index
            .insert(index_key, secret.id);
        state.connector_secrets.insert(secret.id, secret.clone());
        Ok(secret)
    }

    fn get_connector_secret(
        &self,
        tenant_id: Uuid,
        secret_id: Uuid,
    ) -> StorageResult<Option<ConnectorSecret>> {
        Ok(self
            .read_state()?
            .connector_secrets
            .get(&secret_id)
            .filter(|secret| secret.tenant_id == tenant_id)
            .cloned())
    }

    fn list_connector_secrets(&self, tenant_id: Uuid) -> StorageResult<Vec<ConnectorSecret>> {
        let mut secrets = self
            .read_state()?
            .connector_secrets
            .values()
            .filter(|secret| secret.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();

        secrets.sort_by(|left, right| left.secret_key.cmp(&right.secret_key));
        Ok(secrets)
    }

    fn delete_connector_secret(&self, tenant_id: Uuid, secret_id: Uuid) -> StorageResult<()> {
        let mut state = self.write_state()?;
        let secret = state
            .connector_secrets
            .get(&secret_id)
            .filter(|secret| secret.tenant_id == tenant_id)
            .cloned()
            .ok_or(StorageError::NotFound)?;
        state.connector_secrets.remove(&secret_id);
        state
            .connector_secret_key_index
            .remove(&(secret.tenant_id, secret.secret_key));
        let now = Utc::now();
        for connector in state.ingestion_connectors.values_mut() {
            if connector.tenant_id == tenant_id && connector.secret_ref_id == Some(secret_id) {
                connector.secret_ref_id = None;
                connector.updated_at = now;
            }
        }
        Ok(())
    }
}

impl ApiTokenStore for InMemoryStorage {
    fn create_api_token(&self, token: ApiToken) -> StorageResult<ApiToken> {
        let mut state = self.write_state()?;
        let index_key = (token.tenant_id, token.token_prefix.clone());
        if state.api_tokens.contains_key(&token.id)
            || state.api_token_prefix_index.contains_key(&index_key)
        {
            return Err(StorageError::Conflict);
        }

        state.api_token_prefix_index.insert(index_key, token.id);
        state.api_tokens.insert(token.id, token.clone());
        Ok(token)
    }

    fn get_api_token(&self, tenant_id: Uuid, token_id: Uuid) -> StorageResult<Option<ApiToken>> {
        Ok(self
            .read_state()?
            .api_tokens
            .get(&token_id)
            .filter(|token| token.tenant_id == tenant_id)
            .cloned())
    }

    fn list_api_tokens(&self, tenant_id: Uuid) -> StorageResult<Vec<ApiToken>> {
        let mut tokens = self
            .read_state()?
            .api_tokens
            .values()
            .filter(|token| token.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();

        tokens.sort_by(|left, right| left.token_name.cmp(&right.token_name));
        Ok(tokens)
    }

    fn find_api_token_by_prefix(
        &self,
        tenant_id: Uuid,
        token_prefix: &str,
    ) -> StorageResult<Option<ApiToken>> {
        let state = self.read_state()?;
        Ok(state
            .api_token_prefix_index
            .get(&(tenant_id, token_prefix.to_string()))
            .and_then(|token_id| state.api_tokens.get(token_id))
            .filter(|token| token.tenant_id == tenant_id)
            .cloned())
    }

    fn find_api_token_by_prefix_any_tenant(
        &self,
        token_prefix: &str,
    ) -> StorageResult<Option<ApiToken>> {
        Ok(self
            .read_state()?
            .api_tokens
            .values()
            .find(|token| token.token_prefix == token_prefix)
            .cloned())
    }

    fn update_api_token_last_used_at(
        &self,
        tenant_id: Uuid,
        token_id: Uuid,
        last_used_at: DateTime<Utc>,
    ) -> StorageResult<ApiToken> {
        let mut state = self.write_state()?;
        let token = state
            .api_tokens
            .get_mut(&token_id)
            .filter(|token| token.tenant_id == tenant_id)
            .ok_or(StorageError::NotFound)?;
        token.last_used_at = Some(last_used_at);
        token.updated_at = last_used_at;
        Ok(token.clone())
    }

    fn revoke_api_token(
        &self,
        tenant_id: Uuid,
        token_id: Uuid,
        revoked_at: DateTime<Utc>,
    ) -> StorageResult<ApiToken> {
        let mut state = self.write_state()?;
        let token = state
            .api_tokens
            .get_mut(&token_id)
            .filter(|token| token.tenant_id == tenant_id)
            .ok_or(StorageError::NotFound)?;
        token.revoked_at = Some(revoked_at);
        token.updated_at = revoked_at;
        Ok(token.clone())
    }
}

impl TtnDeviceMappingStore for InMemoryStorage {
    fn create_ttn_device_mapping(
        &self,
        mapping: TtnDeviceMapping,
    ) -> StorageResult<TtnDeviceMapping> {
        let mut state = self.write_state()?;
        if state.ttn_device_mappings.contains_key(&mapping.id) {
            return Err(StorageError::Conflict);
        }
        validate_ttn_device_mapping_conflict(state.ttn_device_mappings.values(), &mapping)?;
        state
            .ttn_device_mappings
            .insert(mapping.id, mapping.clone());
        Ok(mapping)
    }

    fn get_ttn_device_mapping(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
        mapping_id: Uuid,
    ) -> StorageResult<Option<TtnDeviceMapping>> {
        Ok(self
            .read_state()?
            .ttn_device_mappings
            .get(&mapping_id)
            .filter(|mapping| {
                mapping.tenant_id == tenant_id && mapping.connector_id == connector_id
            })
            .cloned())
    }

    fn list_ttn_device_mappings(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
    ) -> StorageResult<Vec<TtnDeviceMapping>> {
        let mut mappings = self
            .read_state()?
            .ttn_device_mappings
            .values()
            .filter(|mapping| {
                mapping.tenant_id == tenant_id && mapping.connector_id == connector_id
            })
            .cloned()
            .collect::<Vec<_>>();
        mappings.sort_by(|left, right| {
            left.ttn_device_id
                .cmp(&right.ttn_device_id)
                .then(left.ttn_application_id.cmp(&right.ttn_application_id))
        });
        Ok(mappings)
    }

    fn update_ttn_device_mapping(
        &self,
        mapping: TtnDeviceMapping,
    ) -> StorageResult<TtnDeviceMapping> {
        let mut state = self.write_state()?;
        validate_ttn_device_mapping_conflict(state.ttn_device_mappings.values(), &mapping)?;
        let stored = state
            .ttn_device_mappings
            .get_mut(&mapping.id)
            .filter(|stored| {
                stored.tenant_id == mapping.tenant_id && stored.connector_id == mapping.connector_id
            })
            .ok_or(StorageError::NotFound)?;
        *stored = mapping.clone();
        Ok(mapping)
    }

    fn delete_ttn_device_mapping(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
        mapping_id: Uuid,
    ) -> StorageResult<()> {
        let mut state = self.write_state()?;
        let exists = state
            .ttn_device_mappings
            .get(&mapping_id)
            .map(|mapping| mapping.tenant_id == tenant_id && mapping.connector_id == connector_id)
            .unwrap_or(false);
        if !exists {
            return Err(StorageError::NotFound);
        }
        state.ttn_device_mappings.remove(&mapping_id);
        Ok(())
    }

    fn find_ttn_device_mapping(
        &self,
        tenant_id: Uuid,
        connector_id: Uuid,
        ttn_application_id: Option<&str>,
        ttn_device_id: &str,
    ) -> StorageResult<Option<TtnDeviceMapping>> {
        let mappings = self.read_state()?;
        let matches = |mapping: &&TtnDeviceMapping| {
            mapping.tenant_id == tenant_id
                && mapping.connector_id == connector_id
                && mapping.enabled
                && mapping.ttn_device_id == ttn_device_id
        };
        if let Some(application_id) = ttn_application_id {
            if let Some(mapping) = mappings
                .ttn_device_mappings
                .values()
                .filter(matches)
                .find(|mapping| mapping.ttn_application_id.as_deref() == Some(application_id))
            {
                return Ok(Some(mapping.clone()));
            }
        }
        Ok(mappings
            .ttn_device_mappings
            .values()
            .filter(matches)
            .find(|mapping| mapping.ttn_application_id.is_none())
            .cloned())
    }
}

impl CapabilityStore for InMemoryStorage {
    fn put_capabilities(
        &self,
        tenant_id: Uuid,
        entity_id: Uuid,
        capabilities: Vec<Capability>,
    ) -> StorageResult<Vec<Capability>> {
        let mut state = self.write_state()?;
        state
            .capabilities
            .insert((tenant_id, entity_id), capabilities.clone());
        Ok(capabilities)
    }

    fn list_capabilities(
        &self,
        tenant_id: Uuid,
        entity_id: Uuid,
    ) -> StorageResult<Vec<Capability>> {
        let mut capabilities = self
            .read_state()?
            .capabilities
            .get(&(tenant_id, entity_id))
            .cloned()
            .unwrap_or_default();

        capabilities.sort_by(|left, right| left.capability_name.cmp(&right.capability_name));
        Ok(capabilities)
    }
}

impl ExecutorStore for InMemoryStorage {
    fn create_executor(&self, executor: ExecutorAgent) -> StorageResult<ExecutorAgent> {
        let mut state = self.write_state()?;
        let index_key = (executor.tenant_id, executor.agent_key.clone());
        if state.executors.contains_key(&executor.id)
            || state.executor_key_index.contains_key(&index_key)
        {
            return Err(StorageError::Conflict);
        }

        state.executor_key_index.insert(index_key, executor.id);
        state.executors.insert(executor.id, executor.clone());
        Ok(executor)
    }

    fn update_executor(&self, executor: ExecutorAgent) -> StorageResult<ExecutorAgent> {
        let mut state = self.write_state()?;
        let stored = state
            .executors
            .get_mut(&executor.id)
            .filter(|stored| stored.tenant_id == executor.tenant_id)
            .ok_or(StorageError::NotFound)?;

        *stored = executor.clone();
        Ok(executor)
    }

    fn get_executor(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
    ) -> StorageResult<Option<ExecutorAgent>> {
        Ok(self
            .read_state()?
            .executors
            .get(&executor_id)
            .filter(|executor| executor.tenant_id == tenant_id)
            .cloned())
    }

    fn get_executor_any_tenant(&self, executor_id: Uuid) -> StorageResult<Option<ExecutorAgent>> {
        Ok(self.read_state()?.executors.get(&executor_id).cloned())
    }

    fn list_executors(&self, tenant_id: Uuid) -> StorageResult<Vec<ExecutorAgent>> {
        let mut executors = self
            .read_state()?
            .executors
            .values()
            .filter(|executor| executor.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();

        executors.sort_by(|left, right| left.agent_key.cmp(&right.agent_key));
        Ok(executors)
    }

    fn list_all_executors(&self) -> StorageResult<Vec<ExecutorAgent>> {
        let mut executors = self
            .read_state()?
            .executors
            .values()
            .cloned()
            .collect::<Vec<_>>();

        executors.sort_by(|left, right| {
            left.tenant_id
                .cmp(&right.tenant_id)
                .then_with(|| left.agent_key.cmp(&right.agent_key))
        });
        Ok(executors)
    }

    fn put_executor_capabilities(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
        capabilities: Vec<ExecutorCapability>,
    ) -> StorageResult<Vec<ExecutorCapability>> {
        let mut state = self.write_state()?;
        if !state
            .executors
            .get(&executor_id)
            .map(|executor| executor.tenant_id == tenant_id)
            .unwrap_or(false)
        {
            return Err(StorageError::NotFound);
        }
        state
            .executor_capabilities
            .insert((tenant_id, executor_id), capabilities.clone());
        Ok(capabilities)
    }

    fn list_executor_capabilities(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
    ) -> StorageResult<Vec<ExecutorCapability>> {
        let mut capabilities = self
            .read_state()?
            .executor_capabilities
            .get(&(tenant_id, executor_id))
            .cloned()
            .unwrap_or_default();

        capabilities.sort_by(|left, right| left.command_type.cmp(&right.command_type));
        Ok(capabilities)
    }

    fn put_executor_scopes(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
        scopes: Vec<ExecutorScope>,
    ) -> StorageResult<Vec<ExecutorScope>> {
        let mut state = self.write_state()?;
        if !state
            .executors
            .get(&executor_id)
            .map(|executor| executor.tenant_id == tenant_id)
            .unwrap_or(false)
        {
            return Err(StorageError::NotFound);
        }
        state
            .executor_scopes
            .insert((tenant_id, executor_id), scopes.clone());
        Ok(scopes)
    }

    fn list_executor_scopes(
        &self,
        tenant_id: Uuid,
        executor_id: Uuid,
    ) -> StorageResult<Vec<ExecutorScope>> {
        Ok(self
            .read_state()?
            .executor_scopes
            .get(&(tenant_id, executor_id))
            .cloned()
            .unwrap_or_default())
    }
}

impl EdgeAdapterStore for InMemoryStorage {
    fn create_edge_adapter(&self, adapter: EdgeAdapter) -> StorageResult<EdgeAdapter> {
        let mut state = self.write_state()?;
        let index_key = (adapter.tenant_id, adapter.adapter_key.clone());
        if state.edge_adapters.contains_key(&adapter.id)
            || state.edge_adapter_key_index.contains_key(&index_key)
        {
            return Err(StorageError::Conflict);
        }

        state.edge_adapter_key_index.insert(index_key, adapter.id);
        state.edge_adapters.insert(adapter.id, adapter.clone());
        Ok(adapter)
    }

    fn update_edge_adapter(&self, adapter: EdgeAdapter) -> StorageResult<EdgeAdapter> {
        let mut state = self.write_state()?;
        let index_key = (adapter.tenant_id, adapter.adapter_key.clone());
        match state.edge_adapter_key_index.get(&index_key).copied() {
            Some(existing_id) if existing_id == adapter.id => {}
            Some(_) => return Err(StorageError::Conflict),
            None => return Err(StorageError::NotFound),
        }
        let stored = state
            .edge_adapters
            .get_mut(&adapter.id)
            .filter(|stored| stored.tenant_id == adapter.tenant_id)
            .ok_or(StorageError::NotFound)?;
        *stored = adapter.clone();
        Ok(adapter)
    }

    fn get_edge_adapter(
        &self,
        tenant_id: Uuid,
        adapter_id: Uuid,
    ) -> StorageResult<Option<EdgeAdapter>> {
        Ok(self
            .read_state()?
            .edge_adapters
            .get(&adapter_id)
            .filter(|adapter| adapter.tenant_id == tenant_id)
            .cloned())
    }

    fn get_edge_adapter_by_key(
        &self,
        tenant_id: Uuid,
        adapter_key: &str,
    ) -> StorageResult<Option<EdgeAdapter>> {
        let state = self.read_state()?;
        Ok(state
            .edge_adapter_key_index
            .get(&(tenant_id, adapter_key.to_string()))
            .and_then(|adapter_id| state.edge_adapters.get(adapter_id))
            .cloned())
    }

    fn list_edge_adapters(&self, tenant_id: Uuid) -> StorageResult<Vec<EdgeAdapter>> {
        let mut adapters = self
            .read_state()?
            .edge_adapters
            .values()
            .filter(|adapter| adapter.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();

        adapters.sort_by(|left, right| left.adapter_key.cmp(&right.adapter_key));
        Ok(adapters)
    }

    fn put_edge_adapter_status(
        &self,
        tenant_id: Uuid,
        status: EdgeAdapterStatusReport,
    ) -> StorageResult<EdgeAdapterStatusReport> {
        let mut state = self.write_state()?;
        if !state
            .edge_adapters
            .get(&status.adapter_id)
            .map(|adapter| adapter.tenant_id == tenant_id)
            .unwrap_or(false)
        {
            return Err(StorageError::NotFound);
        }
        state
            .edge_adapter_statuses
            .insert(status.adapter_id, status.clone());
        Ok(status)
    }

    fn get_edge_adapter_status(
        &self,
        tenant_id: Uuid,
        adapter_id: Uuid,
    ) -> StorageResult<Option<EdgeAdapterStatusReport>> {
        let state = self.read_state()?;
        let allowed = state
            .edge_adapters
            .get(&adapter_id)
            .map(|adapter| adapter.tenant_id == tenant_id)
            .unwrap_or(false);
        Ok(state
            .edge_adapter_statuses
            .get(&adapter_id)
            .filter(|status| allowed && status.adapter_id == adapter_id)
            .cloned())
    }
}

impl CommandStore for InMemoryStorage {
    fn store_command(&self, command: Command) -> StorageResult<Command> {
        let mut state = self.write_state()?;
        if state.commands.contains_key(&command.id) {
            return Err(StorageError::Conflict);
        }

        state.commands.insert(command.id, command.clone());
        Ok(command)
    }

    fn update_command(&self, command: Command) -> StorageResult<Command> {
        let mut state = self.write_state()?;
        let stored = state
            .commands
            .get_mut(&command.id)
            .filter(|stored| stored.tenant_id == command.tenant_id)
            .ok_or(StorageError::NotFound)?;

        *stored = command.clone();
        Ok(command)
    }

    fn get_command(&self, tenant_id: Uuid, command_id: Uuid) -> StorageResult<Option<Command>> {
        Ok(self
            .read_state()?
            .commands
            .get(&command_id)
            .filter(|command| command.tenant_id == tenant_id)
            .cloned())
    }

    fn get_command_any_tenant(&self, command_id: Uuid) -> StorageResult<Option<Command>> {
        Ok(self.read_state()?.commands.get(&command_id).cloned())
    }

    fn query_commands(
        &self,
        tenant_id: Uuid,
        target_entity_id: Option<Uuid>,
        status: Option<CommandStatus>,
    ) -> StorageResult<Vec<Command>> {
        let mut commands = self
            .read_state()?
            .commands
            .values()
            .filter(|command| command.tenant_id == tenant_id)
            .filter(|command| {
                target_entity_id
                    .map(|id| command.target_entity_id == id)
                    .unwrap_or(true)
            })
            .filter(|command| {
                status
                    .as_ref()
                    .map(|status| command.status == *status)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();

        commands.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        Ok(commands)
    }

    fn list_all_commands(&self) -> StorageResult<Vec<Command>> {
        let mut commands = self
            .read_state()?
            .commands
            .values()
            .cloned()
            .collect::<Vec<_>>();

        commands.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        Ok(commands)
    }
}

impl CommandLeaseStore for InMemoryStorage {
    fn store_command_lease(&self, lease: CommandLease) -> StorageResult<CommandLease> {
        let mut state = self.write_state()?;
        if state.command_leases.contains_key(&lease.id) {
            return Err(StorageError::Conflict);
        }
        state.command_leases.insert(lease.id, lease.clone());
        Ok(lease)
    }

    fn update_command_lease(&self, lease: CommandLease) -> StorageResult<CommandLease> {
        let mut state = self.write_state()?;
        let stored = state
            .command_leases
            .get_mut(&lease.id)
            .filter(|stored| stored.tenant_id == lease.tenant_id)
            .ok_or(StorageError::NotFound)?;
        *stored = lease.clone();
        Ok(lease)
    }

    fn get_command_lease(
        &self,
        tenant_id: Uuid,
        lease_id: Uuid,
    ) -> StorageResult<Option<CommandLease>> {
        Ok(self
            .read_state()?
            .command_leases
            .get(&lease_id)
            .filter(|lease| lease.tenant_id == tenant_id)
            .cloned())
    }

    fn get_active_command_lease(
        &self,
        tenant_id: Uuid,
        command_id: Uuid,
    ) -> StorageResult<Option<CommandLease>> {
        Ok(self
            .read_state()?
            .command_leases
            .values()
            .find(|lease| {
                lease.tenant_id == tenant_id
                    && lease.command_id == command_id
                    && lease.lease_status == CommandLeaseStatus::Active
            })
            .cloned())
    }

    fn get_latest_command_lease(
        &self,
        tenant_id: Uuid,
        command_id: Uuid,
    ) -> StorageResult<Option<CommandLease>> {
        let state = self.read_state()?;
        Ok(state
            .command_leases
            .values()
            .filter(|lease| lease.tenant_id == tenant_id && lease.command_id == command_id)
            .max_by_key(|lease| lease.claimed_at)
            .cloned())
    }

    fn list_active_command_leases(&self, tenant_id: Uuid) -> StorageResult<Vec<CommandLease>> {
        let mut leases = self
            .read_state()?
            .command_leases
            .values()
            .filter(|lease| {
                lease.tenant_id == tenant_id && lease.lease_status == CommandLeaseStatus::Active
            })
            .cloned()
            .collect::<Vec<_>>();
        leases.sort_by(|left, right| left.expires_at.cmp(&right.expires_at));
        Ok(leases)
    }
}

impl PolicyStore for InMemoryStorage {
    fn put_policies(&self, tenant_id: Uuid, policies: Vec<Policy>) -> StorageResult<Vec<Policy>> {
        let mut state = self.write_state()?;
        state
            .policies
            .retain(|_, policy| policy.tenant_id != tenant_id);
        for policy in &policies {
            if policy.tenant_id != tenant_id {
                return Err(StorageError::InvalidInput(
                    "policy tenant_id does not match requested tenant".to_string(),
                ));
            }
            state.policies.insert(policy.id, policy.clone());
        }

        Ok(policies)
    }

    fn query_policies(
        &self,
        tenant_id: Uuid,
        target_entity_id: Option<Uuid>,
        command_type: Option<&str>,
    ) -> StorageResult<Vec<Policy>> {
        let mut policies = self
            .read_state()?
            .policies
            .values()
            .filter(|policy| policy.tenant_id == tenant_id)
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
            .cloned()
            .collect::<Vec<_>>();

        policies.sort_by_key(|policy| {
            (
                policy.target_entity_id.is_none(),
                policy.command_type.is_none(),
                policy.id,
            )
        });
        Ok(policies)
    }

    fn list_all_policies(&self) -> StorageResult<Vec<Policy>> {
        let mut policies = self
            .read_state()?
            .policies
            .values()
            .cloned()
            .collect::<Vec<_>>();

        policies.sort_by_key(|policy| {
            (
                policy.tenant_id,
                policy.target_entity_id.is_none(),
                policy.command_type.is_none(),
                policy.id,
            )
        });
        Ok(policies)
    }
}

impl ActionStore for InMemoryStorage {
    fn store_action(&self, action: Action) -> StorageResult<Action> {
        let mut state = self.write_state()?;
        if state.actions.contains_key(&action.id) {
            return Err(StorageError::Conflict);
        }

        state.actions.insert(action.id, action.clone());
        Ok(action)
    }

    fn get_action(&self, tenant_id: Uuid, action_id: Uuid) -> StorageResult<Option<Action>> {
        Ok(self
            .read_state()?
            .actions
            .get(&action_id)
            .filter(|action| action.tenant_id == tenant_id)
            .cloned())
    }

    fn get_action_any_tenant(&self, action_id: Uuid) -> StorageResult<Option<Action>> {
        Ok(self.read_state()?.actions.get(&action_id).cloned())
    }

    fn query_actions(
        &self,
        tenant_id: Uuid,
        command_id: Option<Uuid>,
    ) -> StorageResult<Vec<Action>> {
        let mut actions = self
            .read_state()?
            .actions
            .values()
            .filter(|action| action.tenant_id == tenant_id)
            .filter(|action| command_id.map(|id| action.command_id == id).unwrap_or(true))
            .cloned()
            .collect::<Vec<_>>();

        actions.sort_by(|left, right| left.started_at.cmp(&right.started_at));
        Ok(actions)
    }

    fn list_all_actions(&self) -> StorageResult<Vec<Action>> {
        let mut actions = self
            .read_state()?
            .actions
            .values()
            .cloned()
            .collect::<Vec<_>>();

        actions.sort_by(|left, right| {
            right
                .started_at
                .cmp(&left.started_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        Ok(actions)
    }
}

impl ActionResultStore for InMemoryStorage {
    fn store_action_result(&self, result: ActionResult) -> StorageResult<ActionResult> {
        let mut state = self.write_state()?;
        if state.action_results.contains_key(&result.id) {
            return Err(StorageError::Conflict);
        }

        state.action_results.insert(result.id, result.clone());
        Ok(result)
    }

    fn query_action_results(
        &self,
        tenant_id: Uuid,
        action_id: Option<Uuid>,
        command_id: Option<Uuid>,
    ) -> StorageResult<Vec<ActionResult>> {
        let mut results = self
            .read_state()?
            .action_results
            .values()
            .filter(|result| result.tenant_id == tenant_id)
            .filter(|result| action_id.map(|id| result.action_id == id).unwrap_or(true))
            .filter(|result| command_id.map(|id| result.command_id == id).unwrap_or(true))
            .cloned()
            .collect::<Vec<_>>();

        results.sort_by(|left, right| right.observed_at.cmp(&left.observed_at));
        Ok(results)
    }

    fn list_all_action_results(&self) -> StorageResult<Vec<ActionResult>> {
        let mut results = self
            .read_state()?
            .action_results
            .values()
            .cloned()
            .collect::<Vec<_>>();

        results.sort_by(|left, right| {
            right
                .observed_at
                .cmp(&left.observed_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        Ok(results)
    }
}

impl EventStore for InMemoryStorage {
    fn store_event(&self, event: Event) -> StorageResult<Event> {
        let mut state = self.write_state()?;
        if state.events.contains_key(&event.id) {
            return Err(StorageError::Conflict);
        }

        state.events.insert(event.id, event.clone());
        Ok(event)
    }

    fn get_event(&self, tenant_id: Uuid, event_id: Uuid) -> StorageResult<Option<Event>> {
        Ok(self
            .read_state()?
            .events
            .get(&event_id)
            .filter(|event| event.tenant_id == tenant_id)
            .cloned())
    }

    fn get_event_any_tenant(&self, event_id: Uuid) -> StorageResult<Option<Event>> {
        Ok(self.read_state()?.events.get(&event_id).cloned())
    }

    fn query_events(&self, tenant_id: Uuid, filter: EventFilter) -> StorageResult<Vec<Event>> {
        let mut events = self
            .read_state()?
            .events
            .values()
            .filter(|event| event.tenant_id == tenant_id)
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
            .cloned()
            .collect::<Vec<_>>();

        events.sort_by(|left, right| right.occurred_at.cmp(&left.occurred_at));
        Ok(events)
    }

    fn list_all_events(&self) -> StorageResult<Vec<Event>> {
        let mut events = self
            .read_state()?
            .events
            .values()
            .cloned()
            .collect::<Vec<_>>();

        events.sort_by(|left, right| {
            right
                .occurred_at
                .cmp(&left.occurred_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        Ok(events)
    }
}

impl RuleStore for InMemoryStorage {
    fn store_rule(&self, rule: Rule) -> StorageResult<Rule> {
        let mut state = self.write_state()?;
        if state.rules.contains_key(&rule.id) {
            return Err(StorageError::Conflict);
        }

        state.rules.insert(rule.id, rule.clone());
        Ok(rule)
    }

    fn update_rule(&self, rule: Rule) -> StorageResult<Rule> {
        let mut state = self.write_state()?;
        let stored = state
            .rules
            .get_mut(&rule.id)
            .filter(|stored| stored.tenant_id == rule.tenant_id)
            .ok_or(StorageError::NotFound)?;

        *stored = rule.clone();
        Ok(rule)
    }

    fn get_rule(&self, tenant_id: Uuid, rule_id: Uuid) -> StorageResult<Option<Rule>> {
        Ok(self
            .read_state()?
            .rules
            .get(&rule_id)
            .filter(|rule| rule.tenant_id == tenant_id)
            .cloned())
    }

    fn get_rule_any_tenant(&self, rule_id: Uuid) -> StorageResult<Option<Rule>> {
        Ok(self.read_state()?.rules.get(&rule_id).cloned())
    }

    fn list_rules(&self, tenant_id: Uuid) -> StorageResult<Vec<Rule>> {
        let mut rules = self
            .read_state()?
            .rules
            .values()
            .filter(|rule| rule.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();

        rules.sort_by(|left, right| left.created_at.cmp(&right.created_at));
        Ok(rules)
    }

    fn list_all_rules(&self) -> StorageResult<Vec<Rule>> {
        let mut rules = self
            .read_state()?
            .rules
            .values()
            .cloned()
            .collect::<Vec<_>>();

        rules.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(rules)
    }
}

impl FlowStore for InMemoryStorage {
    fn create_flow(&self, flow: Flow) -> StorageResult<Flow> {
        flow.validate()
            .map_err(|err| StorageError::InvalidInput(err.to_string()))?;

        let mut state = self.write_state()?;
        if state.flows.contains_key(&flow.id)
            || state
                .flow_key_index
                .contains_key(&(flow.tenant_id, flow.flow_key.clone()))
        {
            return Err(StorageError::Conflict);
        }

        state
            .flow_key_index
            .insert((flow.tenant_id, flow.flow_key.clone()), flow.id);
        state.flows.insert(flow.id, flow.clone());
        Ok(flow)
    }

    fn update_flow(&self, flow: Flow) -> StorageResult<Flow> {
        flow.validate()
            .map_err(|err| StorageError::InvalidInput(err.to_string()))?;

        let mut state = self.write_state()?;
        let previous = state
            .flows
            .get(&flow.id)
            .filter(|stored| stored.tenant_id == flow.tenant_id)
            .cloned()
            .ok_or(StorageError::NotFound)?;

        if previous.flow_key != flow.flow_key {
            let key = (flow.tenant_id, flow.flow_key.clone());
            if state.flow_key_index.contains_key(&key) {
                return Err(StorageError::Conflict);
            }
            state
                .flow_key_index
                .remove(&(previous.tenant_id, previous.flow_key.clone()));
            state.flow_key_index.insert(key, flow.id);
        }

        state.flows.insert(flow.id, flow.clone());
        Ok(flow)
    }

    fn get_flow(&self, tenant_id: Uuid, flow_id: Uuid) -> StorageResult<Option<Flow>> {
        Ok(self
            .read_state()?
            .flows
            .get(&flow_id)
            .filter(|flow| flow.tenant_id == tenant_id)
            .cloned())
    }

    fn get_flow_any_tenant(&self, flow_id: Uuid) -> StorageResult<Option<Flow>> {
        Ok(self.read_state()?.flows.get(&flow_id).cloned())
    }

    fn list_flows(&self, tenant_id: Uuid) -> StorageResult<Vec<Flow>> {
        let mut flows = self
            .read_state()?
            .flows
            .values()
            .filter(|flow| flow.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();

        flows.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(flows)
    }

    fn list_all_flows(&self) -> StorageResult<Vec<Flow>> {
        let mut flows = self
            .read_state()?
            .flows
            .values()
            .cloned()
            .collect::<Vec<_>>();

        flows.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(flows)
    }

    fn delete_flow(&self, tenant_id: Uuid, flow_id: Uuid) -> StorageResult<()> {
        let mut state = self.write_state()?;
        let flow = state
            .flows
            .get(&flow_id)
            .filter(|flow| flow.tenant_id == tenant_id)
            .cloned()
            .ok_or(StorageError::NotFound)?;

        state.flows.remove(&flow_id);
        state
            .flow_key_index
            .remove(&(flow.tenant_id, flow.flow_key.clone()));
        Ok(())
    }
}

impl DlqStore for InMemoryStorage {
    fn create_dlq_record(&self, record: DlqRecord) -> StorageResult<DlqRecord> {
        let mut state = self.write_state()?;
        if state.dlq_records.contains_key(&record.id) {
            return Err(StorageError::Conflict);
        }

        state.dlq_records.insert(record.id, record.clone());
        Ok(record)
    }

    fn list_dlq_records(
        &self,
        tenant_id: Uuid,
        filter: DlqRecordFilter,
    ) -> StorageResult<Vec<DlqRecord>> {
        let mut records = self
            .read_state()?
            .dlq_records
            .values()
            .filter(|record| record.tenant_id == tenant_id)
            .filter(|record| dlq_record_matches_filter(record, &filter))
            .cloned()
            .collect::<Vec<_>>();

        sort_and_truncate_dlq_records(&mut records, filter.limit);
        Ok(records)
    }

    fn list_all_dlq_records(&self, filter: DlqRecordFilter) -> StorageResult<Vec<DlqRecord>> {
        let mut records = self
            .read_state()?
            .dlq_records
            .values()
            .filter(|record| dlq_record_matches_filter(record, &filter))
            .cloned()
            .collect::<Vec<_>>();

        sort_and_truncate_dlq_records(&mut records, filter.limit);
        Ok(records)
    }

    fn get_dlq_record(&self, tenant_id: Uuid, record_id: Uuid) -> StorageResult<Option<DlqRecord>> {
        Ok(self
            .read_state()?
            .dlq_records
            .get(&record_id)
            .filter(|record| record.tenant_id == tenant_id)
            .cloned())
    }

    fn get_dlq_record_any_tenant(&self, record_id: Uuid) -> StorageResult<Option<DlqRecord>> {
        Ok(self.read_state()?.dlq_records.get(&record_id).cloned())
    }

    fn update_dlq_record_status(
        &self,
        tenant_id: Uuid,
        record_id: Uuid,
        status: DlqStatus,
        now: DateTime<Utc>,
    ) -> StorageResult<DlqRecord> {
        let mut state = self.write_state()?;
        let record = state
            .dlq_records
            .get_mut(&record_id)
            .filter(|record| record.tenant_id == tenant_id)
            .ok_or(StorageError::NotFound)?;
        record.set_status(status, now);
        Ok(record.clone())
    }
}

fn dlq_record_matches_filter(record: &DlqRecord, filter: &DlqRecordFilter) -> bool {
    filter
        .status
        .as_ref()
        .map(|status| &record.status == status)
        .unwrap_or(true)
        && filter
            .failure_stage
            .as_ref()
            .map(|stage| &record.failure_stage == stage)
            .unwrap_or(true)
        && filter
            .source_system
            .as_deref()
            .map(|value| record.source_system.as_deref() == Some(value))
            .unwrap_or(true)
        && filter
            .connector_id
            .map(|value| record.connector_id == Some(value))
            .unwrap_or(true)
        && filter
            .flow_id
            .map(|value| record.flow_id == Some(value))
            .unwrap_or(true)
        && filter
            .raw_message_id
            .map(|value| record.raw_message_id == Some(value))
            .unwrap_or(true)
        && filter
            .idempotency_key
            .as_deref()
            .map(|value| record.idempotency_key.as_deref() == Some(value))
            .unwrap_or(true)
        && filter
            .external_flowfile_uuid
            .as_deref()
            .map(|value| record.external_flowfile_uuid.as_deref() == Some(value))
            .unwrap_or(true)
        && filter
            .sync_session_id
            .as_deref()
            .map(|value| record.sync_session_id.as_deref() == Some(value))
            .unwrap_or(true)
}

fn sort_and_truncate_dlq_records(records: &mut Vec<DlqRecord>, limit: u32) {
    records.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    records.truncate(limit as usize);
}

#[cfg(test)]
mod tests {
    use super::*;
    use aion_dlq::{DlqFailureStage, DlqRecord, DlqStatus};
    use aion_flow::{Flow, FlowEdge, FlowNode, FlowNodeType};
    use chrono::TimeZone;

    #[test]
    fn exposes_ordered_migrations() {
        assert_eq!(ORDERED_MIGRATIONS.len(), 15);
        assert_eq!(ORDERED_MIGRATIONS[0].0, "0001_create_tenants.sql");
        assert_eq!(ORDERED_MIGRATIONS[4].0, "0005_create_observations.sql");
        assert_eq!(
            ORDERED_MIGRATIONS[5].0,
            "0006_create_runtime_persistence_tables.sql"
        );
        assert_eq!(
            ORDERED_MIGRATIONS[6].0,
            "0007_create_ingestion_connectors.sql"
        );
        assert_eq!(ORDERED_MIGRATIONS[7].0, "0008_create_connector_secrets.sql");
        assert_eq!(
            ORDERED_MIGRATIONS[8].0,
            "0009_create_ttn_device_mappings.sql"
        );
        assert_eq!(
            ORDERED_MIGRATIONS[9].0,
            "0010_harden_ttn_device_mapping_uniqueness.sql"
        );
        assert_eq!(ORDERED_MIGRATIONS[10].0, "0011_create_edge_adapters.sql");
        assert_eq!(ORDERED_MIGRATIONS[11].0, "0012_create_api_tokens.sql");
        assert_eq!(ORDERED_MIGRATIONS[12].0, "0013_create_flows.sql");
        assert_eq!(ORDERED_MIGRATIONS[13].0, "0014_create_dlq_records.sql");
        assert_eq!(
            ORDERED_MIGRATIONS[14].0,
            "0015_add_raw_message_idempotency.sql"
        );
    }

    #[test]
    fn migrations_define_required_tables() {
        let combined = ORDERED_MIGRATIONS
            .iter()
            .map(|(_, sql)| *sql)
            .collect::<Vec<_>>()
            .join("\n");

        for table in [
            "CREATE TABLE IF NOT EXISTS tenants",
            "CREATE TABLE IF NOT EXISTS entities",
            "CREATE TABLE IF NOT EXISTS entity_relationships",
            "CREATE TABLE IF NOT EXISTS raw_messages",
            "CREATE TABLE IF NOT EXISTS observations",
            "CREATE TABLE IF NOT EXISTS payload_profiles",
            "CREATE TABLE IF NOT EXISTS capabilities",
            "CREATE TABLE IF NOT EXISTS policies",
            "CREATE TABLE IF NOT EXISTS commands",
            "CREATE TABLE IF NOT EXISTS actions",
            "CREATE TABLE IF NOT EXISTS action_results",
            "CREATE TABLE IF NOT EXISTS events",
            "CREATE TABLE IF NOT EXISTS executor_agents",
            "CREATE TABLE IF NOT EXISTS executor_capabilities",
            "CREATE TABLE IF NOT EXISTS executor_scopes",
            "CREATE TABLE IF NOT EXISTS edge_adapters",
            "CREATE TABLE IF NOT EXISTS edge_adapter_statuses",
            "CREATE TABLE IF NOT EXISTS command_leases",
            "CREATE TABLE IF NOT EXISTS rules",
            "CREATE TABLE IF NOT EXISTS ingestion_connectors",
            "CREATE TABLE IF NOT EXISTS connector_secrets",
            "CREATE TABLE IF NOT EXISTS api_tokens",
            "CREATE TABLE IF NOT EXISTS ttn_device_mappings",
            "CREATE TABLE IF NOT EXISTS dlq_records",
        ] {
            assert!(
                combined.contains(table),
                "missing table definition: {table}"
            );
        }
    }

    #[test]
    fn observation_migration_contains_required_canonical_fields() {
        for field in [
            "producer_entity_id",
            "feature_of_interest_id",
            "observed_property",
            "value_number",
            "value_string",
            "value_bool",
            "value_json",
            "unit",
            "observed_at",
            "received_at",
            "protocol",
            "payload_format",
            "raw_message_id",
        ] {
            assert!(
                MIGRATION_0005_CREATE_OBSERVATIONS.contains(field),
                "missing observation field: {field}"
            );
        }
    }

    #[test]
    fn migrations_preserve_jsonld_and_raw_payload_requirements() {
        assert!(MIGRATION_0002_CREATE_ENTITIES.contains("jsonld jsonb NOT NULL"));
        assert!(MIGRATION_0004_CREATE_RAW_MESSAGES.contains("payload bytea NOT NULL"));
        assert!(MIGRATION_0005_CREATE_OBSERVATIONS.contains("create_hypertable"));
    }

    #[test]
    fn runtime_persistence_migration_uses_jsonb_for_structured_fields() {
        for field in [
            "payload jsonb NOT NULL",
            "metadata jsonb",
            "policy_decision jsonb",
            "result_payload jsonb NOT NULL",
            "condition jsonb NOT NULL",
            "action jsonb NOT NULL",
            "attribute_mapping jsonb",
        ] {
            assert!(
                MIGRATION_0006_CREATE_RUNTIME_PERSISTENCE_TABLES.contains(field),
                "missing JSONB field: {field}"
            );
        }
    }

    #[test]
    fn runtime_persistence_migration_defines_common_query_indexes() {
        for index in [
            "entities_tenant_type_idx",
            "entity_relationships_source_idx",
            "entity_relationships_target_idx",
            "observations_tenant_feature_time_idx",
            "observations_tenant_producer_time_idx",
            "raw_messages_producer_received_at_idx",
            "raw_messages_feature_received_at_idx",
            "raw_messages_payload_format_received_at_idx",
            "commands_target_status_idx",
            "commands_approval_status_idx",
            "events_target_idx",
            "events_source_idx",
            "events_type_idx",
            "events_severity_idx",
            "events_command_idx",
            "events_raw_message_idx",
            "events_correlation_idx",
            "executor_agents_agent_key_idx",
            "executor_agents_status_idx",
            "edge_adapters_adapter_key_idx",
            "edge_adapters_status_idx",
            "edge_adapter_statuses_observed_at_idx",
            "command_leases_command_idx",
            "command_leases_executor_idx",
            "command_leases_status_expires_idx",
            "rules_enabled_trigger_idx",
            "rules_observed_property_idx",
            "rules_event_type_idx",
        ] {
            let defined = ORDERED_MIGRATIONS
                .iter()
                .any(|(_, sql)| sql.contains(index));
            assert!(defined, "missing index: {index}");
        }
    }

    #[test]
    fn embedded_migration_files_have_statement_terminators() {
        for (name, sql) in ORDERED_MIGRATIONS {
            assert!(!sql.trim().is_empty(), "migration is empty: {name}");
            assert!(
                sql.trim_end().ends_with(';'),
                "migration does not end with semicolon: {name}"
            );
        }
    }

    #[test]
    fn in_memory_storage_satisfies_logical_store_boundaries() {
        fn assert_control_plane<T: ControlPlaneStore>() {}
        fn assert_telemetry<T: TelemetryStore>() {}
        fn assert_ai_context<T: AiContextStore>() {}

        assert_control_plane::<InMemoryStorage>();
        assert_telemetry::<InMemoryStorage>();
        assert_ai_context::<InMemoryStorage>();
    }

    #[test]
    fn in_memory_storage_stores_and_revokes_api_tokens() {
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let now = Utc.with_ymd_and_hms(2026, 5, 5, 12, 0, 0).unwrap();
        let token = ApiToken::new(
            tenant_id,
            "operator token",
            "abcd1234",
            "deadbeef",
            ApiTokenPrincipalType::Service,
            Some("service-01".to_string()),
            vec!["entities:read".to_string()],
            None,
            Some(json!({"suite": "memory"})),
            now,
        )
        .expect("valid api token");

        assert_eq!(
            storage
                .create_api_token(token.clone())
                .expect("create api token"),
            token
        );
        assert_eq!(
            storage
                .find_api_token_by_prefix(tenant_id, "abcd1234")
                .expect("find api token by prefix")
                .expect("missing api token by prefix"),
            token
        );

        let updated = storage
            .update_api_token_last_used_at(tenant_id, token.id, now)
            .expect("update api token last used at");
        assert_eq!(updated.last_used_at, Some(now));

        let revoked = storage
            .revoke_api_token(tenant_id, token.id, now)
            .expect("revoke api token");
        assert_eq!(revoked.revoked_at, Some(now));
    }

    #[test]
    fn in_memory_storage_creates_and_lists_entities() {
        use chrono::TimeZone;
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let entity = Entity::new(
            tenant_id,
            "sensor-01",
            "aion:Sensor",
            json!({
                "@context": {"aion": "https://aioncore.org/ns#"},
                "@id": "urn:aion:sensor:sensor-01",
                "@type": "aion:Sensor"
            }),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap(),
        )
        .unwrap();

        storage.create_entity(entity.clone()).unwrap();

        assert_eq!(
            storage
                .get_entity_by_key(tenant_id, "sensor-01")
                .unwrap()
                .unwrap(),
            entity
        );
        assert_eq!(storage.list_entities(tenant_id).unwrap().len(), 1);

        let mut updated = entity.clone();
        updated.entity_type = "aion:OperationalSensor".to_string();
        updated.jsonld = json!({
            "@context": {"aion": "https://aioncore.org/ns#"},
            "@id": "urn:aion:sensor:sensor-01",
            "@type": "aion:OperationalSensor",
            "name": "Updated"
        });
        updated.updated_at = Utc.with_ymd_and_hms(2026, 4, 27, 12, 5, 0).unwrap();
        let stored = storage.update_entity(updated.clone()).unwrap();

        assert_eq!(stored.entity_key, entity.entity_key);
        assert_eq!(stored.id, entity.id);
        assert_eq!(stored.entity_type, "aion:OperationalSensor");
        assert_eq!(
            storage
                .get_entity_by_key(tenant_id, "sensor-01")
                .unwrap()
                .unwrap()
                .jsonld["name"],
            "Updated"
        );
    }

    #[test]
    fn in_memory_storage_queries_observations_by_feature() {
        use aion_observation::ObservationValue;
        use chrono::TimeZone;
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let producer_entity_id = Uuid::new_v4();
        let feature_of_interest_id = Uuid::new_v4();
        let observed_at = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();

        let observation = Observation::new(
            tenant_id,
            producer_entity_id,
            feature_of_interest_id,
            "temperature",
            ObservationValue::Number { value: 21.4 },
            Some("Cel".to_string()),
            observed_at,
            observed_at,
            "http",
            "json_mapping",
            None,
            json!({}),
            json!({}),
        )
        .unwrap();

        storage.store_observation(observation).unwrap();

        let observations = storage
            .query_observations(
                tenant_id,
                Some(feature_of_interest_id),
                None,
                None,
                None,
                10,
            )
            .unwrap();

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].observed_property, "temperature");
    }

    #[test]
    fn in_memory_storage_queries_observations_chronologically() {
        use aion_observation::ObservationValue;
        use chrono::TimeZone;
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let producer_entity_id = Uuid::new_v4();
        let feature_of_interest_id = Uuid::new_v4();
        let times = [
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 2, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2026, 4, 27, 12, 1, 0).unwrap(),
        ];

        for observed_at in times {
            let observation = Observation::new(
                tenant_id,
                producer_entity_id,
                feature_of_interest_id,
                "temperature",
                ObservationValue::Number {
                    value: observed_at.timestamp() as f64,
                },
                Some("Cel".to_string()),
                observed_at,
                observed_at,
                "http",
                "json_mapping",
                None,
                json!({}),
                json!({}),
            )
            .unwrap();
            storage.store_observation(observation).unwrap();
        }

        let observations = storage
            .query_observations_chronological(
                tenant_id,
                Some(feature_of_interest_id),
                Some("temperature"),
                None,
                None,
                10,
            )
            .unwrap();

        assert_eq!(observations.len(), 3);
        assert_eq!(
            observations
                .iter()
                .map(|observation| observation.observed_at)
                .collect::<Vec<_>>(),
            vec![
                Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 4, 27, 12, 1, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 4, 27, 12, 2, 0).unwrap(),
            ]
        );
    }

    #[test]
    fn in_memory_storage_puts_and_gets_payload_profiles() {
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let entity_id = Uuid::new_v4();
        let profile = PayloadProfile::new(
            entity_id,
            "ultralight",
            Some("http".to_string()),
            Some("text/plain".to_string()),
            Some(json!({
                "m": {
                    "observed_property": "aion:SoilMoisture",
                    "unit": "%"
                }
            })),
            Some(json!({"source": "test"})),
        )
        .unwrap();

        storage
            .put_payload_profile(tenant_id, profile.clone())
            .unwrap();

        assert_eq!(
            storage
                .get_payload_profile(tenant_id, entity_id)
                .unwrap()
                .unwrap(),
            profile
        );
    }

    #[test]
    fn in_memory_storage_creates_and_lists_ingestion_connectors() {
        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let connector = IngestionConnector::new(
            tenant_id,
            "default-http",
            IngestionConnectorType::Http,
            ConnectorProfile::Custom,
            false,
            Some("Default HTTP".to_string()),
            Some("http".to_string()),
            Some("/ingestion/connectors/default-http/ingest".to_string()),
            None,
            None,
            None,
            Some("/ingestion/connectors/default-http/ingest".to_string()),
            Some("senml-json".to_string()),
            Some("application/senml+json".to_string()),
            None,
            None,
            None,
            Utc::now(),
        )
        .unwrap();

        let connector = storage.create_ingestion_connector(connector).unwrap();
        assert_eq!(
            storage
                .get_ingestion_connector(tenant_id, connector.id)
                .unwrap()
                .unwrap()
                .connector_key,
            "default-http"
        );
        assert_eq!(
            storage.list_ingestion_connectors(tenant_id).unwrap().len(),
            1
        );

        let mut enabled = connector.clone();
        enabled.set_enabled(true, Utc::now());
        enabled.display_name = Some("Updated HTTP".to_string());
        enabled.payload_format = Some("canonical-json".to_string());
        enabled.metadata = Some(serde_json::json!({"updated": true}));
        let enabled = storage.update_ingestion_connector(enabled).unwrap();
        assert!(enabled.enabled);
        assert_eq!(enabled.display_name.as_deref(), Some("Updated HTTP"));
        assert_eq!(enabled.payload_format.as_deref(), Some("canonical-json"));
        assert_eq!(enabled.metadata, Some(serde_json::json!({"updated": true})));
    }

    #[test]
    fn in_memory_storage_creates_lists_and_deletes_connector_secrets() {
        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let now = Utc::now();
        let secret = ConnectorSecret::new(
            tenant_id,
            "farm-broker",
            ConnectorSecretType::MqttBasicAuth,
            Some("mqtt-user".to_string()),
            "super-secret",
            Some(serde_json::json!({"purpose": "test"})),
            now,
        )
        .unwrap();

        let secret = storage.create_connector_secret(secret).unwrap();
        assert_eq!(
            storage
                .get_connector_secret(tenant_id, secret.id)
                .unwrap()
                .unwrap()
                .secret_value,
            "super-secret"
        );
        assert_eq!(storage.list_connector_secrets(tenant_id).unwrap().len(), 1);
        assert!(!format!("{secret:?}").contains("super-secret"));

        storage
            .delete_connector_secret(tenant_id, secret.id)
            .unwrap();
        assert!(storage
            .get_connector_secret(tenant_id, secret.id)
            .unwrap()
            .is_none());
    }

    #[test]
    fn in_memory_storage_creates_updates_and_finds_ttn_device_mappings() {
        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let connector_id = Uuid::new_v4();
        let producer_entity_id = Uuid::new_v4();
        let feature_of_interest_id = Uuid::new_v4();
        let now = Utc::now();
        let generic = TtnDeviceMapping::new(
            tenant_id,
            connector_id,
            None,
            "soil-node-01",
            producer_entity_id,
            Some(feature_of_interest_id),
            true,
            Some(serde_json::json!({"scope": "generic"})),
            now,
        )
        .unwrap();
        let application_specific = TtnDeviceMapping::new(
            tenant_id,
            connector_id,
            Some("farm-app".to_string()),
            "soil-node-01",
            producer_entity_id,
            Some(feature_of_interest_id),
            true,
            Some(serde_json::json!({"scope": "application"})),
            now,
        )
        .unwrap();

        let generic = storage.create_ttn_device_mapping(generic).unwrap();
        let application_specific = storage
            .create_ttn_device_mapping(application_specific)
            .unwrap();

        assert_eq!(
            storage
                .get_ttn_device_mapping(tenant_id, connector_id, generic.id)
                .unwrap()
                .unwrap(),
            generic
        );
        assert_eq!(
            storage
                .list_ttn_device_mappings(tenant_id, connector_id)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            storage
                .find_ttn_device_mapping(tenant_id, connector_id, Some("farm-app"), "soil-node-01")
                .unwrap()
                .unwrap()
                .id,
            application_specific.id
        );
        assert_eq!(
            storage
                .find_ttn_device_mapping(tenant_id, connector_id, Some("other-app"), "soil-node-01")
                .unwrap()
                .unwrap()
                .id,
            generic.id
        );

        let mut disabled = application_specific.clone();
        disabled.set_enabled(false, Utc::now());
        storage.update_ttn_device_mapping(disabled).unwrap();
        assert_eq!(
            storage
                .find_ttn_device_mapping(tenant_id, connector_id, Some("farm-app"), "soil-node-01")
                .unwrap()
                .unwrap()
                .id,
            generic.id
        );
        assert!(matches!(
            storage.create_ttn_device_mapping(
                TtnDeviceMapping::new(
                    tenant_id,
                    connector_id,
                    None,
                    "soil-node-01",
                    producer_entity_id,
                    Some(feature_of_interest_id),
                    true,
                    None,
                    Utc::now(),
                )
                .unwrap()
            ),
            Err(StorageError::ConflictWithMessage(_))
        ));
        storage
            .delete_ttn_device_mapping(tenant_id, connector_id, generic.id)
            .unwrap();
        assert!(storage
            .find_ttn_device_mapping(tenant_id, connector_id, Some("other-app"), "soil-node-01")
            .unwrap()
            .is_none());
    }

    #[test]
    fn in_memory_storage_lists_raw_messages_by_tenant() {
        use aion_raw_message::RawMessageSource;
        use chrono::TimeZone;
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let other_tenant_id = Uuid::new_v4();
        let received_at = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let raw = RawMessage::new(
            tenant_id,
            RawMessageSource::Http,
            Some("/ingest/http".to_string()),
            Some("sensor-01".to_string()),
            Some("senml-json".to_string()),
            Some("application/senml+json".to_string()),
            None,
            None,
            Some("senml-json".to_string()),
            json!({"payload_format": "senml-json"}),
            br#"[{"n":"temperature","v":21.4}]"#.to_vec(),
            received_at,
        )
        .unwrap();
        let other = RawMessage::new(
            other_tenant_id,
            RawMessageSource::Http,
            Some("/ingest/http".to_string()),
            Some("sensor-02".to_string()),
            Some("senml-json".to_string()),
            Some("application/senml+json".to_string()),
            None,
            None,
            Some("senml-json".to_string()),
            json!({"payload_format": "senml-json"}),
            br#"[{"n":"temperature","v":22.4}]"#.to_vec(),
            received_at,
        )
        .unwrap();

        storage.store_raw_message(raw.clone()).unwrap();
        storage.store_raw_message(other).unwrap();

        assert_eq!(storage.list_raw_messages(tenant_id).unwrap(), vec![raw]);
    }

    #[test]
    fn in_memory_storage_indexes_raw_messages_by_tenant_scoped_idempotency_key() {
        use aion_raw_message::RawMessageSource;
        use chrono::TimeZone;
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let other_tenant_id = Uuid::new_v4();
        let received_at = Utc.with_ymd_and_hms(2026, 5, 6, 12, 0, 0).unwrap();

        let mut first = RawMessage::new(
            tenant_id,
            RawMessageSource::Http,
            Some("/ingest/reliable".to_string()),
            Some("sensor-01".to_string()),
            Some("senml-json".to_string()),
            Some("application/json".to_string()),
            None,
            None,
            Some("senml-json".to_string()),
            json!({}),
            br#"[{"n":"temperature","v":21.4}]"#.to_vec(),
            received_at,
        )
        .unwrap();
        first.idempotency_key = Some("tenant-a:key-01".to_string());

        let mut same_tenant_duplicate = RawMessage::new(
            tenant_id,
            RawMessageSource::Http,
            Some("/ingest/reliable".to_string()),
            Some("sensor-01".to_string()),
            Some("senml-json".to_string()),
            Some("application/json".to_string()),
            None,
            None,
            Some("senml-json".to_string()),
            json!({}),
            br#"[{"n":"temperature","v":21.5}]"#.to_vec(),
            received_at,
        )
        .unwrap();
        same_tenant_duplicate.idempotency_key = Some("tenant-a:key-01".to_string());

        let mut other_tenant_same_key = RawMessage::new(
            other_tenant_id,
            RawMessageSource::Http,
            Some("/ingest/reliable".to_string()),
            Some("sensor-02".to_string()),
            Some("senml-json".to_string()),
            Some("application/json".to_string()),
            None,
            None,
            Some("senml-json".to_string()),
            json!({}),
            br#"[{"n":"temperature","v":22.4}]"#.to_vec(),
            received_at,
        )
        .unwrap();
        other_tenant_same_key.idempotency_key = Some("tenant-a:key-01".to_string());

        storage.store_raw_message(first.clone()).unwrap();
        assert_eq!(
            storage
                .find_raw_message_by_idempotency_key(tenant_id, "tenant-a:key-01")
                .unwrap()
                .unwrap(),
            first
        );
        assert!(matches!(
            storage.store_raw_message(same_tenant_duplicate),
            Err(StorageError::Conflict)
        ));
        storage.store_raw_message(other_tenant_same_key).unwrap();
    }

    #[test]
    fn in_memory_storage_links_command_action_and_result() {
        use chrono::TimeZone;
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let target_entity_id = Uuid::new_v4();
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let command = Command::new(
            tenant_id,
            target_entity_id,
            "StartPump",
            json!({"target_state": "on"}),
            None,
            None,
            None,
            None,
            now,
        )
        .unwrap();

        storage.store_command(command.clone()).unwrap();
        let action = Action::new(
            tenant_id,
            command.id,
            None,
            "StartPump",
            "started",
            Some(now),
            None,
            None,
        )
        .unwrap();
        storage.store_action(action.clone()).unwrap();

        let result = ActionResult::new(
            tenant_id,
            command.id,
            action.id,
            "succeeded",
            true,
            json!({"pump_state": "running"}),
            now,
            None,
        )
        .unwrap();
        storage.store_action_result(result.clone()).unwrap();

        assert_eq!(
            storage
                .query_commands(
                    tenant_id,
                    Some(target_entity_id),
                    Some(CommandStatus::Pending)
                )
                .unwrap(),
            vec![command]
        );
        assert_eq!(
            storage
                .query_actions(tenant_id, Some(action.command_id))
                .unwrap(),
            vec![action]
        );
        assert_eq!(
            storage
                .query_action_results(tenant_id, None, Some(result.command_id))
                .unwrap(),
            vec![result]
        );
    }

    #[test]
    fn in_memory_storage_filters_events() {
        use aion_event::{Event, EventSeverity};
        use chrono::TimeZone;
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let target_entity_id = Uuid::new_v4();
        let command_id = Uuid::new_v4();
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let event = Event::new(
            tenant_id,
            "aion:CommandCreated",
            EventSeverity::Info,
            None,
            Some(target_entity_id),
            Some("Command created".to_string()),
            now,
            None,
            Some("corr-001".to_string()),
            None,
            None,
            Some(command_id),
            None,
            None,
            Some(json!({"source": "test"})),
            now,
        )
        .unwrap();

        storage.store_event(event.clone()).unwrap();
        let events = storage
            .query_events(
                tenant_id,
                EventFilter {
                    target_entity_id: Some(target_entity_id),
                    event_type: Some("aion:CommandCreated".to_string()),
                    severity: Some(EventSeverity::Info),
                    command_id: Some(command_id),
                    correlation_id: Some("corr-001".to_string()),
                    ..EventFilter::default()
                },
            )
            .unwrap();

        assert_eq!(events, vec![event]);
    }

    #[test]
    fn in_memory_storage_creates_updates_lists_and_deletes_flows() {
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let now = Utc::now();
        let flow = Flow::new(
            tenant_id,
            "mqtt-normalize-store",
            "MQTT Normalize Store",
            Some("test flow".to_string()),
            false,
            vec![
                FlowNode {
                    node_id: "source-1".to_string(),
                    node_type: FlowNodeType::Source,
                    name: Some("MQTT Source".to_string()),
                    config: json!({"kind": "mqtt_subscribe", "connector_id": "abc"}),
                    position: None,
                },
                FlowNode {
                    node_id: "sink-1".to_string(),
                    node_type: FlowNodeType::Sink,
                    name: Some("Store".to_string()),
                    config: json!({"kind": "internal_observation_store"}),
                    position: None,
                },
            ],
            vec![FlowEdge {
                edge_id: "edge-1".to_string(),
                source_node_id: "source-1".to_string(),
                target_node_id: "sink-1".to_string(),
                label: None,
                metadata: None,
            }],
            Some(json!({"suite": "storage"})),
            now,
        )
        .unwrap();

        let stored = storage.create_flow(flow.clone()).unwrap();
        assert_eq!(
            storage.get_flow(tenant_id, stored.id).unwrap().unwrap(),
            stored
        );
        assert_eq!(storage.list_flows(tenant_id).unwrap(), vec![stored.clone()]);

        let mut updated = stored.clone();
        updated.name = "Updated Flow".to_string();
        updated.set_enabled(true, Utc::now());
        let updated = storage.update_flow(updated).unwrap();
        assert!(updated.enabled);
        assert_eq!(updated.name, "Updated Flow");

        storage.delete_flow(tenant_id, updated.id).unwrap();
        assert!(storage.get_flow(tenant_id, updated.id).unwrap().is_none());
    }

    #[test]
    fn in_memory_storage_creates_filters_and_updates_dlq_records() {
        use serde_json::json;

        let storage = InMemoryStorage::new();
        let tenant_id = Uuid::new_v4();
        let other_tenant_id = Uuid::new_v4();
        let raw_message_id = Uuid::new_v4();
        let connector_id = Uuid::new_v4();
        let now = Utc.with_ymd_and_hms(2026, 5, 6, 12, 0, 0).unwrap();

        let record = DlqRecord::new(
            tenant_id,
            Some("decode-01".to_string()),
            Some("minifi".to_string()),
            Some("edge-01".to_string()),
            Some(connector_id),
            None,
            Some(raw_message_id),
            None,
            None,
            Some("tenant-a:key-01".to_string()),
            Some("flow-01".to_string()),
            Some("Edge Sync".to_string()),
            Some("flowfile-01".to_string()),
            Some("pg-01".to_string()),
            Some("proc-01".to_string()),
            Some("nifi://provenance/1".to_string()),
            Some("sync-01".to_string()),
            Some("senml-json".to_string()),
            Some(json!({"raw": true})),
            Some("sha256:abc".to_string()),
            DlqFailureStage::Decoding,
            "decoder rejected payload",
            Some("invalid measurement".to_string()),
            2,
            1,
            DlqStatus::Pending,
            Some(json!({"external.source_system": "minifi"})),
            now,
        )
        .unwrap();
        let other_record = DlqRecord::new(
            other_tenant_id,
            Some("validation-01".to_string()),
            Some("nifi".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("flowfile-02".to_string()),
            None,
            None,
            None,
            None,
            Some("canonical-json".to_string()),
            Some(json!({"payload": 1})),
            None,
            DlqFailureStage::Validation,
            "schema mismatch",
            None,
            0,
            0,
            DlqStatus::Inspecting,
            None,
            now + chrono::Duration::seconds(1),
        )
        .unwrap();

        let record = storage.create_dlq_record(record).unwrap();
        storage.create_dlq_record(other_record).unwrap();

        assert_eq!(
            storage
                .get_dlq_record(tenant_id, record.id)
                .unwrap()
                .unwrap()
                .id,
            record.id
        );
        assert_eq!(
            storage
                .list_dlq_records(
                    tenant_id,
                    DlqRecordFilter {
                        status: Some(DlqStatus::Pending),
                        failure_stage: Some(DlqFailureStage::Decoding),
                        source_system: Some("minifi".to_string()),
                        connector_id: Some(connector_id),
                        raw_message_id: Some(raw_message_id),
                        idempotency_key: Some("tenant-a:key-01".to_string()),
                        external_flowfile_uuid: Some("flowfile-01".to_string()),
                        sync_session_id: Some("sync-01".to_string()),
                        limit: 10,
                        ..DlqRecordFilter::default()
                    }
                )
                .unwrap(),
            vec![record.clone()]
        );

        let updated = storage
            .update_dlq_record_status(
                tenant_id,
                record.id,
                DlqStatus::Resolved,
                now + chrono::Duration::seconds(5),
            )
            .unwrap();
        assert_eq!(updated.status, DlqStatus::Resolved);
        assert!(updated.resolved_at.is_some());
    }

    #[test]
    fn ingestion_connector_migration_defines_required_indexes() {
        let migration = MIGRATION_0007_CREATE_INGESTION_CONNECTORS;
        for index in [
            "idx_ingestion_connectors_tenant",
            "idx_ingestion_connectors_connector_key",
            "idx_ingestion_connectors_connector_type",
            "idx_ingestion_connectors_connector_profile",
            "idx_ingestion_connectors_enabled",
        ] {
            assert!(migration.contains(index), "missing index: {index}");
        }
    }

    #[test]
    fn connector_secret_migration_defines_required_indexes_and_redaction_columns() {
        let migration = MIGRATION_0008_CREATE_CONNECTOR_SECRETS;
        for required in [
            "CREATE TABLE IF NOT EXISTS connector_secrets",
            "secret_value TEXT NOT NULL",
            "secret_type IN ('mqtt_basic_auth', 'token', 'api_key', 'custom')",
            "ADD COLUMN IF NOT EXISTS secret_ref_id UUID REFERENCES connector_secrets(id) ON DELETE SET NULL",
            "idx_connector_secrets_tenant",
            "idx_connector_secrets_secret_key",
            "idx_connector_secrets_secret_type",
            "idx_ingestion_connectors_secret_ref",
        ] {
            assert!(migration.contains(required), "missing migration item: {required}");
        }
    }

    #[test]
    fn flow_migration_defines_required_columns_and_indexes() {
        let migration = MIGRATION_0013_CREATE_FLOWS;
        for required in [
            "CREATE TABLE IF NOT EXISTS flows",
            "flow_key TEXT NOT NULL",
            "nodes JSONB NOT NULL DEFAULT '[]'::jsonb",
            "edges JSONB NOT NULL DEFAULT '[]'::jsonb",
            "CONSTRAINT flows_tenant_key_unique UNIQUE (tenant_id, flow_key)",
            "idx_flows_tenant_created_at",
            "idx_flows_flow_key",
            "idx_flows_enabled",
        ] {
            assert!(
                migration.contains(required),
                "missing migration item: {required}"
            );
        }
    }

    #[test]
    fn dlq_migration_defines_required_columns_and_indexes() {
        let migration = MIGRATION_0014_CREATE_DLQ_RECORDS;
        for required in [
            "CREATE TABLE IF NOT EXISTS dlq_records",
            "dlq_key TEXT",
            "source_system TEXT",
            "external_flowfile_uuid TEXT",
            "external_provenance_uri TEXT",
            "payload JSONB",
            "payload_hash TEXT",
            "failure_stage TEXT NOT NULL CHECK",
            "failure_reason TEXT NOT NULL",
            "retry_count INTEGER NOT NULL DEFAULT 0",
            "replay_count INTEGER NOT NULL DEFAULT 0",
            "status TEXT NOT NULL CHECK",
            "metadata JSONB NOT NULL DEFAULT '{}'::jsonb",
            "idx_dlq_records_status",
            "idx_dlq_records_failure_stage",
            "idx_dlq_records_source_system",
            "idx_dlq_records_idempotency_key",
            "idx_dlq_records_external_flowfile_uuid",
            "idx_dlq_records_sync_session_id",
        ] {
            assert!(
                migration.contains(required),
                "missing migration item: {required}"
            );
        }
    }

    #[test]
    fn raw_message_idempotency_migration_defines_required_columns_and_indexes() {
        let migration = MIGRATION_0015_ADD_RAW_MESSAGE_IDEMPOTENCY;
        for required in [
            "ALTER TABLE raw_messages",
            "ADD COLUMN IF NOT EXISTS idempotency_key TEXT",
            "raw_messages_idempotency_lookup_idx",
            "raw_messages_tenant_idempotency_unique_idx",
            "WHERE idempotency_key IS NOT NULL",
        ] {
            assert!(
                migration.contains(required),
                "missing migration item: {required}"
            );
        }
    }

    #[test]
    fn ttn_device_mapping_migration_defines_required_columns_and_indexes() {
        let migration = MIGRATION_0009_CREATE_TTN_DEVICE_MAPPINGS;
        for required in [
            "CREATE TABLE IF NOT EXISTS ttn_device_mappings",
            "connector_id UUID NOT NULL REFERENCES ingestion_connectors(id) ON DELETE CASCADE",
            "ttn_application_id TEXT",
            "ttn_device_id TEXT NOT NULL",
            "producer_entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE RESTRICT",
            "feature_of_interest_id UUID REFERENCES entities(id) ON DELETE SET NULL",
            "idx_ttn_device_mappings_connector",
            "idx_ttn_device_mappings_device",
            "idx_ttn_device_mappings_enabled",
        ] {
            assert!(
                migration.contains(required),
                "missing migration item: {required}"
            );
        }
    }

    #[test]
    fn ttn_device_mapping_hardening_migration_defines_enabled_uniqueness() {
        let migration = MIGRATION_0010_HARDEN_TTN_DEVICE_MAPPING_UNIQUENESS;
        for required in [
            "DROP INDEX IF EXISTS idx_ttn_device_mappings_unique_no_application",
            "idx_ttn_device_mappings_unique_enabled_application",
            "idx_ttn_device_mappings_unique_enabled_no_application",
            "WHERE enabled = TRUE AND ttn_application_id IS NOT NULL",
            "WHERE enabled = TRUE AND ttn_application_id IS NULL",
        ] {
            assert!(
                migration.contains(required),
                "missing migration item: {required}"
            );
        }
    }
}
