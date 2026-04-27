use aion_entity::Entity;
use aion_observation::Observation;
use aion_raw_message::RawMessage;
use aion_relationship::Relationship;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, RwLock},
};
use uuid::Uuid;

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
    InvalidInput(String),
    Backend(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("record was not found"),
            Self::Conflict => f.write_str("record conflicts with existing data"),
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
    fn get_entity(&self, tenant_id: Uuid, entity_id: Uuid) -> StorageResult<Option<Entity>>;
    fn get_entity_by_key(&self, tenant_id: Uuid, entity_key: &str)
        -> StorageResult<Option<Entity>>;
    fn list_entities(&self, tenant_id: Uuid) -> StorageResult<Vec<Entity>>;
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
    fn get_raw_message(
        &self,
        tenant_id: Uuid,
        raw_message_id: Uuid,
    ) -> StorageResult<Option<RawMessage>>;
    fn list_raw_messages(&self, tenant_id: Uuid) -> StorageResult<Vec<RawMessage>>;
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
    fn query_observations(
        &self,
        tenant_id: Uuid,
        feature_of_interest_id: Option<Uuid>,
        observed_property: Option<&str>,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: u32,
    ) -> StorageResult<Vec<Observation>>;
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
    observations: HashMap<Uuid, Observation>,
    payload_profiles: HashMap<(Uuid, Uuid), PayloadProfile>,
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

    fn get_entity(&self, tenant_id: Uuid, entity_id: Uuid) -> StorageResult<Option<Entity>> {
        Ok(self
            .read_state()?
            .entities
            .get(&entity_id)
            .filter(|entity| entity.tenant_id == tenant_id)
            .cloned())
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

        state
            .raw_messages
            .insert(raw_message.id, raw_message.clone());
        Ok(raw_message)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_ordered_migrations() {
        assert_eq!(ORDERED_MIGRATIONS.len(), 5);
        assert_eq!(ORDERED_MIGRATIONS[0].0, "0001_create_tenants.sql");
        assert_eq!(ORDERED_MIGRATIONS[4].0, "0005_create_observations.sql");
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
            json!({"payload_format": "senml-json"}),
            br#"[{"n":"temperature","v":22.4}]"#.to_vec(),
            received_at,
        )
        .unwrap();

        storage.store_raw_message(raw.clone()).unwrap();
        storage.store_raw_message(other).unwrap();

        assert_eq!(storage.list_raw_messages(tenant_id).unwrap(), vec![raw]);
    }
}
