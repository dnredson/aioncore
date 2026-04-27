use aion_entity::Entity;
use aion_observation::Observation;
use aion_raw_message::RawMessage;
use aion_relationship::Relationship;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
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
}
