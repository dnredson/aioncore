use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityError {
    EmptyEntityKey,
    EmptyEntityType,
    JsonLdMustBeObject,
    MissingContext,
    MissingType,
    MissingIdentifier,
}

impl fmt::Display for EntityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyEntityKey => "entity_key must not be empty",
            Self::EmptyEntityType => "entity_type must not be empty",
            Self::JsonLdMustBeObject => "jsonld must be a JSON object",
            Self::MissingContext => "jsonld must include @context",
            Self::MissingType => "jsonld must include @type",
            Self::MissingIdentifier => "jsonld must include @id or entity_key must be provided",
        };
        f.write_str(message)
    }
}

impl std::error::Error for EntityError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub entity_key: String,
    pub entity_type: String,
    pub jsonld: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Entity {
    pub fn new(
        tenant_id: Uuid,
        entity_key: impl Into<String>,
        entity_type: impl Into<String>,
        jsonld: Value,
        now: DateTime<Utc>,
    ) -> Result<Self, EntityError> {
        let entity_key = entity_key.into();
        let entity_type = entity_type.into();

        validate_entity_key(&entity_key)?;
        validate_entity_type(&entity_type)?;
        validate_jsonld(&jsonld, &entity_key)?;

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            entity_key,
            entity_type,
            jsonld,
            created_at: now,
            updated_at: now,
        })
    }
}

fn validate_entity_key(entity_key: &str) -> Result<(), EntityError> {
    if entity_key.trim().is_empty() {
        return Err(EntityError::EmptyEntityKey);
    }
    Ok(())
}

fn validate_entity_type(entity_type: &str) -> Result<(), EntityError> {
    if entity_type.trim().is_empty() {
        return Err(EntityError::EmptyEntityType);
    }
    Ok(())
}

fn validate_jsonld(jsonld: &Value, entity_key: &str) -> Result<(), EntityError> {
    let object = jsonld.as_object().ok_or(EntityError::JsonLdMustBeObject)?;

    if !object.contains_key("@context") {
        return Err(EntityError::MissingContext);
    }

    if !object.contains_key("@type") {
        return Err(EntityError::MissingType);
    }

    if !object.contains_key("@id") && entity_key.trim().is_empty() {
        return Err(EntityError::MissingIdentifier);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn creates_valid_entity() {
        let tenant_id = Uuid::new_v4();
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let jsonld = json!({
            "@context": {"aion": "https://aioncore.org/ns#"},
            "@id": "urn:aion:device:weather-station-01",
            "@type": "aion:Device",
            "name": "Weather Station 01"
        });

        let entity = Entity::new(tenant_id, "weather-station-01", "aion:Device", jsonld, now)
            .expect("entity should be valid");

        assert_eq!(entity.tenant_id, tenant_id);
        assert_eq!(entity.entity_key, "weather-station-01");
        assert_eq!(entity.entity_type, "aion:Device");
        assert_eq!(entity.created_at, now);
    }

    #[test]
    fn rejects_jsonld_without_context() {
        let jsonld = json!({"@type": "aion:Device"});
        let err = Entity::new(
            Uuid::new_v4(),
            "device-01",
            "aion:Device",
            jsonld,
            Utc::now(),
        )
        .expect_err("missing @context should fail");

        assert_eq!(err, EntityError::MissingContext);
    }
}
