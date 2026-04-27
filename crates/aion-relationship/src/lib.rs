use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipError {
    EmptyRelationshipType,
    SameSourceAndTarget,
    JsonLdMustBeObject,
}

impl fmt::Display for RelationshipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyRelationshipType => "relationship_type must not be empty",
            Self::SameSourceAndTarget => "source and target entities must be different",
            Self::JsonLdMustBeObject => "jsonld must be a JSON object",
        };
        f.write_str(message)
    }
}

impl std::error::Error for RelationshipError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Relationship {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub source_entity_id: Uuid,
    pub relationship_type: String,
    pub target_entity_id: Uuid,
    pub jsonld: Value,
    pub created_at: DateTime<Utc>,
}

impl Relationship {
    pub fn new(
        tenant_id: Uuid,
        source_entity_id: Uuid,
        relationship_type: impl Into<String>,
        target_entity_id: Uuid,
        jsonld: Value,
        created_at: DateTime<Utc>,
    ) -> Result<Self, RelationshipError> {
        let relationship_type = relationship_type.into();

        if relationship_type.trim().is_empty() {
            return Err(RelationshipError::EmptyRelationshipType);
        }

        if source_entity_id == target_entity_id {
            return Err(RelationshipError::SameSourceAndTarget);
        }

        if !jsonld.is_object() {
            return Err(RelationshipError::JsonLdMustBeObject);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            source_entity_id,
            relationship_type,
            target_entity_id,
            jsonld,
            created_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn creates_valid_relationship() {
        let tenant_id = Uuid::new_v4();
        let source = Uuid::new_v4();
        let target = Uuid::new_v4();
        let created_at = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();

        let relationship = Relationship::new(
            tenant_id,
            source,
            "aion:locatedIn",
            target,
            json!({"@type": "aion:Relationship"}),
            created_at,
        )
        .expect("relationship should be valid");

        assert_eq!(relationship.tenant_id, tenant_id);
        assert_eq!(relationship.source_entity_id, source);
        assert_eq!(relationship.target_entity_id, target);
        assert_eq!(relationship.relationship_type, "aion:locatedIn");
    }

    #[test]
    fn rejects_self_relationship() {
        let entity_id = Uuid::new_v4();
        let err = Relationship::new(
            Uuid::new_v4(),
            entity_id,
            "aion:partOf",
            entity_id,
            json!({}),
            Utc::now(),
        )
        .expect_err("self relationship should fail");

        assert_eq!(err, RelationshipError::SameSourceAndTarget);
    }
}
