use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ObservationValue {
    Number { value: f64 },
    Text { value: String },
    Bool { value: bool },
    Json { value: Value },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationError {
    EmptyObservedProperty,
}

impl fmt::Display for ObservationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyObservedProperty => f.write_str("observed_property must not be empty"),
        }
    }
}

impl std::error::Error for ObservationError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Observation {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub entity_id: Uuid,
    pub observed_property: String,
    pub time: DateTime<Utc>,
    pub value: ObservationValue,
    pub unit: Option<String>,
    pub raw_message_id: Option<Uuid>,
    pub quality: Value,
    pub metadata: Value,
}

impl Observation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        entity_id: Uuid,
        observed_property: impl Into<String>,
        time: DateTime<Utc>,
        value: ObservationValue,
        unit: Option<String>,
        raw_message_id: Option<Uuid>,
        quality: Value,
        metadata: Value,
    ) -> Result<Self, ObservationError> {
        let observed_property = observed_property.into();

        if observed_property.trim().is_empty() {
            return Err(ObservationError::EmptyObservedProperty);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            entity_id,
            observed_property,
            time,
            value,
            unit,
            raw_message_id,
            quality,
            metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn creates_numeric_observation() {
        let tenant_id = Uuid::new_v4();
        let entity_id = Uuid::new_v4();
        let raw_message_id = Uuid::new_v4();
        let time = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();

        let observation = Observation::new(
            tenant_id,
            entity_id,
            "temperature",
            time,
            ObservationValue::Number { value: 21.4 },
            Some("Cel".to_string()),
            Some(raw_message_id),
            json!({}),
            json!({"decoder": "senml_json"}),
        )
        .expect("observation should be valid");

        assert_eq!(observation.tenant_id, tenant_id);
        assert_eq!(observation.entity_id, entity_id);
        assert_eq!(observation.observed_property, "temperature");
        assert_eq!(observation.unit.as_deref(), Some("Cel"));
        assert_eq!(observation.raw_message_id, Some(raw_message_id));
    }

    #[test]
    fn rejects_empty_observed_property() {
        let err = Observation::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            " ",
            Utc::now(),
            ObservationValue::Bool { value: true },
            None,
            None,
            json!({}),
            json!({}),
        )
        .expect_err("empty observed property should fail");

        assert_eq!(err, ObservationError::EmptyObservedProperty);
    }
}
