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
    pub quality: Value,
    pub metadata: Value,
}

impl Observation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        producer_entity_id: Uuid,
        feature_of_interest_id: Uuid,
        observed_property: impl Into<String>,
        value: ObservationValue,
        unit: Option<String>,
        observed_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
        protocol: impl Into<String>,
        payload_format: impl Into<String>,
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
            producer_entity_id,
            feature_of_interest_id,
            observed_property,
            value,
            unit,
            observed_at,
            received_at,
            protocol: protocol.into(),
            payload_format: payload_format.into(),
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
        let producer_entity_id = Uuid::new_v4();
        let feature_of_interest_id = Uuid::new_v4();
        let raw_message_id = Uuid::new_v4();
        let observed_at = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let received_at = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 1).unwrap();

        let observation = Observation::new(
            tenant_id,
            producer_entity_id,
            feature_of_interest_id,
            "temperature",
            ObservationValue::Number { value: 21.4 },
            Some("Cel".to_string()),
            observed_at,
            received_at,
            "http",
            "senml_json",
            Some(raw_message_id),
            json!({}),
            json!({"decoder": "senml_json"}),
        )
        .expect("observation should be valid");

        assert_eq!(observation.tenant_id, tenant_id);
        assert_eq!(observation.producer_entity_id, producer_entity_id);
        assert_eq!(observation.feature_of_interest_id, feature_of_interest_id);
        assert_eq!(observation.observed_property, "temperature");
        assert_eq!(observation.unit.as_deref(), Some("Cel"));
        assert_eq!(observation.observed_at, observed_at);
        assert_eq!(observation.received_at, received_at);
        assert_eq!(observation.protocol, "http");
        assert_eq!(observation.payload_format, "senml_json");
        assert_eq!(observation.raw_message_id, Some(raw_message_id));
    }

    #[test]
    fn rejects_empty_observed_property() {
        let err = Observation::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            " ",
            ObservationValue::Bool { value: true },
            None,
            Utc::now(),
            Utc::now(),
            "http",
            "json_mapping",
            None,
            json!({}),
            json!({}),
        )
        .expect_err("empty observed property should fail");

        assert_eq!(err, ObservationError::EmptyObservedProperty);
    }
}
