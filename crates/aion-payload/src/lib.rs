use aion_observation::ObservationValue;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, str::FromStr};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadFormat {
    SenMlJson,
    UltraLight,
    JsonMapping,
    Unknown(String),
}

impl fmt::Display for PayloadFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SenMlJson => f.write_str("senml_json"),
            Self::UltraLight => f.write_str("ultralight"),
            Self::JsonMapping => f.write_str("json_mapping"),
            Self::Unknown(value) => f.write_str(value),
        }
    }
}

impl FromStr for PayloadFormat {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
        let format = match normalized.as_str() {
            "senml" | "senml_json" | "application/senml+json" => Self::SenMlJson,
            "ultralight" | "ultra_light" | "text/plain" => Self::UltraLight,
            "json_mapping" | "mapping" | "application/json" => Self::JsonMapping,
            _ => Self::Unknown(value.to_string()),
        };
        Ok(format)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecodeInput {
    pub tenant_id: Uuid,
    pub device_key: Option<String>,
    pub format: PayloadFormat,
    pub content_type: Option<String>,
    pub payload: Vec<u8>,
    pub received_at: DateTime<Utc>,
    pub config: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecodedMeasurement {
    pub entity_key: String,
    pub observed_property: String,
    pub time: DateTime<Utc>,
    pub value: ObservationValue,
    pub unit: Option<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    message: String,
}

impl DecodeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DecodeError {}

pub trait PayloadDecoder: Send + Sync {
    fn name(&self) -> &'static str;

    fn decode(&self, input: DecodeInput) -> Result<Vec<DecodedMeasurement>, DecodeError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    struct EchoDecoder;

    impl PayloadDecoder for EchoDecoder {
        fn name(&self) -> &'static str {
            "echo"
        }

        fn decode(&self, input: DecodeInput) -> Result<Vec<DecodedMeasurement>, DecodeError> {
            Ok(vec![DecodedMeasurement {
                entity_key: input.device_key.unwrap_or_else(|| "unknown".to_string()),
                observed_property: "payload_size".to_string(),
                time: input.received_at,
                value: ObservationValue::Number {
                    value: input.payload.len() as f64,
                },
                unit: Some("By".to_string()),
                metadata: json!({"decoder": self.name()}),
            }])
        }
    }

    #[test]
    fn parses_known_payload_formats() {
        assert_eq!(
            "senml_json".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::SenMlJson
        );
        assert_eq!(
            "text/plain".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::UltraLight
        );
        assert_eq!(
            "application/json".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::JsonMapping
        );
    }

    #[test]
    fn decoder_trait_can_return_measurements() {
        let received_at = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let decoder = EchoDecoder;
        let input = DecodeInput {
            tenant_id: Uuid::new_v4(),
            device_key: Some("device-01".to_string()),
            format: PayloadFormat::Unknown("echo".to_string()),
            content_type: None,
            payload: b"abc".to_vec(),
            received_at,
            config: None,
        };

        let measurements = decoder.decode(input).expect("decode should succeed");

        assert_eq!(measurements.len(), 1);
        assert_eq!(measurements[0].entity_key, "device-01");
        assert_eq!(measurements[0].observed_property, "payload_size");
        assert_eq!(measurements[0].time, received_at);
    }
}
