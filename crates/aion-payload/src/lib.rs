use aion_observation::ObservationValue;
use chrono::{DateTime, TimeDelta, Utc};
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
    CanonicalJson,
    Unknown(String),
}

impl fmt::Display for PayloadFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SenMlJson => f.write_str("senml_json"),
            Self::UltraLight => f.write_str("ultralight"),
            Self::JsonMapping => f.write_str("json_mapping"),
            Self::CanonicalJson => f.write_str("canonical_json"),
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
            "canonical_json" | "canonical" => Self::CanonicalJson,
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

#[derive(Debug, Clone, Default)]
pub struct SenMlJsonDecoder;

impl PayloadDecoder for SenMlJsonDecoder {
    fn name(&self) -> &'static str {
        "senml-json"
    }

    fn decode(&self, input: DecodeInput) -> Result<Vec<DecodedMeasurement>, DecodeError> {
        let value: Value = serde_json::from_slice(&input.payload)
            .map_err(|err| DecodeError::new(format!("invalid SenML JSON payload: {err}")))?;
        let entries = value
            .as_array()
            .ok_or_else(|| DecodeError::new("SenML JSON payload must be an array"))?;

        let mut base_name = String::new();
        let mut base_time = None;
        let mut base_unit = None;
        let mut measurements = Vec::new();

        for entry in entries {
            let object = entry
                .as_object()
                .ok_or_else(|| DecodeError::new("SenML entries must be JSON objects"))?;

            if let Some(value) = object.get("bn").and_then(Value::as_str) {
                base_name = value.to_string();
            }
            if let Some(value) = object.get("bt").and_then(Value::as_f64) {
                base_time = Some(epoch_seconds_to_utc(value)?);
            }
            if let Some(value) = object.get("bu").and_then(Value::as_str) {
                base_unit = Some(value.to_string());
            }

            let name = object
                .get("n")
                .and_then(Value::as_str)
                .ok_or_else(|| DecodeError::new("SenML entry missing string n"))?;
            let unit = object
                .get("u")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| base_unit.clone());
            let time = object
                .get("t")
                .and_then(Value::as_f64)
                .map(|value| senml_time(value, base_time))
                .transpose()?
                .or(base_time)
                .unwrap_or(input.received_at);
            let value = senml_value(object)?;

            measurements.push(DecodedMeasurement {
                entity_key: input
                    .device_key
                    .clone()
                    .unwrap_or_else(|| base_name.clone()),
                observed_property: name.to_string(),
                time,
                value,
                unit,
                metadata: serde_json::json!({
                    "decoder": self.name(),
                    "senml_name": format!("{base_name}{name}")
                }),
            });
        }

        if measurements.is_empty() {
            return Err(DecodeError::new(
                "SenML JSON payload produced no measurements",
            ));
        }

        Ok(measurements)
    }
}

#[derive(Debug, Clone, Default)]
pub struct UltraLightDecoder;

impl PayloadDecoder for UltraLightDecoder {
    fn name(&self) -> &'static str {
        "ultralight"
    }

    fn decode(&self, input: DecodeInput) -> Result<Vec<DecodedMeasurement>, DecodeError> {
        let payload = std::str::from_utf8(&input.payload)
            .map_err(|err| DecodeError::new(format!("UltraLight payload is not UTF-8: {err}")))?;
        let parts = payload.split('|').collect::<Vec<_>>();

        if parts.is_empty() || parts.len() % 2 != 0 {
            return Err(DecodeError::new(
                "UltraLight payload must contain key/value pairs separated by |",
            ));
        }

        let mappings = input.config.unwrap_or(Value::Null);
        let mut measurements = Vec::new();

        for pair in parts.chunks(2) {
            let key = pair[0].trim();
            let raw_value = pair[1].trim();
            if key.is_empty() {
                return Err(DecodeError::new(
                    "UltraLight attribute key must not be empty",
                ));
            }

            let mapping = mappings.get(key);
            let observed_property = mapping
                .and_then(|value| value.get("observed_property"))
                .and_then(Value::as_str)
                .unwrap_or(key)
                .to_string();
            let unit = mapping
                .and_then(|value| value.get("unit"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let value = raw_value
                .parse::<f64>()
                .map(|value| ObservationValue::Number { value })
                .unwrap_or_else(|_| ObservationValue::Text {
                    value: raw_value.to_string(),
                });

            measurements.push(DecodedMeasurement {
                entity_key: input.device_key.clone().unwrap_or_default(),
                observed_property,
                time: input.received_at,
                value,
                unit,
                metadata: serde_json::json!({
                    "decoder": self.name(),
                    "ultralight_key": key
                }),
            });
        }

        Ok(measurements)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CanonicalJsonDecoder;

impl PayloadDecoder for CanonicalJsonDecoder {
    fn name(&self) -> &'static str {
        "canonical-json"
    }

    fn decode(&self, input: DecodeInput) -> Result<Vec<DecodedMeasurement>, DecodeError> {
        let value: Value = serde_json::from_slice(&input.payload)
            .map_err(|err| DecodeError::new(format!("invalid canonical JSON payload: {err}")))?;
        let entries = if let Some(entries) = value.get("observations").and_then(Value::as_array) {
            entries.clone()
        } else if value.is_array() {
            value.as_array().cloned().unwrap_or_default()
        } else {
            vec![value]
        };

        let mut measurements = Vec::new();
        for entry in entries {
            let object = entry.as_object().ok_or_else(|| {
                DecodeError::new("canonical JSON observations must be JSON objects")
            })?;
            let observed_property = object
                .get("observed_property")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    DecodeError::new("canonical JSON observation missing observed_property")
                })?;
            let value = object
                .get("value")
                .ok_or_else(|| DecodeError::new("canonical JSON observation missing value"))
                .and_then(json_value_to_observation_value)?;
            let unit = object
                .get("unit")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let time = object
                .get("observed_at")
                .and_then(Value::as_str)
                .map(|value| {
                    DateTime::parse_from_rfc3339(value)
                        .map(|value| value.with_timezone(&Utc))
                        .map_err(|err| {
                            DecodeError::new(format!("invalid canonical observed_at: {err}"))
                        })
                })
                .transpose()?
                .unwrap_or(input.received_at);

            measurements.push(DecodedMeasurement {
                entity_key: input.device_key.clone().unwrap_or_default(),
                observed_property: observed_property.to_string(),
                time,
                value,
                unit,
                metadata: serde_json::json!({"decoder": self.name()}),
            });
        }

        if measurements.is_empty() {
            return Err(DecodeError::new(
                "canonical JSON payload produced no observations",
            ));
        }

        Ok(measurements)
    }
}

fn senml_value(object: &serde_json::Map<String, Value>) -> Result<ObservationValue, DecodeError> {
    if let Some(value) = object.get("v").and_then(Value::as_f64) {
        return Ok(ObservationValue::Number { value });
    }
    if let Some(value) = object.get("vs").and_then(Value::as_str) {
        return Ok(ObservationValue::Text {
            value: value.to_string(),
        });
    }
    if let Some(value) = object.get("vb").and_then(Value::as_bool) {
        return Ok(ObservationValue::Bool { value });
    }
    if let Some(value) = object.get("vd").and_then(Value::as_str) {
        return Ok(ObservationValue::Text {
            value: value.to_string(),
        });
    }
    Err(DecodeError::new(
        "SenML entry missing supported value field",
    ))
}

fn json_value_to_observation_value(value: &Value) -> Result<ObservationValue, DecodeError> {
    if let Some(object) = value.as_object() {
        match object.get("type").and_then(Value::as_str) {
            Some("number") => {
                return object
                    .get("value")
                    .and_then(Value::as_f64)
                    .map(|value| ObservationValue::Number { value })
                    .ok_or_else(|| DecodeError::new("number value must be numeric"))
            }
            Some("text") | Some("string") => {
                return object
                    .get("value")
                    .and_then(Value::as_str)
                    .map(|value| ObservationValue::Text {
                        value: value.to_string(),
                    })
                    .ok_or_else(|| DecodeError::new("text value must be a string"))
            }
            Some("bool") | Some("boolean") => {
                return object
                    .get("value")
                    .and_then(Value::as_bool)
                    .map(|value| ObservationValue::Bool { value })
                    .ok_or_else(|| DecodeError::new("bool value must be boolean"))
            }
            Some("json") => {
                return object
                    .get("value")
                    .cloned()
                    .map(|value| ObservationValue::Json { value })
                    .ok_or_else(|| DecodeError::new("json value must be present"))
            }
            _ => {}
        }
    }

    if let Some(value) = value.as_f64() {
        return Ok(ObservationValue::Number { value });
    }
    if let Some(value) = value.as_str() {
        return Ok(ObservationValue::Text {
            value: value.to_string(),
        });
    }
    if let Some(value) = value.as_bool() {
        return Ok(ObservationValue::Bool { value });
    }

    Ok(ObservationValue::Json {
        value: value.clone(),
    })
}

fn epoch_seconds_to_utc(value: f64) -> Result<DateTime<Utc>, DecodeError> {
    let seconds = value.trunc() as i64;
    let nanos = ((value.fract().abs()) * 1_000_000_000.0) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .ok_or_else(|| DecodeError::new("timestamp is out of range"))
}

fn senml_time(value: f64, base_time: Option<DateTime<Utc>>) -> Result<DateTime<Utc>, DecodeError> {
    if let Some(base_time) = base_time {
        let millis = (value * 1_000.0).round() as i64;
        return base_time
            .checked_add_signed(TimeDelta::milliseconds(millis))
            .ok_or_else(|| DecodeError::new("timestamp is out of range"));
    }

    epoch_seconds_to_utc(value)
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
            "senml-json".parse::<PayloadFormat>().unwrap(),
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
        assert_eq!(
            "canonical-json".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::CanonicalJson
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
