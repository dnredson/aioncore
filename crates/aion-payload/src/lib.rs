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
    TtnUplinkJson,
    SmartSentinelSnapshotJson,
    Unknown(String),
}

impl fmt::Display for PayloadFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SenMlJson => f.write_str("senml_json"),
            Self::UltraLight => f.write_str("ultralight"),
            Self::JsonMapping => f.write_str("json_mapping"),
            Self::CanonicalJson => f.write_str("canonical_json"),
            Self::TtnUplinkJson => f.write_str("ttn_uplink_json"),
            Self::SmartSentinelSnapshotJson => f.write_str("smartsentinel_snapshot_json"),
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
            "ttn_uplink_json" | "application/vnd.thethings.uplink+json" => Self::TtnUplinkJson,
            "smartsentinel_snapshot_json" => Self::SmartSentinelSnapshotJson,
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

#[derive(Debug, Clone, Default)]
pub struct TtnUplinkJsonDecoder;

impl PayloadDecoder for TtnUplinkJsonDecoder {
    fn name(&self) -> &'static str {
        "ttn-uplink-json"
    }

    fn decode(&self, input: DecodeInput) -> Result<Vec<DecodedMeasurement>, DecodeError> {
        let value: Value = serde_json::from_slice(&input.payload)
            .map_err(|err| DecodeError::new(format!("invalid TTN uplink JSON payload: {err}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| DecodeError::new("TTN uplink JSON payload must be an object"))?;
        let uplink = object
            .get("uplink_message")
            .and_then(Value::as_object)
            .ok_or_else(|| DecodeError::new("TTN uplink JSON missing uplink_message object"))?;
        let decoded_payload = uplink
            .get("decoded_payload")
            .and_then(Value::as_object)
            .ok_or_else(|| DecodeError::new("TTN uplink JSON missing decoded_payload object"))?;

        let end_device_ids = object.get("end_device_ids").and_then(Value::as_object);
        let device_id = end_device_ids
            .and_then(|value| value.get("device_id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let application_id = end_device_ids
            .and_then(|value| value.get("application_ids"))
            .and_then(Value::as_object)
            .and_then(|value| value.get("application_id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let observed_at = uplink
            .get("received_at")
            .and_then(Value::as_str)
            .or_else(|| object.get("received_at").and_then(Value::as_str))
            .map(parse_rfc3339_utc)
            .transpose()?
            .unwrap_or(input.received_at);
        let decoded_payload_keys = decoded_payload.keys().cloned().collect::<Vec<_>>();
        let skipped_decoded_payload_keys = decoded_payload
            .iter()
            .filter(|(_, value)| !(value.is_number() || value.is_string() || value.is_boolean()))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();

        let mut measurements = Vec::new();
        for (key, value) in decoded_payload {
            let observation_value = if let Some(value) = value.as_f64() {
                ObservationValue::Number { value }
            } else if let Some(value) = value.as_str() {
                ObservationValue::Text {
                    value: value.to_string(),
                }
            } else if let Some(value) = value.as_bool() {
                ObservationValue::Bool { value }
            } else {
                continue;
            };
            let unit = ttn_unit_for_key(input.config.as_ref(), key);

            measurements.push(DecodedMeasurement {
                entity_key: input
                    .device_key
                    .clone()
                    .or_else(|| device_id.clone())
                    .unwrap_or_default(),
                observed_property: format!("ttn:{key}"),
                time: observed_at,
                value: observation_value,
                unit,
                metadata: serde_json::json!({
                    "decoder": self.name(),
                    "ttn_device_id": device_id,
                    "ttn_application_id": application_id,
                    "ttn_f_port": uplink.get("f_port").cloned(),
                    "ttn_f_cnt": uplink.get("f_cnt").cloned(),
                    "ttn_frm_payload": uplink.get("frm_payload").cloned(),
                    "ttn_rx_metadata": uplink.get("rx_metadata").cloned(),
                    "ttn_settings": uplink.get("settings").cloned(),
                    "decoded_payload_key": key,
                    "decoded_payload_keys": decoded_payload_keys,
                    "skipped_decoded_payload_keys": skipped_decoded_payload_keys
                }),
            });
        }

        if measurements.is_empty() {
            return Err(DecodeError::new(
                "TTN uplink JSON decoded_payload produced no primitive measurements",
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

fn parse_rfc3339_utc(value: &str) -> Result<DateTime<Utc>, DecodeError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|err| DecodeError::new(format!("invalid TTN received_at timestamp: {err}")))
}

fn ttn_unit_for_key(config: Option<&Value>, key: &str) -> Option<String> {
    let config = config?;
    config
        .get("unit_mapping")
        .or_else(|| config.get("units"))
        .and_then(|value| value.get(key))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
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
        assert_eq!(
            "ttn-uplink-json".parse::<PayloadFormat>().unwrap(),
            PayloadFormat::TtnUplinkJson
        );
        assert_eq!(
            "smartsentinel-snapshot-json"
                .parse::<PayloadFormat>()
                .unwrap(),
            PayloadFormat::SmartSentinelSnapshotJson
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

    fn ttn_sample(decoded_payload: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "end_device_ids": {
                "device_id": "soil-node-01",
                "application_ids": {
                    "application_id": "farm-app"
                }
            },
            "received_at": "2026-04-29T12:00:00Z",
            "uplink_message": {
                "received_at": "2026-04-29T12:01:02Z",
                "f_port": 1,
                "f_cnt": 42,
                "frm_payload": "AQID",
                "decoded_payload": decoded_payload,
                "rx_metadata": [{"gateway_ids": {"gateway_id": "gw-1"}, "rssi": -71}],
                "settings": {"data_rate": {"lora": {"spreading_factor": 7}}}
            }
        }))
        .unwrap()
    }

    #[test]
    fn ttn_decoder_creates_numeric_observations() {
        let decoder = TtnUplinkJsonDecoder;
        let input = DecodeInput {
            tenant_id: Uuid::new_v4(),
            device_key: Some("producer".to_string()),
            format: PayloadFormat::TtnUplinkJson,
            content_type: Some("application/json".to_string()),
            payload: ttn_sample(json!({"temperature": 21.5, "soil_moisture": 44})),
            received_at: Utc.with_ymd_and_hms(2026, 4, 29, 11, 0, 0).unwrap(),
            config: Some(json!({"unit_mapping": {"temperature": "Cel", "soil_moisture": "%"}})),
        };

        let decoded = decoder.decode(input).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].observed_property, "ttn:soil_moisture");
        assert_eq!(decoded[0].unit.as_deref(), Some("%"));
        assert_eq!(decoded[1].observed_property, "ttn:temperature");
        assert_eq!(decoded[1].unit.as_deref(), Some("Cel"));
    }

    #[test]
    fn ttn_decoder_creates_string_and_bool_observations() {
        let decoder = TtnUplinkJsonDecoder;
        let input = DecodeInput {
            tenant_id: Uuid::new_v4(),
            device_key: None,
            format: PayloadFormat::TtnUplinkJson,
            content_type: None,
            payload: ttn_sample(json!({"state": "ok", "battery_low": false})),
            received_at: Utc.with_ymd_and_hms(2026, 4, 29, 11, 0, 0).unwrap(),
            config: None,
        };

        let decoded = decoder.decode(input).unwrap();

        assert_eq!(decoded.len(), 2);
        assert!(decoded.iter().any(|measurement| {
            measurement.observed_property == "ttn:battery_low"
                && measurement.value == ObservationValue::Bool { value: false }
        }));
        assert!(decoded.iter().any(|measurement| {
            measurement.observed_property == "ttn:state"
                && measurement.value
                    == ObservationValue::Text {
                        value: "ok".to_string(),
                    }
        }));
    }

    #[test]
    fn ttn_decoder_errors_when_decoded_payload_is_missing() {
        let decoder = TtnUplinkJsonDecoder;
        let payload = serde_json::to_vec(&json!({"uplink_message": {}})).unwrap();
        let input = DecodeInput {
            tenant_id: Uuid::new_v4(),
            device_key: None,
            format: PayloadFormat::TtnUplinkJson,
            content_type: None,
            payload,
            received_at: Utc.with_ymd_and_hms(2026, 4, 29, 11, 0, 0).unwrap(),
            config: None,
        };

        let error = decoder.decode(input).unwrap_err();

        assert!(error.message().contains("decoded_payload"));
    }

    #[test]
    fn ttn_decoder_skips_nested_values_and_preserves_keys() {
        let decoder = TtnUplinkJsonDecoder;
        let input = DecodeInput {
            tenant_id: Uuid::new_v4(),
            device_key: None,
            format: PayloadFormat::TtnUplinkJson,
            content_type: None,
            payload: ttn_sample(json!({
                "temperature": 21.5,
                "location": {"lat": 1},
                "samples": [1, 2]
            })),
            received_at: Utc.with_ymd_and_hms(2026, 4, 29, 11, 0, 0).unwrap(),
            config: None,
        };

        let decoded = decoder.decode(input).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0].metadata["skipped_decoded_payload_keys"],
            json!(["location", "samples"])
        );
    }

    #[test]
    fn ttn_decoder_prefers_uplink_received_at_and_preserves_device_metadata() {
        let decoder = TtnUplinkJsonDecoder;
        let input = DecodeInput {
            tenant_id: Uuid::new_v4(),
            device_key: None,
            format: PayloadFormat::TtnUplinkJson,
            content_type: None,
            payload: ttn_sample(json!({"temperature": 21.5})),
            received_at: Utc.with_ymd_and_hms(2026, 4, 29, 11, 0, 0).unwrap(),
            config: None,
        };

        let decoded = decoder.decode(input).unwrap();

        assert_eq!(
            decoded[0].time,
            Utc.with_ymd_and_hms(2026, 4, 29, 12, 1, 2).unwrap()
        );
        assert_eq!(decoded[0].metadata["ttn_device_id"], "soil-node-01");
        assert_eq!(decoded[0].metadata["ttn_application_id"], "farm-app");
        assert_eq!(
            decoded[0].metadata["decoded_payload_keys"],
            json!(["temperature"])
        );
    }
}
