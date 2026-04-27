use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawMessageSource {
    Http,
    Mqtt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizationStatus {
    Pending,
    Normalized,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawMessageError {
    EmptyPayload,
}

impl fmt::Display for RawMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPayload => f.write_str("payload must not be empty"),
        }
    }
}

impl std::error::Error for RawMessageError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawMessage {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub source_type: RawMessageSource,
    pub source_ref: Option<String>,
    pub device_key: Option<String>,
    pub decoder_hint: Option<String>,
    pub content_type: Option<String>,
    pub headers: Value,
    pub payload: Vec<u8>,
    pub received_at: DateTime<Utc>,
    pub normalization_status: NormalizationStatus,
    pub normalization_error: Option<String>,
}

impl RawMessage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        source_type: RawMessageSource,
        source_ref: Option<String>,
        device_key: Option<String>,
        decoder_hint: Option<String>,
        content_type: Option<String>,
        headers: Value,
        payload: Vec<u8>,
        received_at: DateTime<Utc>,
    ) -> Result<Self, RawMessageError> {
        if payload.is_empty() {
            return Err(RawMessageError::EmptyPayload);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            source_type,
            source_ref,
            device_key,
            decoder_hint,
            content_type,
            headers,
            payload,
            received_at,
            normalization_status: NormalizationStatus::Pending,
            normalization_error: None,
        })
    }

    pub fn mark_normalized(&mut self) {
        self.normalization_status = NormalizationStatus::Normalized;
        self.normalization_error = None;
    }

    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.normalization_status = NormalizationStatus::Failed;
        self.normalization_error = Some(error.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[test]
    fn creates_pending_raw_message() {
        let received_at = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let raw = RawMessage::new(
            Uuid::new_v4(),
            RawMessageSource::Http,
            Some("/v1/ingest/http".to_string()),
            Some("device-01".to_string()),
            Some("senml_json".to_string()),
            Some("application/json".to_string()),
            json!({"x-aion-device": "device-01"}),
            br#"{"temperature":21.4}"#.to_vec(),
            received_at,
        )
        .expect("raw message should be valid");

        assert_eq!(raw.source_type, RawMessageSource::Http);
        assert_eq!(raw.normalization_status, NormalizationStatus::Pending);
        assert_eq!(raw.received_at, received_at);
    }

    #[test]
    fn can_mark_failed() {
        let mut raw = RawMessage::new(
            Uuid::new_v4(),
            RawMessageSource::Mqtt,
            Some("aion/demo/device-01/telemetry".to_string()),
            Some("device-01".to_string()),
            None,
            Some("text/plain".to_string()),
            json!({}),
            b"t|21.4".to_vec(),
            Utc::now(),
        )
        .expect("raw message should be valid");

        raw.mark_failed("unknown entity");

        assert_eq!(raw.normalization_status, NormalizationStatus::Failed);
        assert_eq!(raw.normalization_error.as_deref(), Some("unknown entity"));
    }
}
