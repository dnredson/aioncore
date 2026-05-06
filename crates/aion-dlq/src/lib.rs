use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DlqError {
    EmptyFailureReason,
}

impl fmt::Display for DlqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFailureReason => f.write_str("failure_reason must not be empty"),
        }
    }
}

impl std::error::Error for DlqError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DlqFailureStage {
    Ingestion,
    Decoding,
    Validation,
    Mapping,
    RuleEvaluation,
    FlowProcessing,
    SinkDelivery,
    CommandCreation,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DlqStatus {
    Pending,
    Inspecting,
    Resolved,
    Ignored,
    ReplayRequested,
    FailedReplay,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DlqRecord {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub dlq_key: Option<String>,
    pub source_system: Option<String>,
    pub source_id: Option<String>,
    pub connector_id: Option<Uuid>,
    pub flow_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub event_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
    pub idempotency_key: Option<String>,
    pub external_flow_id: Option<String>,
    pub external_flow_name: Option<String>,
    pub external_flowfile_uuid: Option<String>,
    pub external_process_group_id: Option<String>,
    pub external_processor_id: Option<String>,
    pub external_provenance_uri: Option<String>,
    pub sync_session_id: Option<String>,
    pub payload_format: Option<String>,
    pub payload: Option<Value>,
    pub payload_hash: Option<String>,
    pub failure_stage: DlqFailureStage,
    pub failure_reason: String,
    pub failure_detail: Option<String>,
    pub retry_count: u32,
    pub replay_count: u32,
    pub status: DlqStatus,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

impl DlqRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        dlq_key: Option<String>,
        source_system: Option<String>,
        source_id: Option<String>,
        connector_id: Option<Uuid>,
        flow_id: Option<Uuid>,
        raw_message_id: Option<Uuid>,
        event_id: Option<Uuid>,
        command_id: Option<Uuid>,
        idempotency_key: Option<String>,
        external_flow_id: Option<String>,
        external_flow_name: Option<String>,
        external_flowfile_uuid: Option<String>,
        external_process_group_id: Option<String>,
        external_processor_id: Option<String>,
        external_provenance_uri: Option<String>,
        sync_session_id: Option<String>,
        payload_format: Option<String>,
        payload: Option<Value>,
        payload_hash: Option<String>,
        failure_stage: DlqFailureStage,
        failure_reason: impl Into<String>,
        failure_detail: Option<String>,
        retry_count: u32,
        replay_count: u32,
        status: DlqStatus,
        metadata: Option<Value>,
        now: DateTime<Utc>,
    ) -> Result<Self, DlqError> {
        let failure_reason = failure_reason.into();
        if failure_reason.trim().is_empty() {
            return Err(DlqError::EmptyFailureReason);
        }

        let resolved_at = if status_marks_resolution(&status) {
            Some(now)
        } else {
            None
        };

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            dlq_key: trim_optional(dlq_key),
            source_system: trim_optional(source_system),
            source_id: trim_optional(source_id),
            connector_id,
            flow_id,
            raw_message_id,
            event_id,
            command_id,
            idempotency_key: trim_optional(idempotency_key),
            external_flow_id: trim_optional(external_flow_id),
            external_flow_name: trim_optional(external_flow_name),
            external_flowfile_uuid: trim_optional(external_flowfile_uuid),
            external_process_group_id: trim_optional(external_process_group_id),
            external_processor_id: trim_optional(external_processor_id),
            external_provenance_uri: trim_optional(external_provenance_uri),
            sync_session_id: trim_optional(sync_session_id),
            payload_format: trim_optional(payload_format),
            payload,
            payload_hash: trim_optional(payload_hash),
            failure_stage,
            failure_reason,
            failure_detail: trim_optional(failure_detail),
            retry_count,
            replay_count,
            status,
            metadata: metadata.unwrap_or_else(|| json!({})),
            created_at: now,
            updated_at: now,
            resolved_at,
        })
    }

    pub fn set_status(&mut self, status: DlqStatus, now: DateTime<Utc>) {
        self.status = status;
        self.updated_at = now;
        self.resolved_at = if status_marks_resolution(&self.status) {
            Some(now)
        } else {
            None
        };
    }
}

fn trim_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn status_marks_resolution(status: &DlqStatus) -> bool {
    matches!(status, DlqStatus::Resolved | DlqStatus::Ignored)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_dlq_record() {
        let now = Utc::now();
        let record = DlqRecord::new(
            Uuid::new_v4(),
            Some("decode-failure-01".to_string()),
            Some("nifi".to_string()),
            Some("edge-site-01".to_string()),
            None,
            None,
            None,
            None,
            None,
            Some("tenant-a:key-1".to_string()),
            Some("flow-01".to_string()),
            Some("Ingest Flow".to_string()),
            Some("flowfile-01".to_string()),
            None,
            None,
            Some("nifi://provenance/123".to_string()),
            Some("sync-01".to_string()),
            Some("senml-json".to_string()),
            Some(json!([{"n": "temperature", "v": 21.4}])),
            Some("sha256:abc".to_string()),
            DlqFailureStage::Decoding,
            "decoder rejected payload",
            Some("invalid field".to_string()),
            2,
            1,
            DlqStatus::Pending,
            None,
            now,
        )
        .unwrap();

        assert_eq!(record.failure_reason, "decoder rejected payload");
        assert_eq!(record.metadata, json!({}));
        assert_eq!(record.resolved_at, None);
    }

    #[test]
    fn resolved_status_sets_resolved_at() {
        let now = Utc::now();
        let mut record = DlqRecord::new(
            Uuid::new_v4(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            DlqFailureStage::Unknown,
            "failed",
            None,
            0,
            0,
            DlqStatus::Pending,
            None,
            now,
        )
        .unwrap();

        let resolved_at = now + chrono::Duration::seconds(10);
        record.set_status(DlqStatus::Resolved, resolved_at);
        assert_eq!(record.resolved_at, Some(resolved_at));
    }
}
