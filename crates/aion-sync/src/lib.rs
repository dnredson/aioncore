use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncSessionError {
    EmptySyncSessionId,
}

impl fmt::Display for SyncSessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySyncSessionId => f.write_str("sync_session_id must not be empty"),
        }
    }
}

impl std::error::Error for SyncSessionError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncSessionStatus {
    Open,
    Receiving,
    Completed,
    Failed,
    Abandoned,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncSession {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub sync_session_id: String,
    pub source_system: Option<String>,
    pub source_id: Option<String>,
    pub connector_id: Option<Uuid>,
    pub edge_adapter_id: Option<Uuid>,
    pub status: SyncSessionStatus,
    pub connectivity_state: Option<String>,
    pub started_at: DateTime<Utc>,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_batch_id: Option<String>,
    pub expected_items: Option<u64>,
    pub received_items: u64,
    pub accepted_count: u64,
    pub duplicate_count: u64,
    pub failed_count: u64,
    pub observations_created: u64,
    pub first_observed_at: Option<DateTime<Utc>>,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SyncSession {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        sync_session_id: impl Into<String>,
        source_system: Option<String>,
        source_id: Option<String>,
        connector_id: Option<Uuid>,
        edge_adapter_id: Option<Uuid>,
        status: Option<SyncSessionStatus>,
        connectivity_state: Option<String>,
        expected_items: Option<u64>,
        metadata: Option<Value>,
        now: DateTime<Utc>,
    ) -> Result<Self, SyncSessionError> {
        let sync_session_id = sync_session_id.into();
        if sync_session_id.trim().is_empty() {
            return Err(SyncSessionError::EmptySyncSessionId);
        }
        let status = status.unwrap_or(SyncSessionStatus::Open);
        let completed_at = if status_marks_completion(&status) {
            Some(now)
        } else {
            None
        };

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            sync_session_id: sync_session_id.trim().to_string(),
            source_system: trim_optional(source_system),
            source_id: trim_optional(source_id),
            connector_id,
            edge_adapter_id,
            status,
            connectivity_state: trim_optional(connectivity_state),
            started_at: now,
            last_seen_at: None,
            completed_at,
            last_batch_id: None,
            expected_items,
            received_items: 0,
            accepted_count: 0,
            duplicate_count: 0,
            failed_count: 0,
            observations_created: 0,
            first_observed_at: None,
            last_observed_at: None,
            metadata: metadata.unwrap_or_else(|| json!({})),
            created_at: now,
            updated_at: now,
        })
    }

    pub fn set_status(&mut self, status: SyncSessionStatus, now: DateTime<Utc>) {
        self.status = status;
        self.updated_at = now;
        self.last_seen_at = Some(now);
        self.completed_at = if status_marks_completion(&self.status) {
            Some(now)
        } else {
            None
        };
    }

    #[allow(clippy::too_many_arguments)]
    pub fn apply_batch_result(
        &mut self,
        batch_id: Option<String>,
        received_items: u64,
        accepted_count: u64,
        duplicate_count: u64,
        failed_count: u64,
        observations_created: u64,
        connectivity_state: Option<String>,
        now: DateTime<Utc>,
    ) {
        self.last_batch_id = trim_optional(batch_id).or_else(|| self.last_batch_id.clone());
        self.received_items = self.received_items.saturating_add(received_items);
        self.accepted_count = self.accepted_count.saturating_add(accepted_count);
        self.duplicate_count = self.duplicate_count.saturating_add(duplicate_count);
        self.failed_count = self.failed_count.saturating_add(failed_count);
        self.observations_created = self
            .observations_created
            .saturating_add(observations_created);
        self.connectivity_state =
            trim_optional(connectivity_state).or_else(|| self.connectivity_state.clone());
        self.last_seen_at = Some(now);
        self.updated_at = now;
        if matches!(self.status, SyncSessionStatus::Open) {
            self.status = SyncSessionStatus::Receiving;
        }
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

fn status_marks_completion(status: &SyncSessionStatus) -> bool {
    matches!(
        status,
        SyncSessionStatus::Completed | SyncSessionStatus::Failed | SyncSessionStatus::Abandoned
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_and_updates_sync_session_counts() {
        let now = Utc::now();
        let mut session = SyncSession::new(
            Uuid::new_v4(),
            "sync-01",
            Some("minifi".to_string()),
            Some("edge-01".to_string()),
            None,
            None,
            None,
            Some("reconnected_backfill".to_string()),
            Some(10),
            None,
            now,
        )
        .unwrap();

        assert_eq!(session.status, SyncSessionStatus::Open);
        session.apply_batch_result(Some("batch-01".to_string()), 3, 2, 1, 0, 2, None, now);
        assert_eq!(session.status, SyncSessionStatus::Receiving);
        assert_eq!(session.received_items, 3);
        assert_eq!(session.duplicate_count, 1);
    }
}
