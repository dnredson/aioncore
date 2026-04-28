use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventError {
    EmptyEventType,
}

impl fmt::Display for EventError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyEventType => f.write_str("event_type must not be empty"),
        }
    }
}

impl std::error::Error for EventError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSeverity {
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub event_type: String,
    pub severity: EventSeverity,
    pub source_entity_id: Option<Uuid>,
    pub target_entity_id: Option<Uuid>,
    pub message: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub observed_at: Option<DateTime<Utc>>,
    pub correlation_id: Option<String>,
    pub raw_message_id: Option<Uuid>,
    pub observation_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
    pub action_id: Option<Uuid>,
    pub action_result_id: Option<Uuid>,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
}

impl Event {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        event_type: impl Into<String>,
        severity: EventSeverity,
        source_entity_id: Option<Uuid>,
        target_entity_id: Option<Uuid>,
        message: Option<String>,
        occurred_at: DateTime<Utc>,
        observed_at: Option<DateTime<Utc>>,
        correlation_id: Option<String>,
        raw_message_id: Option<Uuid>,
        observation_id: Option<Uuid>,
        command_id: Option<Uuid>,
        action_id: Option<Uuid>,
        action_result_id: Option<Uuid>,
        metadata: Option<Value>,
        created_at: DateTime<Utc>,
    ) -> Result<Self, EventError> {
        let event_type = event_type.into();
        if event_type.trim().is_empty() {
            return Err(EventError::EmptyEventType);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            event_type,
            severity,
            source_entity_id,
            target_entity_id,
            message,
            occurred_at,
            observed_at,
            correlation_id,
            raw_message_id,
            observation_id,
            command_id,
            action_id,
            action_result_id,
            metadata,
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
    fn creates_event() {
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let command_id = Uuid::new_v4();
        let event = Event::new(
            Uuid::new_v4(),
            "aion:CommandCreated",
            EventSeverity::Info,
            None,
            Some(Uuid::new_v4()),
            Some("Command created".to_string()),
            now,
            None,
            Some("corr-001".to_string()),
            None,
            None,
            Some(command_id),
            None,
            None,
            Some(json!({"source": "test"})),
            now,
        )
        .unwrap();

        assert_eq!(event.event_type, "aion:CommandCreated");
        assert_eq!(event.severity, EventSeverity::Info);
        assert_eq!(event.command_id, Some(command_id));
    }

    #[test]
    fn rejects_empty_event_type() {
        let err = Event::new(
            Uuid::new_v4(),
            " ",
            EventSeverity::Info,
            None,
            None,
            None,
            Utc::now(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Utc::now(),
        )
        .unwrap_err();

        assert_eq!(err, EventError::EmptyEventType);
    }
}
