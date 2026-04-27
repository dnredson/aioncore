use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionModelError {
    EmptyCapabilityName,
    EmptyCommandType,
    EmptyActionType,
    EmptyStatus,
}

impl fmt::Display for ActionModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCapabilityName => f.write_str("capability_name must not be empty"),
            Self::EmptyCommandType => f.write_str("command_type must not be empty"),
            Self::EmptyActionType => f.write_str("action_type must not be empty"),
            Self::EmptyStatus => f.write_str("status must not be empty"),
        }
    }
}

impl std::error::Error for ActionModelError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Pending,
    Claimed,
    Executed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capability {
    pub entity_id: Uuid,
    pub capability_name: String,
    pub command_type: String,
    pub protocol: Option<String>,
    pub metadata: Option<Value>,
}

impl Capability {
    pub fn new(
        entity_id: Uuid,
        capability_name: impl Into<String>,
        command_type: impl Into<String>,
        protocol: Option<String>,
        metadata: Option<Value>,
    ) -> Result<Self, ActionModelError> {
        let capability_name = capability_name.into();
        let command_type = command_type.into();

        if capability_name.trim().is_empty() {
            return Err(ActionModelError::EmptyCapabilityName);
        }
        if command_type.trim().is_empty() {
            return Err(ActionModelError::EmptyCommandType);
        }

        Ok(Self {
            entity_id,
            capability_name,
            command_type,
            protocol,
            metadata,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub target_entity_id: Uuid,
    pub command_type: String,
    pub payload: Value,
    pub status: CommandStatus,
    pub requested_by: Option<String>,
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Command {
    pub fn new(
        tenant_id: Uuid,
        target_entity_id: Uuid,
        command_type: impl Into<String>,
        payload: Value,
        requested_by: Option<String>,
        reason: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<Self, ActionModelError> {
        let command_type = command_type.into();
        if command_type.trim().is_empty() {
            return Err(ActionModelError::EmptyCommandType);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            target_entity_id,
            command_type,
            payload,
            status: CommandStatus::Pending,
            requested_by,
            reason,
            created_at: now,
            updated_at: now,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Action {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub command_id: Uuid,
    pub executor_entity_id: Option<Uuid>,
    pub action_type: String,
    pub status: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub metadata: Option<Value>,
}

impl Action {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        command_id: Uuid,
        executor_entity_id: Option<Uuid>,
        action_type: impl Into<String>,
        status: impl Into<String>,
        started_at: Option<DateTime<Utc>>,
        finished_at: Option<DateTime<Utc>>,
        metadata: Option<Value>,
    ) -> Result<Self, ActionModelError> {
        let action_type = action_type.into();
        let status = status.into();

        if action_type.trim().is_empty() {
            return Err(ActionModelError::EmptyActionType);
        }
        if status.trim().is_empty() {
            return Err(ActionModelError::EmptyStatus);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            command_id,
            executor_entity_id,
            action_type,
            status,
            started_at,
            finished_at,
            metadata,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionResult {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub command_id: Uuid,
    pub action_id: Uuid,
    pub status: String,
    pub verified: bool,
    pub result_payload: Value,
    pub observed_at: DateTime<Utc>,
    pub metadata: Option<Value>,
}

impl ActionResult {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        command_id: Uuid,
        action_id: Uuid,
        status: impl Into<String>,
        verified: bool,
        result_payload: Value,
        observed_at: DateTime<Utc>,
        metadata: Option<Value>,
    ) -> Result<Self, ActionModelError> {
        let status = status.into();
        if status.trim().is_empty() {
            return Err(ActionModelError::EmptyStatus);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            command_id,
            action_id,
            status,
            verified,
            result_payload,
            observed_at,
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
    fn creates_pending_command() {
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let command = Command::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "StartPump",
            json!({"mode": "auto"}),
            Some("operator".to_string()),
            Some("low pressure".to_string()),
            now,
        )
        .unwrap();

        assert_eq!(command.command_type, "StartPump");
        assert_eq!(command.status, CommandStatus::Pending);
        assert_eq!(command.created_at, now);
        assert_eq!(command.updated_at, now);
    }

    #[test]
    fn rejects_empty_capability_name() {
        let err = Capability::new(Uuid::new_v4(), " ", "StartPump", None, None).unwrap_err();
        assert_eq!(err, ActionModelError::EmptyCapabilityName);
    }
}
