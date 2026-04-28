use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionModelError {
    EmptyCapabilityName,
    EmptyCommandType,
    EmptyAgentKey,
    EmptyAgentType,
    EmptyActionType,
    EmptyStatus,
    InvalidLease(String),
    InvalidTransition(String),
}

impl fmt::Display for ActionModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCapabilityName => f.write_str("capability_name must not be empty"),
            Self::EmptyCommandType => f.write_str("command_type must not be empty"),
            Self::EmptyAgentKey => f.write_str("agent_key must not be empty"),
            Self::EmptyAgentType => f.write_str("agent_type must not be empty"),
            Self::EmptyActionType => f.write_str("action_type must not be empty"),
            Self::EmptyStatus => f.write_str("status must not be empty"),
            Self::InvalidLease(message) => f.write_str(message),
            Self::InvalidTransition(message) => f.write_str(message),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    NotRequired,
    Required,
    Approved,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorAgentStatus {
    Online,
    Offline,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandLeaseStatus {
    Active,
    Expired,
    Released,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandLease {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub command_id: Uuid,
    pub executor_id: Uuid,
    pub lease_status: CommandLeaseStatus,
    pub claimed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub released_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub metadata: Option<Value>,
}

impl CommandLease {
    pub fn new(
        tenant_id: Uuid,
        command_id: Uuid,
        executor_id: Uuid,
        claimed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        metadata: Option<Value>,
    ) -> Result<Self, ActionModelError> {
        if expires_at <= claimed_at {
            return Err(ActionModelError::InvalidLease(
                "expires_at must be after claimed_at".to_string(),
            ));
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            command_id,
            executor_id,
            lease_status: CommandLeaseStatus::Active,
            claimed_at,
            expires_at,
            released_at: None,
            completed_at: None,
            metadata,
        })
    }

    pub fn is_active_at(&self, now: DateTime<Utc>) -> bool {
        self.lease_status == CommandLeaseStatus::Active && self.expires_at > now
    }

    pub fn refresh(
        &mut self,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), ActionModelError> {
        if self.lease_status != CommandLeaseStatus::Active {
            return Err(ActionModelError::InvalidLease(
                "only active leases can be refreshed".to_string(),
            ));
        }
        if expires_at <= now {
            return Err(ActionModelError::InvalidLease(
                "expires_at must be in the future".to_string(),
            ));
        }
        self.expires_at = expires_at;
        Ok(())
    }

    pub fn mark_expired(&mut self, now: DateTime<Utc>) {
        self.lease_status = CommandLeaseStatus::Expired;
        self.released_at = Some(now);
    }

    pub fn mark_released(&mut self, now: DateTime<Utc>) {
        self.lease_status = CommandLeaseStatus::Released;
        self.released_at = Some(now);
    }

    pub fn mark_completed(&mut self, now: DateTime<Utc>) {
        self.lease_status = CommandLeaseStatus::Completed;
        self.completed_at = Some(now);
    }

    pub fn mark_failed(&mut self, now: DateTime<Utc>) {
        self.lease_status = CommandLeaseStatus::Failed;
        self.completed_at = Some(now);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutorAgent {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub agent_key: String,
    pub agent_type: String,
    pub display_name: Option<String>,
    pub status: ExecutorAgentStatus,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ExecutorAgent {
    pub fn new(
        tenant_id: Uuid,
        agent_key: impl Into<String>,
        agent_type: impl Into<String>,
        display_name: Option<String>,
        status: ExecutorAgentStatus,
        metadata: Option<Value>,
        now: DateTime<Utc>,
    ) -> Result<Self, ActionModelError> {
        let agent_key = agent_key.into();
        let agent_type = agent_type.into();
        if agent_key.trim().is_empty() {
            return Err(ActionModelError::EmptyAgentKey);
        }
        if agent_type.trim().is_empty() {
            return Err(ActionModelError::EmptyAgentType);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            agent_key,
            agent_type,
            display_name,
            status,
            last_seen_at: None,
            metadata,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn heartbeat(&mut self, status: ExecutorAgentStatus, now: DateTime<Utc>) {
        self.status = status;
        self.last_seen_at = Some(now);
        self.updated_at = now;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutorCapability {
    pub agent_id: Uuid,
    pub command_type: String,
    pub protocol: Option<String>,
    pub metadata: Option<Value>,
}

impl ExecutorCapability {
    pub fn new(
        agent_id: Uuid,
        command_type: impl Into<String>,
        protocol: Option<String>,
        metadata: Option<Value>,
    ) -> Result<Self, ActionModelError> {
        let command_type = command_type.into();
        if command_type.trim().is_empty() {
            return Err(ActionModelError::EmptyCommandType);
        }

        Ok(Self {
            agent_id,
            command_type,
            protocol,
            metadata,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutorScope {
    pub agent_id: Uuid,
    pub target_entity_id: Option<Uuid>,
    pub entity_type: Option<String>,
    pub relationship_type: Option<String>,
    pub metadata: Option<Value>,
}

impl ExecutorScope {
    pub fn new(
        agent_id: Uuid,
        target_entity_id: Option<Uuid>,
        entity_type: Option<String>,
        relationship_type: Option<String>,
        metadata: Option<Value>,
    ) -> Self {
        Self {
            agent_id,
            target_entity_id,
            entity_type,
            relationship_type,
            metadata,
        }
    }
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
    pub claimed_by: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failure_reason: Option<String>,
    pub approval_status: Option<ApprovalStatus>,
    pub policy_decision: Option<Value>,
    pub retry_count: u32,
    pub max_retries: Option<u32>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_claimed_by: Option<String>,
    pub last_failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Command {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        target_entity_id: Uuid,
        command_type: impl Into<String>,
        payload: Value,
        requested_by: Option<String>,
        reason: Option<String>,
        approval_status: Option<ApprovalStatus>,
        policy_decision: Option<Value>,
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
            claimed_by: None,
            claimed_at: None,
            completed_at: None,
            failure_reason: None,
            approval_status,
            policy_decision,
            retry_count: 0,
            max_retries: None,
            lease_expires_at: None,
            last_claimed_by: None,
            last_failure_reason: None,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn claim(
        &mut self,
        claimed_by: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<(), ActionModelError> {
        if self.status != CommandStatus::Pending {
            return Err(ActionModelError::InvalidTransition(
                "command can only be claimed when status is pending".to_string(),
            ));
        }

        match self.approval_status {
            Some(ApprovalStatus::Required) => {
                return Err(ActionModelError::InvalidTransition(
                    "command requires approval before it can be claimed".to_string(),
                ));
            }
            Some(ApprovalStatus::Rejected) => {
                return Err(ActionModelError::InvalidTransition(
                    "command approval was rejected and cannot be claimed".to_string(),
                ));
            }
            _ => {}
        }

        let claimed_by = claimed_by.into();
        if claimed_by.trim().is_empty() {
            return Err(ActionModelError::InvalidTransition(
                "claimed_by must not be empty".to_string(),
            ));
        }

        self.status = CommandStatus::Claimed;
        self.claimed_by = Some(claimed_by);
        self.last_claimed_by = self.claimed_by.clone();
        self.claimed_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    pub fn release(&mut self, now: DateTime<Utc>) -> Result<(), ActionModelError> {
        if self.status != CommandStatus::Claimed {
            return Err(ActionModelError::InvalidTransition(
                "command can only be released when status is claimed".to_string(),
            ));
        }

        self.status = CommandStatus::Pending;
        self.claimed_by = None;
        self.claimed_at = None;
        self.lease_expires_at = None;
        self.updated_at = now;
        Ok(())
    }

    pub fn mark_executed(&mut self, now: DateTime<Utc>) -> Result<(), ActionModelError> {
        if self.status != CommandStatus::Claimed {
            return Err(ActionModelError::InvalidTransition(
                "command can only be marked executed when status is claimed".to_string(),
            ));
        }

        self.status = CommandStatus::Executed;
        self.completed_at = Some(now);
        self.failure_reason = None;
        self.lease_expires_at = None;
        self.updated_at = now;
        Ok(())
    }

    pub fn mark_failed(
        &mut self,
        failure_reason: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<(), ActionModelError> {
        if self.status != CommandStatus::Claimed {
            return Err(ActionModelError::InvalidTransition(
                "command can only be marked failed when status is claimed".to_string(),
            ));
        }

        let failure_reason = failure_reason.into();
        if failure_reason.trim().is_empty() {
            return Err(ActionModelError::InvalidTransition(
                "failure_reason must not be empty".to_string(),
            ));
        }

        self.status = CommandStatus::Failed;
        self.completed_at = Some(now);
        self.failure_reason = Some(failure_reason);
        self.last_failure_reason = self.failure_reason.clone();
        self.lease_expires_at = None;
        self.updated_at = now;
        Ok(())
    }

    pub fn cancel(&mut self, now: DateTime<Utc>) -> Result<(), ActionModelError> {
        if !matches!(self.status, CommandStatus::Pending | CommandStatus::Claimed) {
            return Err(ActionModelError::InvalidTransition(
                "command can only be cancelled when status is pending or claimed".to_string(),
            ));
        }

        self.status = CommandStatus::Cancelled;
        self.completed_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    pub fn approve(&mut self, now: DateTime<Utc>) -> Result<(), ActionModelError> {
        if self.approval_status == Some(ApprovalStatus::Rejected) {
            return Err(ActionModelError::InvalidTransition(
                "rejected command approval cannot be approved".to_string(),
            ));
        }
        if self.status != CommandStatus::Pending {
            return Err(ActionModelError::InvalidTransition(
                "command can only be approved when status is pending".to_string(),
            ));
        }

        self.approval_status = Some(ApprovalStatus::Approved);
        self.updated_at = now;
        Ok(())
    }

    pub fn reject(&mut self, now: DateTime<Utc>) -> Result<(), ActionModelError> {
        if self.status != CommandStatus::Pending {
            return Err(ActionModelError::InvalidTransition(
                "command can only be rejected when status is pending".to_string(),
            ));
        }

        self.approval_status = Some(ApprovalStatus::Rejected);
        self.updated_at = now;
        Ok(())
    }

    pub fn set_lease_expires_at(&mut self, expires_at: Option<DateTime<Utc>>, now: DateTime<Utc>) {
        self.lease_expires_at = expires_at;
        self.updated_at = now;
    }

    pub fn schedule_retry(&mut self, now: DateTime<Utc>) {
        self.status = CommandStatus::Pending;
        self.claimed_by = None;
        self.claimed_at = None;
        self.lease_expires_at = None;
        self.retry_count = self.retry_count.saturating_add(1);
        self.updated_at = now;
    }

    pub fn retry_limit_exceeded(&self) -> bool {
        self.max_retries
            .map(|max_retries| self.retry_count >= max_retries)
            .unwrap_or(false)
    }

    pub fn mark_failed_due_to_retry_limit(
        &mut self,
        reason: impl Into<String>,
        now: DateTime<Utc>,
    ) {
        self.status = CommandStatus::Failed;
        self.completed_at = Some(now);
        self.failure_reason = Some(reason.into());
        self.last_failure_reason = self.failure_reason.clone();
        self.claimed_by = None;
        self.claimed_at = None;
        self.lease_expires_at = None;
        self.updated_at = now;
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Policy {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub target_entity_id: Option<Uuid>,
    pub command_type: Option<String>,
    pub requires_approval: bool,
    pub auto_execute_allowed: bool,
    pub metadata: Option<Value>,
}

impl Policy {
    pub fn new(
        tenant_id: Uuid,
        target_entity_id: Option<Uuid>,
        command_type: Option<String>,
        requires_approval: bool,
        auto_execute_allowed: bool,
        metadata: Option<Value>,
    ) -> Result<Self, ActionModelError> {
        if command_type
            .as_ref()
            .map(|value| value.trim().is_empty())
            .unwrap_or(false)
        {
            return Err(ActionModelError::EmptyCommandType);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            target_entity_id,
            command_type,
            requires_approval,
            auto_execute_allowed,
            metadata,
        })
    }

    pub fn matches(&self, target_entity_id: Uuid, command_type: &str) -> bool {
        self.target_entity_id
            .map(|id| id == target_entity_id)
            .unwrap_or(true)
            && self
                .command_type
                .as_deref()
                .map(|policy_command_type| policy_command_type == command_type)
                .unwrap_or(true)
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
            Some(ApprovalStatus::NotRequired),
            None,
            now,
        )
        .unwrap();

        assert_eq!(command.command_type, "StartPump");
        assert_eq!(command.status, CommandStatus::Pending);
        assert_eq!(command.approval_status, Some(ApprovalStatus::NotRequired));
        assert_eq!(command.created_at, now);
        assert_eq!(command.updated_at, now);
    }

    #[test]
    fn transitions_pending_command_to_claimed() {
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let mut command = Command::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "StartPump",
            json!({}),
            None,
            None,
            Some(ApprovalStatus::Approved),
            None,
            now,
        )
        .unwrap();

        command.claim("executor-01", now).unwrap();

        assert_eq!(command.status, CommandStatus::Claimed);
        assert_eq!(command.claimed_by.as_deref(), Some("executor-01"));
        assert_eq!(command.claimed_at, Some(now));
    }

    #[test]
    fn blocks_claim_when_approval_is_required() {
        let now = Utc.with_ymd_and_hms(2026, 4, 27, 12, 0, 0).unwrap();
        let mut command = Command::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "StartPump",
            json!({}),
            None,
            None,
            Some(ApprovalStatus::Required),
            None,
            now,
        )
        .unwrap();

        let err = command.claim("executor-01", now).unwrap_err();

        assert_eq!(
            err,
            ActionModelError::InvalidTransition(
                "command requires approval before it can be claimed".to_string()
            )
        );
    }

    #[test]
    fn rejects_empty_capability_name() {
        let err = Capability::new(Uuid::new_v4(), " ", "StartPump", None, None).unwrap_err();
        assert_eq!(err, ActionModelError::EmptyCapabilityName);
    }

    #[test]
    fn creates_executor_agent() {
        let now = Utc.with_ymd_and_hms(2026, 4, 28, 12, 0, 0).unwrap();
        let agent = ExecutorAgent::new(
            Uuid::new_v4(),
            "edge-agent-01",
            "edge",
            Some("Edge Agent 01".to_string()),
            ExecutorAgentStatus::Online,
            Some(json!({"site": "building-01"})),
            now,
        )
        .unwrap();

        assert_eq!(agent.agent_key, "edge-agent-01");
        assert_eq!(agent.status, ExecutorAgentStatus::Online);
        assert_eq!(agent.created_at, now);
        assert!(agent.last_seen_at.is_none());
    }
}
