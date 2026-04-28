use aion_event::EventSeverity;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{cmp::Ordering, fmt};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleError {
    EmptyName,
    EmptyEventType,
    EmptyCommandType,
    InvalidConditionValue(String),
}

impl fmt::Display for RuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName => f.write_str("rule name must not be empty"),
            Self::EmptyEventType => f.write_str("event_type must not be empty"),
            Self::EmptyCommandType => f.write_str("command_type must not be empty"),
            Self::InvalidConditionValue(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RuleError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleTriggerType {
    ObservationCreated,
    EventCreated,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuleComparison {
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleCondition {
    pub comparison: RuleComparison,
    pub value: Value,
}

impl RuleCondition {
    pub fn matches(&self, actual: &Value) -> Result<bool, RuleError> {
        match self.comparison {
            RuleComparison::Equals => Ok(actual == &self.value),
            RuleComparison::NotEquals => Ok(actual != &self.value),
            RuleComparison::GreaterThan => {
                compare_values(actual, &self.value).map(|ordering| ordering == Ordering::Greater)
            }
            RuleComparison::GreaterThanOrEqual => compare_values(actual, &self.value)
                .map(|ordering| matches!(ordering, Ordering::Greater | Ordering::Equal)),
            RuleComparison::LessThan => {
                compare_values(actual, &self.value).map(|ordering| ordering == Ordering::Less)
            }
            RuleComparison::LessThanOrEqual => compare_values(actual, &self.value)
                .map(|ordering| matches!(ordering, Ordering::Less | Ordering::Equal)),
        }
    }
}

fn compare_values(left: &Value, right: &Value) -> Result<Ordering, RuleError> {
    if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
        return left.partial_cmp(&right).ok_or_else(|| {
            RuleError::InvalidConditionValue(
                "numeric condition value is not comparable".to_string(),
            )
        });
    }

    if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
        return Ok(left.cmp(right));
    }

    Err(RuleError::InvalidConditionValue(
        "comparison requires matching numeric or string values".to_string(),
    ))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuleAction {
    CreateEvent {
        event_type: String,
        severity: EventSeverity,
        source_entity_id: Option<Uuid>,
        target_entity_id: Option<Uuid>,
        message: Option<String>,
        metadata: Option<Value>,
    },
    CreateCommand {
        target_entity_id: Uuid,
        command_type: String,
        payload: Value,
        requested_by: Option<String>,
        reason: Option<String>,
        metadata: Option<Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rule {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub trigger_type: RuleTriggerType,
    pub target_entity_id: Option<Uuid>,
    pub observed_property: Option<String>,
    pub event_type: Option<String>,
    pub condition: RuleCondition,
    pub action: RuleAction,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Rule {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        name: impl Into<String>,
        description: Option<String>,
        enabled: bool,
        trigger_type: RuleTriggerType,
        target_entity_id: Option<Uuid>,
        observed_property: Option<String>,
        event_type: Option<String>,
        condition: RuleCondition,
        action: RuleAction,
        metadata: Option<Value>,
        now: DateTime<Utc>,
    ) -> Result<Self, RuleError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(RuleError::EmptyName);
        }
        validate_action(&action)?;

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            name,
            description,
            enabled,
            trigger_type,
            target_entity_id,
            observed_property,
            event_type,
            condition,
            action,
            metadata,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn set_enabled(&mut self, enabled: bool, now: DateTime<Utc>) {
        self.enabled = enabled;
        self.updated_at = now;
    }
}

fn validate_action(action: &RuleAction) -> Result<(), RuleError> {
    match action {
        RuleAction::CreateEvent { event_type, .. } if event_type.trim().is_empty() => {
            Err(RuleError::EmptyEventType)
        }
        RuleAction::CreateCommand { command_type, .. } if command_type.trim().is_empty() => {
            Err(RuleError::EmptyCommandType)
        }
        _ => Ok(()),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleEvaluationResult {
    pub rule_id: Uuid,
    pub matched: bool,
    pub generated_command_ids: Vec<Uuid>,
    pub generated_event_ids: Vec<Uuid>,
    pub reason: Option<String>,
}

impl RuleEvaluationResult {
    pub fn skipped(rule_id: Uuid, reason: impl Into<String>) -> Self {
        Self {
            rule_id,
            matched: false,
            generated_command_ids: Vec::new(),
            generated_event_ids: Vec::new(),
            reason: Some(reason.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numeric_less_than_matches() {
        let condition = RuleCondition {
            comparison: RuleComparison::LessThan,
            value: json!(20.0),
        };

        assert!(condition.matches(&json!(12.0)).unwrap());
        assert!(!condition.matches(&json!(22.0)).unwrap());
    }

    #[test]
    fn creates_rule() {
        let now = Utc::now();
        let rule = Rule::new(
            Uuid::new_v4(),
            "Low level",
            None,
            true,
            RuleTriggerType::ObservationCreated,
            Some(Uuid::new_v4()),
            Some("WaterTankLevel".to_string()),
            None,
            RuleCondition {
                comparison: RuleComparison::LessThan,
                value: json!(20),
            },
            RuleAction::CreateCommand {
                target_entity_id: Uuid::new_v4(),
                command_type: "StartPump".to_string(),
                payload: json!({}),
                requested_by: Some("rule-engine".to_string()),
                reason: None,
                metadata: None,
            },
            None,
            now,
        )
        .unwrap();

        assert_eq!(rule.name, "Low level");
        assert!(rule.enabled);
    }
}
