use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowError {
    EmptyFlowKey,
    EmptyName,
    EmptyNodeId,
    DuplicateNodeId(String),
    EmptyEdgeId,
    InvalidNodeType(String),
    UnknownEdgeNode { edge_id: String, node_id: String },
}

impl fmt::Display for FlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyFlowKey => f.write_str("flow_key must not be empty"),
            Self::EmptyName => f.write_str("name must not be empty"),
            Self::EmptyNodeId => f.write_str("node_id must not be empty"),
            Self::DuplicateNodeId(node_id) => {
                write!(f, "duplicate node_id '{node_id}' in flow definition")
            }
            Self::EmptyEdgeId => f.write_str("edge_id must not be empty"),
            Self::InvalidNodeType(value) => write!(f, "invalid node_type '{value}'"),
            Self::UnknownEdgeNode { edge_id, node_id } => {
                write!(f, "edge '{edge_id}' references unknown node_id '{node_id}'")
            }
        }
    }
}

impl std::error::Error for FlowError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Flow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub flow_key: String,
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub nodes: Vec<FlowNode>,
    pub edges: Vec<FlowEdge>,
    pub metadata: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Flow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        flow_key: impl Into<String>,
        name: impl Into<String>,
        description: Option<String>,
        enabled: bool,
        nodes: Vec<FlowNode>,
        edges: Vec<FlowEdge>,
        metadata: Option<Value>,
        now: DateTime<Utc>,
    ) -> Result<Self, FlowError> {
        let flow_key = flow_key.into();
        if flow_key.trim().is_empty() {
            return Err(FlowError::EmptyFlowKey);
        }

        let name = name.into();
        if name.trim().is_empty() {
            return Err(FlowError::EmptyName);
        }

        validate_nodes_and_edges(&nodes, &edges)?;

        Ok(Self {
            id: Uuid::new_v4(),
            tenant_id,
            flow_key,
            name,
            description,
            enabled,
            nodes,
            edges,
            metadata,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn set_enabled(&mut self, enabled: bool, now: DateTime<Utc>) {
        self.enabled = enabled;
        self.updated_at = now;
    }

    pub fn validate(&self) -> Result<(), FlowError> {
        if self.flow_key.trim().is_empty() {
            return Err(FlowError::EmptyFlowKey);
        }
        if self.name.trim().is_empty() {
            return Err(FlowError::EmptyName);
        }
        validate_nodes_and_edges(&self.nodes, &self.edges)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowNode {
    pub node_id: String,
    pub node_type: FlowNodeType,
    pub name: Option<String>,
    pub config: Value,
    pub position: Option<FlowNodePosition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowNodePosition {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FlowEdge {
    pub edge_id: String,
    pub source_node_id: String,
    pub target_node_id: String,
    pub label: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowNodeType {
    Source,
    Decoder,
    Transform,
    Filter,
    Rule,
    Sink,
    Dlq,
}

impl FromStr for FlowNodeType {
    type Err = FlowError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "source" => Ok(Self::Source),
            "decoder" => Ok(Self::Decoder),
            "transform" => Ok(Self::Transform),
            "filter" => Ok(Self::Filter),
            "rule" => Ok(Self::Rule),
            "sink" => Ok(Self::Sink),
            "dlq" => Ok(Self::Dlq),
            other => Err(FlowError::InvalidNodeType(other.to_string())),
        }
    }
}

pub fn validate_nodes_and_edges(nodes: &[FlowNode], edges: &[FlowEdge]) -> Result<(), FlowError> {
    let mut node_ids = HashSet::with_capacity(nodes.len());
    for node in nodes {
        if node.node_id.trim().is_empty() {
            return Err(FlowError::EmptyNodeId);
        }
        if !node_ids.insert(node.node_id.clone()) {
            return Err(FlowError::DuplicateNodeId(node.node_id.clone()));
        }
    }

    for edge in edges {
        if edge.edge_id.trim().is_empty() {
            return Err(FlowError::EmptyEdgeId);
        }
        if !node_ids.contains(&edge.source_node_id) {
            return Err(FlowError::UnknownEdgeNode {
                edge_id: edge.edge_id.clone(),
                node_id: edge.source_node_id.clone(),
            });
        }
        if !node_ids.contains(&edge.target_node_id) {
            return Err(FlowError::UnknownEdgeNode {
                edge_id: edge.edge_id.clone(),
                node_id: edge.target_node_id.clone(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn creates_valid_flow() {
        let now = Utc::now();
        let flow = Flow::new(
            Uuid::new_v4(),
            "mqtt-normalize-store",
            "MQTT Normalize Store",
            None,
            false,
            vec![
                FlowNode {
                    node_id: "src".to_string(),
                    node_type: FlowNodeType::Source,
                    name: Some("MQTT source".to_string()),
                    config: json!({"kind": "mqtt_subscribe"}),
                    position: None,
                },
                FlowNode {
                    node_id: "sink".to_string(),
                    node_type: FlowNodeType::Sink,
                    name: Some("Store".to_string()),
                    config: json!({"kind": "internal_observation_store"}),
                    position: None,
                },
            ],
            vec![FlowEdge {
                edge_id: "edge-1".to_string(),
                source_node_id: "src".to_string(),
                target_node_id: "sink".to_string(),
                label: None,
                metadata: None,
            }],
            None,
            now,
        )
        .unwrap();

        assert_eq!(flow.flow_key, "mqtt-normalize-store");
        assert_eq!(flow.nodes.len(), 2);
    }

    #[test]
    fn rejects_duplicate_node_ids() {
        let err = Flow::new(
            Uuid::new_v4(),
            "duplicate",
            "Duplicate",
            None,
            false,
            vec![
                FlowNode {
                    node_id: "dup".to_string(),
                    node_type: FlowNodeType::Source,
                    name: None,
                    config: json!({}),
                    position: None,
                },
                FlowNode {
                    node_id: "dup".to_string(),
                    node_type: FlowNodeType::Sink,
                    name: None,
                    config: json!({}),
                    position: None,
                },
            ],
            vec![],
            None,
            Utc::now(),
        )
        .unwrap_err();

        assert!(matches!(err, FlowError::DuplicateNodeId(_)));
    }
}
