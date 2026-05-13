use crate::{error::ApiError, AppState};
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

const REDACTED_VALUE: &str = "***REDACTED***";
const SECRET_LIKE_KEYS: [&str; 7] = [
    "password",
    "secret",
    "token",
    "api_key",
    "access_key",
    "private_key",
    "credential",
];

#[derive(Debug, Clone)]
pub(crate) struct FlowNodeDraft {
    pub node_id: String,
    pub node_type: String,
    pub name: Option<String>,
    pub config: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct FlowEdgeDraft {
    pub edge_id: Option<String>,
    pub source_node_id: String,
    pub target_node_id: String,
    pub label: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FlowValidationSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FlowValidationIssue {
    pub severity: FlowValidationSeverity,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FlowReferencedConnector {
    pub node_id: String,
    pub connector_id: String,
    pub verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exists: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FlowNodePlan {
    pub node_id: String,
    pub node_type: String,
    pub category: String,
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub reachable: bool,
    pub config: Value,
    pub incoming_from: Vec<String>,
    pub outgoing_to: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FlowPlannedSink {
    pub node_id: String,
    pub node_type: String,
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub config: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FlowAnalysis {
    pub valid: bool,
    pub validation_issues: Vec<FlowValidationIssue>,
    pub planned_path: Vec<String>,
    pub node_plan: Vec<FlowNodePlan>,
    pub referenced_connectors: Vec<FlowReferencedConnector>,
    pub planned_sinks: Vec<FlowPlannedSink>,
    pub would_store_observation: bool,
    pub would_publish_mqtt: bool,
    pub would_forward_http: bool,
    pub would_create_event: bool,
    pub would_create_command: bool,
    pub would_use_dlq: bool,
}

struct NodeState<'a> {
    node: &'a FlowNodeDraft,
    normalized_type: Option<&'static str>,
}

pub(crate) fn redact_sensitive_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let redacted = if is_secret_like_key(key) {
                        Value::String(REDACTED_VALUE.to_string())
                    } else {
                        redact_sensitive_json(value)
                    };
                    (key.clone(), redacted)
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(redact_sensitive_json).collect()),
        _ => value.clone(),
    }
}

pub(crate) fn analyze_flow(
    state: &AppState,
    tenant_id: Uuid,
    nodes: &[FlowNodeDraft],
    edges: &[FlowEdgeDraft],
    requested_source_node_id: Option<&str>,
) -> Result<FlowAnalysis, ApiError> {
    let mut issues = Vec::new();
    let mut node_states = Vec::with_capacity(nodes.len());
    let mut node_index = HashMap::with_capacity(nodes.len());
    let mut source_nodes = Vec::new();
    let mut sink_or_dlq_nodes = Vec::new();

    for node in nodes {
        if node.node_id.trim().is_empty() {
            issues.push(issue(
                FlowValidationSeverity::Error,
                "flow_node_id_empty",
                "node_id must not be empty",
                None,
                None,
                Some("nodes[].node_id"),
            ));
            continue;
        }

        if node_index.contains_key(node.node_id.as_str()) {
            issues.push(issue(
                FlowValidationSeverity::Error,
                "flow_duplicate_node_id",
                format!("duplicate node_id '{}' in flow definition", node.node_id),
                Some(node.node_id.clone()),
                None,
                Some("nodes[].node_id"),
            ));
        } else {
            node_index.insert(node.node_id.as_str(), node_states.len());
        }

        let normalized_type = normalize_node_type(&node.node_type);
        if normalized_type.is_none() {
            issues.push(issue(
                FlowValidationSeverity::Error,
                "flow_invalid_node_type",
                format!("invalid node_type '{}'", node.node_type),
                Some(node.node_id.clone()),
                None,
                Some("nodes[].node_type"),
            ));
        } else if normalized_type == Some("source") {
            source_nodes.push(node.node_id.clone());
        } else if matches!(normalized_type, Some("sink" | "dlq")) {
            sink_or_dlq_nodes.push(node.node_id.clone());
        }

        node_states.push(NodeState {
            node,
            normalized_type,
        });
    }

    if nodes.is_empty() {
        issues.push(issue(
            FlowValidationSeverity::Error,
            "flow_nodes_empty",
            "flow must include at least one node",
            None,
            None,
            Some("nodes"),
        ));
    }

    if source_nodes.is_empty() {
        issues.push(issue(
            FlowValidationSeverity::Error,
            "flow_source_missing",
            "flow must include at least one source node",
            None,
            None,
            Some("nodes"),
        ));
    }

    if sink_or_dlq_nodes.is_empty() {
        issues.push(issue(
            FlowValidationSeverity::Error,
            "flow_sink_or_dlq_missing",
            "flow must include at least one sink or dlq node",
            None,
            None,
            Some("nodes"),
        ));
    }

    let mut seen_edge_ids = HashSet::new();
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut reverse_adjacency: HashMap<&str, Vec<&str>> = HashMap::new();

    for edge in edges {
        match edge.edge_id.as_deref() {
            Some(edge_id) if edge_id.trim().is_empty() => issues.push(issue(
                FlowValidationSeverity::Error,
                "flow_edge_id_empty",
                "edge_id must not be empty when provided",
                None,
                None,
                Some("edges[].edge_id"),
            )),
            Some(edge_id) => {
                if !seen_edge_ids.insert(edge_id.to_string()) {
                    issues.push(issue(
                        FlowValidationSeverity::Error,
                        "flow_duplicate_edge_id",
                        format!("duplicate edge_id '{edge_id}' in flow definition"),
                        None,
                        Some(edge_id.to_string()),
                        Some("edges[].edge_id"),
                    ));
                }
            }
            None => {}
        }

        let source_exists = node_index.contains_key(edge.source_node_id.as_str());
        let target_exists = node_index.contains_key(edge.target_node_id.as_str());
        if !source_exists {
            issues.push(issue(
                FlowValidationSeverity::Error,
                "flow_unknown_edge_source",
                format!(
                    "edge references unknown source node_id '{}'",
                    edge.source_node_id
                ),
                Some(edge.source_node_id.clone()),
                edge.edge_id.clone(),
                Some("edges[].source_node_id"),
            ));
        }
        if !target_exists {
            issues.push(issue(
                FlowValidationSeverity::Error,
                "flow_unknown_edge_target",
                format!(
                    "edge references unknown target node_id '{}'",
                    edge.target_node_id
                ),
                Some(edge.target_node_id.clone()),
                edge.edge_id.clone(),
                Some("edges[].target_node_id"),
            ));
        }

        if source_exists && target_exists {
            adjacency
                .entry(edge.source_node_id.as_str())
                .or_default()
                .push(edge.target_node_id.as_str());
            reverse_adjacency
                .entry(edge.target_node_id.as_str())
                .or_default()
                .push(edge.source_node_id.as_str());
        }
    }

    for state_node in &node_states {
        let incoming = reverse_adjacency
            .get(state_node.node.node_id.as_str())
            .map(Vec::len)
            .unwrap_or(0);
        let outgoing = adjacency
            .get(state_node.node.node_id.as_str())
            .map(Vec::len)
            .unwrap_or(0);
        if incoming == 0 && outgoing == 0 {
            issues.push(issue(
                FlowValidationSeverity::Warning,
                "flow_isolated_node",
                format!(
                    "node '{}' is isolated and not connected by any edge",
                    state_node.node.node_id
                ),
                Some(state_node.node.node_id.clone()),
                None,
                None,
            ));
        }
    }

    if let Some(cycle_node) = detect_cycle(nodes, &adjacency) {
        issues.push(issue(
            FlowValidationSeverity::Error,
            "flow_cycle_detected",
            format!("flow graph contains a cycle involving node '{cycle_node}'"),
            Some(cycle_node),
            None,
            None,
        ));
    }

    let referenced_connectors =
        collect_connector_references(state, tenant_id, &node_states, &mut issues)?;

    let start_nodes = resolve_start_nodes(
        requested_source_node_id,
        &node_index,
        &source_nodes,
        &mut issues,
    );
    let reachable = collect_reachable_nodes(&adjacency, &start_nodes);
    let planned_path = node_states
        .iter()
        .filter(|state_node| reachable.contains(state_node.node.node_id.as_str()))
        .map(|state_node| state_node.node.node_id.clone())
        .collect::<Vec<_>>();

    let node_plan = node_states
        .iter()
        .map(|state_node| FlowNodePlan {
            node_id: state_node.node.node_id.clone(),
            node_type: state_node.node.node_type.clone(),
            category: node_category(state_node.normalized_type).to_string(),
            name: state_node.node.name.clone(),
            kind: config_kind(&state_node.node.config),
            reachable: reachable.contains(state_node.node.node_id.as_str()),
            config: redact_sensitive_json(&state_node.node.config),
            incoming_from: reverse_adjacency
                .get(state_node.node.node_id.as_str())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
            outgoing_to: adjacency
                .get(state_node.node.node_id.as_str())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
        })
        .collect::<Vec<_>>();

    let planned_sinks = node_states
        .iter()
        .filter(|state_node| {
            matches!(state_node.normalized_type, Some("sink" | "dlq"))
                && reachable.contains(state_node.node.node_id.as_str())
        })
        .map(|state_node| FlowPlannedSink {
            node_id: state_node.node.node_id.clone(),
            node_type: state_node.node.node_type.clone(),
            name: state_node.node.name.clone(),
            kind: config_kind(&state_node.node.config),
            config: redact_sensitive_json(&state_node.node.config),
        })
        .collect::<Vec<_>>();

    let planned_sink_kinds = planned_sinks
        .iter()
        .filter_map(|sink| sink.kind.clone())
        .collect::<HashSet<_>>();
    let has_dlq_node = node_states
        .iter()
        .any(|state_node| matches!(state_node.normalized_type, Some("dlq")));
    let valid = !issues
        .iter()
        .any(|issue| issue.severity == FlowValidationSeverity::Error);

    Ok(FlowAnalysis {
        valid,
        validation_issues: issues,
        planned_path,
        node_plan,
        referenced_connectors,
        planned_sinks,
        would_store_observation: planned_sink_kinds.contains("internal_observation_store"),
        would_publish_mqtt: planned_sink_kinds.contains("mqtt_publish"),
        would_forward_http: planned_sink_kinds.contains("http_forward"),
        would_create_event: planned_sink_kinds.contains("event_create"),
        would_create_command: planned_sink_kinds.contains("command_create"),
        would_use_dlq: has_dlq_node || planned_sink_kinds.contains("dlq"),
    })
}

fn collect_connector_references(
    state: &AppState,
    tenant_id: Uuid,
    node_states: &[NodeState<'_>],
    issues: &mut Vec<FlowValidationIssue>,
) -> Result<Vec<FlowReferencedConnector>, ApiError> {
    let mut references = Vec::new();

    for state_node in node_states {
        if !matches!(state_node.normalized_type, Some("source" | "sink")) {
            continue;
        }

        let Some(raw_connector_id) = state_node
            .node
            .config
            .get("connector_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };

        match Uuid::parse_str(raw_connector_id) {
            Ok(connector_id) => {
                let exists = state
                    .storage
                    .get_ingestion_connector(tenant_id, connector_id)?
                    .is_some();
                references.push(FlowReferencedConnector {
                    node_id: state_node.node.node_id.clone(),
                    connector_id: raw_connector_id.to_string(),
                    verified: true,
                    exists: Some(exists),
                });
                if !exists {
                    issues.push(issue(
                        FlowValidationSeverity::Error,
                        "flow_connector_not_found",
                        format!(
                            "node '{}' references connector '{}' that does not exist for the tenant",
                            state_node.node.node_id, raw_connector_id
                        ),
                        Some(state_node.node.node_id.clone()),
                        None,
                        Some("config.connector_id"),
                    ));
                }
            }
            Err(_) => {
                references.push(FlowReferencedConnector {
                    node_id: state_node.node.node_id.clone(),
                    connector_id: raw_connector_id.to_string(),
                    verified: false,
                    exists: None,
                });
                issues.push(issue(
                    FlowValidationSeverity::Info,
                    "flow_connector_reference_unverified",
                    format!(
                        "node '{}' uses a non-UUID connector_id reference that was not verified",
                        state_node.node.node_id
                    ),
                    Some(state_node.node.node_id.clone()),
                    None,
                    Some("config.connector_id"),
                ));
            }
        }
    }

    Ok(references)
}

fn resolve_start_nodes<'a>(
    requested_source_node_id: Option<&'a str>,
    node_index: &HashMap<&str, usize>,
    source_nodes: &'a [String],
    issues: &mut Vec<FlowValidationIssue>,
) -> Vec<&'a str> {
    match requested_source_node_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(source_node_id) if node_index.contains_key(source_node_id) => vec![source_node_id],
        Some(source_node_id) => {
            issues.push(issue(
                FlowValidationSeverity::Error,
                "flow_requested_source_missing",
                format!(
                    "requested source_node_id '{}' was not found in the flow",
                    source_node_id
                ),
                Some(source_node_id.to_string()),
                None,
                Some("source_node_id"),
            ));
            Vec::new()
        }
        None => source_nodes.iter().map(String::as_str).collect(),
    }
}

fn collect_reachable_nodes<'a>(
    adjacency: &HashMap<&'a str, Vec<&'a str>>,
    start_nodes: &[&'a str],
) -> HashSet<&'a str> {
    let mut reachable = HashSet::new();
    let mut stack = start_nodes.to_vec();
    while let Some(node_id) = stack.pop() {
        if !reachable.insert(node_id) {
            continue;
        }
        if let Some(children) = adjacency.get(node_id) {
            for child in children {
                stack.push(child);
            }
        }
    }
    reachable
}

fn detect_cycle<'a>(
    nodes: &'a [FlowNodeDraft],
    adjacency: &HashMap<&'a str, Vec<&'a str>>,
) -> Option<String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum VisitState {
        Visiting,
        Visited,
    }

    fn visit<'a>(
        node_id: &'a str,
        adjacency: &HashMap<&'a str, Vec<&'a str>>,
        states: &mut HashMap<&'a str, VisitState>,
    ) -> Option<&'a str> {
        match states.get(node_id) {
            Some(VisitState::Visiting) => return Some(node_id),
            Some(VisitState::Visited) => return None,
            None => {}
        }

        states.insert(node_id, VisitState::Visiting);
        if let Some(children) = adjacency.get(node_id) {
            for child in children {
                if let Some(found) = visit(child, adjacency, states) {
                    return Some(found);
                }
            }
        }
        states.insert(node_id, VisitState::Visited);
        None
    }

    let mut states = HashMap::new();
    for node in nodes {
        if let Some(found) = visit(node.node_id.as_str(), adjacency, &mut states) {
            return Some(found.to_string());
        }
    }
    None
}

fn normalize_node_type(value: &str) -> Option<&'static str> {
    match value {
        "source" => Some("source"),
        "decoder" => Some("decoder"),
        "transform" => Some("transform"),
        "filter" => Some("filter"),
        "rule" => Some("rule"),
        "sink" => Some("sink"),
        "dlq" => Some("dlq"),
        _ => None,
    }
}

fn config_kind(config: &Value) -> Option<String> {
    config
        .get("kind")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn node_category(normalized_type: Option<&str>) -> &'static str {
    match normalized_type {
        Some("source") => "source",
        Some("sink") => "sink",
        Some("dlq") => "dlq",
        Some("decoder" | "transform" | "filter" | "rule") => "transform",
        _ => "unknown",
    }
}

fn is_secret_like_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    SECRET_LIKE_KEYS
        .iter()
        .any(|candidate| normalized.contains(candidate))
}

fn issue(
    severity: FlowValidationSeverity,
    code: impl Into<String>,
    message: impl Into<String>,
    node_id: Option<String>,
    edge_id: Option<String>,
    field: Option<&str>,
) -> FlowValidationIssue {
    FlowValidationIssue {
        severity,
        code: code.into(),
        message: message.into(),
        node_id,
        edge_id,
        field: field.map(ToOwned::to_owned),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_secret_like_keys_recursively() {
        let value = json!({
            "token": "abc",
            "nested": {
                "api_key": "def",
                "items": [
                    {"password_hint": "still secret"},
                    {"safe": true}
                ]
            }
        });

        let redacted = redact_sensitive_json(&value);

        assert_eq!(redacted["token"], REDACTED_VALUE);
        assert_eq!(redacted["nested"]["api_key"], REDACTED_VALUE);
        assert_eq!(
            redacted["nested"]["items"][0]["password_hint"],
            REDACTED_VALUE
        );
        assert_eq!(redacted["nested"]["items"][1]["safe"], true);
    }
}
