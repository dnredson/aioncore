use crate::{
    error::ApiError,
    flow_support::{
        analyze_flow, redact_sensitive_json, FlowEdgeDraft, FlowNodeDraft, FlowValidationIssue,
    },
    record_event, AppState, EventDraft,
};
use aion_event::EventSeverity;
use aion_observation::{Observation, ObservationValue};
use aion_payload::{
    CanonicalJsonDecoder, DecodeInput, PayloadDecoder, PayloadFormat, SenMlJsonDecoder,
    UltraLightDecoder,
};
use aion_storage::{ConnectorSecretType, IngestionConnector, IngestionConnectorType};
use chrono::{DateTime, Utc};
use rumqttc::{Client as MqttClient, Event as MqttEvent, Incoming, MqttOptions, Outgoing, QoS};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct FlowExecutionRequest {
    pub sample_payload: Option<Value>,
    pub raw_message_id: Option<Uuid>,
    pub payload_format: Option<String>,
    pub metadata: Option<Value>,
    #[serde(default = "default_execution_mode")]
    pub mode: String,
    #[serde(default)]
    pub allow_side_effects: bool,
    #[serde(default)]
    pub requested_sink_actions: Vec<String>,
    pub operator_reason: Option<String>,
    pub approval_reference: Option<String>,
}

impl FlowExecutionRequest {
    pub(crate) fn requests_side_effects(&self) -> bool {
        self.allow_side_effects || !self.requested_sink_actions.is_empty()
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FlowExecutionResponse {
    pub flow_id: Option<Uuid>,
    pub flow_key: Option<String>,
    pub execution_id: Uuid,
    pub simulated: bool,
    pub side_effects_performed: bool,
    pub valid: bool,
    pub authorization: FlowExecutionAuthorization,
    pub validation_issues: Vec<FlowValidationIssue>,
    pub node_results: Vec<NodeExecutionResult>,
    pub edge_results: Vec<EdgeExecutionResult>,
    pub sink_results: Vec<SinkExecutionResult>,
    pub observations_preview: Vec<Value>,
    pub events_preview: Vec<Value>,
    pub commands_preview: Vec<Value>,
    pub dlq_preview: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FlowExecutionAuthorization {
    pub requested_side_effects: bool,
    pub side_effects_authorized: bool,
    pub real_side_effects_supported: bool,
    pub policy: String,
    pub supported_sink_actions: Vec<String>,
    pub requested_sink_actions: Vec<String>,
    pub operator_reason_present: bool,
    pub approval_reference_present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denied_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct NodeExecutionResult {
    pub node_id: String,
    pub node_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub status: NodeExecutionStatus,
    pub input_summary: Value,
    pub output_summary: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeExecutionStatus {
    Skipped,
    Passed,
    Failed,
    Simulated,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SinkExecutionResult {
    pub node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub action: SinkExecutionAction,
    pub side_effect_performed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SinkExecutionAction {
    WouldStoreObservation,
    WouldPublishMqtt,
    WouldForwardHttp,
    WouldCreateEvent,
    WouldCreateCommand,
    WouldWriteDlq,
    NoOp,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct EdgeExecutionResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub source_node_id: String,
    pub target_node_id: String,
    pub status: EdgeExecutionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EdgeExecutionStatus {
    Traversed,
    Skipped,
    Failed,
}

#[derive(Debug, Clone)]
struct ExecutionEdgeState {
    edge_id: Option<String>,
    label: Option<String>,
    source_node_id: String,
    target_node_id: String,
    metadata: Option<Value>,
}

#[derive(Debug, Clone)]
struct ExecutionInput {
    payload: Value,
    payload_bytes: Vec<u8>,
    payload_format: Option<String>,
    raw_message_id: Option<Uuid>,
    source_metadata: Value,
}

#[derive(Debug, Clone)]
struct ExecutionFrame {
    payload: Value,
    payload_bytes: Vec<u8>,
    payload_format: Option<String>,
    raw_message_id: Option<Uuid>,
    observations_preview: Vec<Value>,
    metadata: Value,
}

#[derive(Debug, Clone)]
struct FlowNodeState {
    node_id: String,
    node_type: String,
    kind: Option<String>,
    config: Value,
}

pub(crate) async fn execute_flow(
    state: &AppState,
    tenant_id: Uuid,
    flow_id: Option<Uuid>,
    flow_key: Option<String>,
    nodes: &[FlowNodeDraft],
    edges: &[FlowEdgeDraft],
    request: &FlowExecutionRequest,
    side_effects_authorized: bool,
) -> Result<FlowExecutionResponse, ApiError> {
    if !request.mode.eq_ignore_ascii_case("simulate") {
        return Err(ApiError::bad_request(
            "only mode=simulate is supported for flow execution in this milestone",
        ));
    }

    let started_at = Utc::now();
    let execution_id = Uuid::new_v4();
    let analysis = analyze_flow(state, tenant_id, nodes, edges, None)?;
    let mut response = FlowExecutionResponse {
        flow_id,
        flow_key,
        execution_id,
        simulated: true,
        side_effects_performed: false,
        valid: analysis.valid,
        authorization: build_execution_authorization(request, side_effects_authorized),
        validation_issues: analysis.validation_issues.clone(),
        node_results: Vec::new(),
        edge_results: Vec::new(),
        sink_results: Vec::new(),
        observations_preview: Vec::new(),
        events_preview: Vec::new(),
        commands_preview: Vec::new(),
        dlq_preview: Vec::new(),
        error: None,
        started_at,
        completed_at: started_at,
    };

    if !analysis.valid {
        response.error = Some("flow validation failed; execution was not attempted".to_string());
        response.completed_at = Utc::now();
        return Ok(response);
    }

    let input = match resolve_execution_input(state, tenant_id, request)? {
        Some(input) => input,
        None => {
            response.valid = false;
            response.error = Some(
                "sample_payload or raw_message_id is required for simulated execution".to_string(),
            );
            response.completed_at = Utc::now();
            return Ok(response);
        }
    };

    let node_map = nodes
        .iter()
        .map(|node| {
            (
                node.node_id.clone(),
                FlowNodeState {
                    node_id: node.node_id.clone(),
                    node_type: node.node_type.clone(),
                    kind: config_kind(&node.config),
                    config: node.config.clone(),
                },
            )
        })
        .collect::<HashMap<_, _>>();
    let adjacency = build_adjacency(edges);
    let source_nodes = nodes
        .iter()
        .filter(|node| node.node_type == "source")
        .map(|node| node.node_id.clone())
        .collect::<Vec<_>>();

    let root_frame = ExecutionFrame {
        payload: input.payload,
        payload_bytes: input.payload_bytes,
        payload_format: input.payload_format,
        raw_message_id: input.raw_message_id,
        observations_preview: Vec::new(),
        metadata: json!({
            "request_metadata": request.metadata.clone().unwrap_or_else(|| json!({})),
            "source_metadata": input.source_metadata,
        }),
    };

    for source_node_id in source_nodes {
        let mut path = Vec::new();
        execute_node_path(
            state,
            tenant_id,
            &source_node_id,
            &node_map,
            &adjacency,
            Some(root_frame.clone()),
            None,
            &mut response,
            &mut path,
        );
    }

    response.completed_at = Utc::now();
    Ok(response)
}

fn build_execution_authorization(
    request: &FlowExecutionRequest,
    side_effects_authorized: bool,
) -> FlowExecutionAuthorization {
    let requested_side_effects = request.requests_side_effects();
    FlowExecutionAuthorization {
        requested_side_effects,
        side_effects_authorized: requested_side_effects && side_effects_authorized,
        real_side_effects_supported: true,
        policy: "safe_internal_sinks_only".to_string(),
        supported_sink_actions: vec![
            "store_observation".to_string(),
            "create_event".to_string(),
            "publish_mqtt".to_string(),
            "forward_http".to_string(),
        ],
        requested_sink_actions: request.requested_sink_actions.clone(),
        operator_reason_present: request
            .operator_reason
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false),
        approval_reference_present: request
            .approval_reference
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false),
        denied_reason: if requested_side_effects && !side_effects_authorized {
            Some(
                "flows:execute scope is required before real side effects can be considered"
                    .to_string(),
            )
        } else {
            None
        },
    }
}

fn resolve_execution_input(
    state: &AppState,
    tenant_id: Uuid,
    request: &FlowExecutionRequest,
) -> Result<Option<ExecutionInput>, ApiError> {
    if let Some(payload) = request.sample_payload.clone() {
        return Ok(Some(ExecutionInput {
            payload_bytes: value_to_payload_bytes(&payload)?,
            payload,
            payload_format: request.payload_format.clone(),
            raw_message_id: None,
            source_metadata: json!({
                "source": "sample_payload",
            }),
        }));
    }

    let Some(raw_message_id) = request.raw_message_id else {
        return Ok(None);
    };
    let raw_message = state
        .storage
        .get_raw_message(tenant_id, raw_message_id)?
        .ok_or_else(ApiError::not_found)?;
    let payload = raw_payload_value(&raw_message.payload);
    let payload_format = request
        .payload_format
        .clone()
        .or(raw_message.payload_format.clone())
        .or(raw_message.decoder_hint.clone());

    Ok(Some(ExecutionInput {
        payload,
        payload_bytes: raw_message.payload.clone(),
        payload_format,
        raw_message_id: Some(raw_message.id),
        source_metadata: json!({
            "source": "raw_message",
            "raw_message_id": raw_message.id,
            "content_type": raw_message.content_type,
            "received_at": raw_message.received_at,
            "headers": raw_message.headers,
        }),
    }))
}

fn execute_node_path(
    state: &AppState,
    tenant_id: Uuid,
    node_id: &str,
    node_map: &HashMap<String, FlowNodeState>,
    adjacency: &HashMap<String, Vec<ExecutionEdgeState>>,
    frame: Option<ExecutionFrame>,
    skip_reason: Option<&str>,
    response: &mut FlowExecutionResponse,
    path: &mut Vec<String>,
) {
    if path.iter().any(|visited| visited == node_id) {
        response.node_results.push(NodeExecutionResult {
            node_id: node_id.to_string(),
            node_type: "unknown".to_string(),
            kind: None,
            status: NodeExecutionStatus::Failed,
            input_summary: json!({
                "path": path.to_vec(),
            }),
            output_summary: json!({
                "cycle_guard_triggered": true,
            }),
            error: Some("cycle detected while simulating flow path".to_string()),
        });
        return;
    }
    path.push(node_id.to_string());

    let Some(node) = node_map.get(node_id) else {
        path.pop();
        return;
    };

    let next = match frame {
        Some(frame) => execute_single_node(state, tenant_id, node, frame, response),
        None => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Skipped,
                input_summary: json!({
                    "reason": skip_reason.unwrap_or("upstream node did not produce output"),
                    "path": path.to_vec(),
                }),
                output_summary: json!({
                    "propagated": false,
                }),
                error: None,
            });
            maybe_record_skipped_sink(node, response, skip_reason);
            None
        }
    };

    let children = adjacency.get(node_id).cloned().unwrap_or_default();
    for edge in children {
        let edge_condition = edge_condition(&edge.metadata);
        let should_traverse = match next.as_ref() {
            Some(frame) => evaluate_edge_condition(edge_condition.as_ref(), frame),
            None => Ok(false),
        };

        match should_traverse {
            Ok(true) => {
                response.edge_results.push(EdgeExecutionResult {
                    edge_id: edge.edge_id.clone(),
                    label: edge.label.clone(),
                    source_node_id: edge.source_node_id.clone(),
                    target_node_id: edge.target_node_id.clone(),
                    status: EdgeExecutionStatus::Traversed,
                    condition: edge_condition
                        .clone()
                        .map(|value| redact_sensitive_json(&value)),
                    error: None,
                });
                execute_node_path(
                    state,
                    tenant_id,
                    &edge.target_node_id,
                    node_map,
                    adjacency,
                    next.clone(),
                    None,
                    response,
                    path,
                );
            }
            Ok(false) => {
                let reason = if next.is_none() {
                    skip_reason.unwrap_or("upstream execution did not continue")
                } else {
                    "edge condition evaluated to false"
                };
                response.edge_results.push(EdgeExecutionResult {
                    edge_id: edge.edge_id.clone(),
                    label: edge.label.clone(),
                    source_node_id: edge.source_node_id.clone(),
                    target_node_id: edge.target_node_id.clone(),
                    status: EdgeExecutionStatus::Skipped,
                    condition: edge_condition
                        .clone()
                        .map(|value| redact_sensitive_json(&value)),
                    error: Some(reason.to_string()),
                });
                execute_node_path(
                    state,
                    tenant_id,
                    &edge.target_node_id,
                    node_map,
                    adjacency,
                    None,
                    Some(reason),
                    response,
                    path,
                );
            }
            Err(error) => {
                response.edge_results.push(EdgeExecutionResult {
                    edge_id: edge.edge_id.clone(),
                    label: edge.label.clone(),
                    source_node_id: edge.source_node_id.clone(),
                    target_node_id: edge.target_node_id.clone(),
                    status: EdgeExecutionStatus::Failed,
                    condition: edge_condition.map(|value| redact_sensitive_json(&value)),
                    error: Some(error.clone()),
                });
                execute_node_path(
                    state,
                    tenant_id,
                    &edge.target_node_id,
                    node_map,
                    adjacency,
                    None,
                    Some(error.as_str()),
                    response,
                    path,
                );
            }
        }
    }

    path.pop();
}

fn execute_single_node(
    state: &AppState,
    tenant_id: Uuid,
    node: &FlowNodeState,
    frame: ExecutionFrame,
    response: &mut FlowExecutionResponse,
) -> Option<ExecutionFrame> {
    let input_summary = frame_summary(&frame);

    match node.node_type.as_str() {
        "source" => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Passed,
                input_summary,
                output_summary: json!({
                    "accepted": true,
                    "payload_format": frame.payload_format.clone(),
                    "preview": summarize_payload(&frame.payload),
                }),
                error: None,
            });
            Some(frame)
        }
        "decoder" => execute_decoder_node(node, frame, response, input_summary),
        "transform" => execute_transform_node(node, frame, response, input_summary),
        "filter" => execute_filter_node(node, frame, response, input_summary),
        "rule" => execute_rule_node(node, frame, response, input_summary),
        "sink" | "dlq" => execute_sink_node(state, tenant_id, node, frame, response, input_summary),
        _ => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Simulated,
                input_summary,
                output_summary: json!({
                    "unsupported_node_type": node.node_type,
                }),
                error: None,
            });
            Some(frame)
        }
    }
}

fn execute_decoder_node(
    node: &FlowNodeState,
    mut frame: ExecutionFrame,
    response: &mut FlowExecutionResponse,
    input_summary: Value,
) -> Option<ExecutionFrame> {
    let decode_result = match node.kind.as_deref() {
        Some("senml_decode") => decode_measurements(
            &SenMlJsonDecoder,
            &frame,
            node,
            node.kind.as_deref().unwrap_or("senml_decode"),
        ),
        Some("ultralight_decode") => decode_measurements(
            &UltraLightDecoder,
            &frame,
            node,
            node.kind.as_deref().unwrap_or("ultralight_decode"),
        ),
        Some("canonical_json") => decode_measurements(
            &CanonicalJsonDecoder,
            &frame,
            node,
            node.kind.as_deref().unwrap_or("canonical_json"),
        ),
        _ => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Simulated,
                input_summary,
                output_summary: json!({
                    "decoder_preview": "unsupported",
                }),
                error: None,
            });
            return Some(frame);
        }
    };

    match decode_result {
        Ok(preview) => {
            frame.observations_preview = preview;
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Passed,
                input_summary,
                output_summary: json!({
                    "decoded_measurement_count": frame.observations_preview.len(),
                    "preview": frame.observations_preview,
                }),
                error: None,
            });
            Some(frame)
        }
        Err(error) => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Failed,
                input_summary,
                output_summary: json!({
                    "decoded_measurement_count": 0,
                }),
                error: Some(error),
            });
            None
        }
    }
}

fn execute_transform_node(
    node: &FlowNodeState,
    mut frame: ExecutionFrame,
    response: &mut FlowExecutionResponse,
    input_summary: Value,
) -> Option<ExecutionFrame> {
    match node.kind.as_deref() {
        Some("canonical_json") => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Passed,
                input_summary,
                output_summary: json!({
                    "preview": summarize_payload(&frame.payload),
                    "payload_format": frame.payload_format.clone(),
                }),
                error: None,
            });
            Some(frame)
        }
        Some("json_map") => {
            let mapping = match parse_jsonish_value(node.config.get("mapping")) {
                Ok(Some(mapping)) => mapping,
                Ok(None) => json!({}),
                Err(error) => {
                    response.node_results.push(NodeExecutionResult {
                        node_id: node.node_id.clone(),
                        node_type: node.node_type.clone(),
                        kind: node.kind.clone(),
                        status: NodeExecutionStatus::Failed,
                        input_summary,
                        output_summary: json!({}),
                        error: Some(error),
                    });
                    return None;
                }
            };
            let mapped = apply_simple_mapping(&mapping, &frame.payload);
            frame.payload = mapped.clone();
            frame.payload_bytes = value_to_payload_bytes(&mapped).ok()?;
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Passed,
                input_summary,
                output_summary: json!({
                    "mapping_preview": {
                        "mapping": mapping,
                        "output": mapped,
                    }
                }),
                error: None,
            });
            Some(frame)
        }
        _ => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Simulated,
                input_summary,
                output_summary: json!({
                    "transform_preview": "unsupported",
                }),
                error: None,
            });
            Some(frame)
        }
    }
}

fn execute_filter_node(
    node: &FlowNodeState,
    frame: ExecutionFrame,
    response: &mut FlowExecutionResponse,
    input_summary: Value,
) -> Option<ExecutionFrame> {
    if node.kind.as_deref() != Some("filter_condition") {
        response.node_results.push(NodeExecutionResult {
            node_id: node.node_id.clone(),
            node_type: node.node_type.clone(),
            kind: node.kind.clone(),
            status: NodeExecutionStatus::Simulated,
            input_summary,
            output_summary: json!({
                "filter_preview": "unsupported",
            }),
            error: None,
        });
        return Some(frame);
    }

    let outcome = evaluate_condition_from_config(&node.config, &frame);
    match outcome {
        Ok(matched) => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Passed,
                input_summary,
                output_summary: json!({
                    "matched": matched,
                }),
                error: None,
            });
            if matched {
                Some(frame)
            } else {
                None
            }
        }
        Err(error) => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Failed,
                input_summary,
                output_summary: json!({
                    "matched": false,
                }),
                error: Some(error),
            });
            None
        }
    }
}

fn execute_rule_node(
    node: &FlowNodeState,
    frame: ExecutionFrame,
    response: &mut FlowExecutionResponse,
    input_summary: Value,
) -> Option<ExecutionFrame> {
    if node.kind.as_deref() != Some("threshold_rule") {
        response.node_results.push(NodeExecutionResult {
            node_id: node.node_id.clone(),
            node_type: node.node_type.clone(),
            kind: node.kind.clone(),
            status: NodeExecutionStatus::Simulated,
            input_summary,
            output_summary: json!({
                "rule_preview": "unsupported",
            }),
            error: None,
        });
        return Some(frame);
    }

    let condition = match parse_jsonish_value(node.config.get("condition")) {
        Ok(Some(condition)) => condition,
        Ok(None) => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Simulated,
                input_summary,
                output_summary: json!({
                    "triggered": false,
                    "condition": Value::Null,
                }),
                error: None,
            });
            return Some(frame);
        }
        Err(error) => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Failed,
                input_summary,
                output_summary: json!({
                    "triggered": false,
                }),
                error: Some(error),
            });
            return None;
        }
    };

    let triggered = evaluate_condition_object(&condition, &frame);
    match triggered {
        Ok(triggered) => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Passed,
                input_summary,
                output_summary: json!({
                    "triggered": triggered,
                    "condition": condition,
                }),
                error: None,
            });
            if triggered {
                Some(frame)
            } else {
                None
            }
        }
        Err(error) => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Failed,
                input_summary,
                output_summary: json!({
                    "triggered": false,
                    "condition": condition,
                }),
                error: Some(error),
            });
            None
        }
    }
}

fn execute_sink_node(
    state: &AppState,
    tenant_id: Uuid,
    node: &FlowNodeState,
    frame: ExecutionFrame,
    response: &mut FlowExecutionResponse,
    input_summary: Value,
) -> Option<ExecutionFrame> {
    let redacted_config = redact_sensitive_json(&node.config);
    let sink_preview = match node.kind.as_deref() {
        Some("internal_observation_store") => {
            let preview = build_observation_preview(node, &frame);
            response.observations_preview.extend(preview.clone());
            if sink_action_requested(&response.authorization, "store_observation") {
                match store_observation_previews(
                    state,
                    tenant_id,
                    node,
                    &frame,
                    &preview,
                    response.execution_id,
                ) {
                    Ok(stored) => {
                        let stored_count = stored.len();
                        response.side_effects_performed = true;
                        response.sink_results.push(SinkExecutionResult {
                            node_id: node.node_id.clone(),
                            kind: node.kind.clone(),
                            action: SinkExecutionAction::WouldStoreObservation,
                            side_effect_performed: true,
                            preview: Some(json!({
                                "stored_observations": stored,
                                "requested_action": "store_observation",
                            })),
                        });
                        json!({
                            "action": "stored_observation",
                            "stored_count": stored_count,
                            "preview_count": preview.len(),
                            "config": redacted_config,
                        })
                    }
                    Err(error) => {
                        response.sink_results.push(SinkExecutionResult {
                            node_id: node.node_id.clone(),
                            kind: node.kind.clone(),
                            action: SinkExecutionAction::WouldStoreObservation,
                            side_effect_performed: false,
                            preview: Some(json!({
                                "error": error,
                                "preview": preview,
                            })),
                        });
                        json!({
                            "action": "would_store_observation",
                            "preview_count": preview.len(),
                            "side_effect_error": error,
                            "config": redacted_config,
                        })
                    }
                }
            } else {
                response.sink_results.push(SinkExecutionResult {
                    node_id: node.node_id.clone(),
                    kind: node.kind.clone(),
                    action: SinkExecutionAction::WouldStoreObservation,
                    side_effect_performed: false,
                    preview: Some(json!(preview)),
                });
                json!({
                    "action": "would_store_observation",
                    "preview_count": preview.len(),
                    "config": redacted_config,
                })
            }
        }
        Some("raw_message_store") => {
            let preview = json!({
                "payload": frame.payload.clone(),
                "payload_format": frame.payload_format.clone(),
                "raw_message_id": frame.raw_message_id,
            });
            response.sink_results.push(SinkExecutionResult {
                node_id: node.node_id.clone(),
                kind: node.kind.clone(),
                action: SinkExecutionAction::NoOp,
                side_effect_performed: false,
                preview: Some(preview.clone()),
            });
            json!({
                "action": "raw_message_preview_only",
                "config": redacted_config,
                "preview": preview,
            })
        }
        Some("mqtt_publish") => {
            let preview = build_mqtt_publish_preview(node, &frame);
            if external_sink_action_requested(&response.authorization, "publish_mqtt") {
                match execute_mqtt_publish(state, tenant_id, node, &frame, response.execution_id) {
                    Ok(result) => {
                        response.side_effects_performed = true;
                        response.sink_results.push(SinkExecutionResult {
                            node_id: node.node_id.clone(),
                            kind: node.kind.clone(),
                            action: SinkExecutionAction::WouldPublishMqtt,
                            side_effect_performed: true,
                            preview: Some(result.clone()),
                        });
                        json!({
                            "action": "published_mqtt",
                            "preview": preview,
                            "result": result,
                            "config": redacted_config,
                        })
                    }
                    Err(error) => {
                        response.sink_results.push(SinkExecutionResult {
                            node_id: node.node_id.clone(),
                            kind: node.kind.clone(),
                            action: SinkExecutionAction::WouldPublishMqtt,
                            side_effect_performed: false,
                            preview: Some(json!({
                                "error": error,
                                "preview": preview,
                            })),
                        });
                        json!({
                            "action": "would_publish_mqtt",
                            "preview": preview,
                            "side_effect_error": error,
                            "config": redacted_config,
                        })
                    }
                }
            } else {
                response.sink_results.push(SinkExecutionResult {
                    node_id: node.node_id.clone(),
                    kind: node.kind.clone(),
                    action: SinkExecutionAction::WouldPublishMqtt,
                    side_effect_performed: false,
                    preview: Some(preview.clone()),
                });
                json!({
                    "action": "would_publish_mqtt",
                    "preview": preview,
                    "config": redacted_config,
                    "side_effect_note": "real MQTT publish requires flows:execute and requested_sink_actions including publish_mqtt",
                })
            }
        }
        Some("http_forward") => {
            let preview = build_http_forward_preview(node, &frame);
            if external_sink_action_requested(&response.authorization, "forward_http") {
                match execute_http_forward(state, tenant_id, node, &frame, response.execution_id) {
                    Ok(result) => {
                        response.side_effects_performed = true;
                        response.sink_results.push(SinkExecutionResult {
                            node_id: node.node_id.clone(),
                            kind: node.kind.clone(),
                            action: SinkExecutionAction::WouldForwardHttp,
                            side_effect_performed: true,
                            preview: Some(result.clone()),
                        });
                        json!({
                            "action": "forwarded_http",
                            "preview": preview,
                            "result": result,
                            "config": redacted_config,
                        })
                    }
                    Err(error) => {
                        response.sink_results.push(SinkExecutionResult {
                            node_id: node.node_id.clone(),
                            kind: node.kind.clone(),
                            action: SinkExecutionAction::WouldForwardHttp,
                            side_effect_performed: false,
                            preview: Some(json!({
                                "error": error,
                                "preview": preview,
                            })),
                        });
                        json!({
                            "action": "would_forward_http",
                            "preview": preview,
                            "side_effect_error": error,
                            "config": redacted_config,
                        })
                    }
                }
            } else {
                response.sink_results.push(SinkExecutionResult {
                    node_id: node.node_id.clone(),
                    kind: node.kind.clone(),
                    action: SinkExecutionAction::WouldForwardHttp,
                    side_effect_performed: false,
                    preview: Some(preview.clone()),
                });
                json!({
                    "action": "would_forward_http",
                    "preview": preview,
                    "config": redacted_config,
                    "side_effect_note": "real HTTP forward requires flows:execute and requested_sink_actions including forward_http",
                })
            }
        }
        Some("event_create") => {
            let preview = build_event_preview(node, &frame, response.execution_id);
            response.events_preview.push(preview.clone());
            if sink_action_requested(&response.authorization, "create_event") {
                match create_flow_event(state, node, &frame, response.execution_id) {
                    Ok(event_value) => {
                        response.side_effects_performed = true;
                        response.sink_results.push(SinkExecutionResult {
                            node_id: node.node_id.clone(),
                            kind: node.kind.clone(),
                            action: SinkExecutionAction::WouldCreateEvent,
                            side_effect_performed: true,
                            preview: Some(json!({
                                "created_event": event_value,
                                "requested_action": "create_event",
                            })),
                        });
                        json!({
                            "action": "created_event",
                            "preview": preview,
                            "config": redacted_config,
                        })
                    }
                    Err(error) => {
                        response.sink_results.push(SinkExecutionResult {
                            node_id: node.node_id.clone(),
                            kind: node.kind.clone(),
                            action: SinkExecutionAction::WouldCreateEvent,
                            side_effect_performed: false,
                            preview: Some(json!({
                                "error": error,
                                "preview": preview,
                            })),
                        });
                        json!({
                            "action": "would_create_event",
                            "preview": preview,
                            "side_effect_error": error,
                            "config": redacted_config,
                        })
                    }
                }
            } else {
                response.sink_results.push(SinkExecutionResult {
                    node_id: node.node_id.clone(),
                    kind: node.kind.clone(),
                    action: SinkExecutionAction::WouldCreateEvent,
                    side_effect_performed: false,
                    preview: Some(preview.clone()),
                });
                json!({
                    "action": "would_create_event",
                    "preview": preview,
                    "config": redacted_config,
                })
            }
        }
        Some("command_create") => {
            let preview = json!({
                "command_type": node.config.get("command_type").cloned().unwrap_or_else(|| json!("unknown")),
                "target_entity_id": node.config.get("target_entity_id").cloned(),
                "payload_preview": summarize_payload(&frame.payload),
                "raw_message_id": frame.raw_message_id,
            });
            response.commands_preview.push(preview.clone());
            response.sink_results.push(SinkExecutionResult {
                node_id: node.node_id.clone(),
                kind: node.kind.clone(),
                action: SinkExecutionAction::WouldCreateCommand,
                side_effect_performed: false,
                preview: Some(preview.clone()),
            });
            json!({
                "action": "would_create_command",
                "preview": preview,
                "config": redacted_config,
            })
        }
        Some("dlq") => {
            let preview = json!({
                "failure_stage": node.config.get("failure_stage").cloned().unwrap_or_else(|| json!("unknown")),
                "failure_reason": node.config.get("failure_reason").cloned(),
                "payload_preview": summarize_payload(&frame.payload),
                "raw_message_id": frame.raw_message_id,
            });
            response.dlq_preview.push(preview.clone());
            response.sink_results.push(SinkExecutionResult {
                node_id: node.node_id.clone(),
                kind: node.kind.clone(),
                action: SinkExecutionAction::WouldWriteDlq,
                side_effect_performed: false,
                preview: Some(preview.clone()),
            });
            json!({
                "action": "would_write_dlq",
                "preview": preview,
                "config": redacted_config,
            })
        }
        _ => {
            response.sink_results.push(SinkExecutionResult {
                node_id: node.node_id.clone(),
                kind: node.kind.clone(),
                action: SinkExecutionAction::NoOp,
                side_effect_performed: false,
                preview: Some(json!({
                    "config": redacted_config,
                })),
            });
            json!({
                "action": "no_op",
                "config": redacted_config,
            })
        }
    };

    response.node_results.push(NodeExecutionResult {
        node_id: node.node_id.clone(),
        node_type: node.node_type.clone(),
        kind: node.kind.clone(),
        status: NodeExecutionStatus::Simulated,
        input_summary,
        output_summary: sink_preview,
        error: None,
    });
    Some(frame)
}

fn maybe_record_skipped_sink(
    node: &FlowNodeState,
    response: &mut FlowExecutionResponse,
    skip_reason: Option<&str>,
) {
    if node.node_type != "sink" && node.node_type != "dlq" {
        return;
    }

    response.sink_results.push(SinkExecutionResult {
        node_id: node.node_id.clone(),
        kind: node.kind.clone(),
        action: SinkExecutionAction::NoOp,
        side_effect_performed: false,
        preview: Some(json!({
            "reason": skip_reason.unwrap_or("upstream execution did not continue"),
        })),
    });
}

fn external_sink_action_requested(
    authorization: &FlowExecutionAuthorization,
    action: &str,
) -> bool {
    if !authorization.side_effects_authorized {
        return false;
    }
    // External side effects must be explicitly requested even when allow_side_effects=true.
    authorization
        .requested_sink_actions
        .iter()
        .any(|requested| sink_action_alias_matches(requested, action))
}

fn build_mqtt_publish_preview(node: &FlowNodeState, frame: &ExecutionFrame) -> Value {
    json!({
        "connector_id": node.config.get("connector_id").cloned(),
        "topic": rendered_topic(node, frame).ok(),
        "topic_template": node.config.get("topic_template").cloned(),
        "qos": node.config.get("qos").cloned().unwrap_or_else(|| json!("at_least_once")),
        "retain": node.config.get("retain").cloned().unwrap_or_else(|| json!(false)),
        "payload": summarize_payload(&frame.payload),
    })
}

fn build_http_forward_preview(node: &FlowNodeState, frame: &ExecutionFrame) -> Value {
    json!({
        "connector_id": node.config.get("connector_id").cloned(),
        "endpoint_url": redact_url_value(node.config.get("endpoint_url")),
        "method": node.config.get("method").cloned().unwrap_or_else(|| json!("POST")),
        "payload": summarize_payload(&frame.payload),
    })
}

fn execute_mqtt_publish(
    state: &AppState,
    tenant_id: Uuid,
    node: &FlowNodeState,
    frame: &ExecutionFrame,
    execution_id: Uuid,
) -> Result<Value, String> {
    let connector = execution_connector(
        state,
        tenant_id,
        node,
        "mqtt_publish",
        IngestionConnectorType::Mqtt,
    )?;
    if !connector.enabled {
        return Err("mqtt_publish connector is disabled".to_string());
    }
    if connector.connector_profile == aion_storage::ConnectorProfile::TtnV3 {
        return Err(
            "TTN v3 connectors are subscriber-oriented and are not supported for flow MQTT publish"
                .to_string(),
        );
    }
    let broker_url = connector
        .broker_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "mqtt_publish connector requires broker_url".to_string())?;
    let (host, port) = parse_mqtt_broker_url(broker_url)?;
    let topic = rendered_topic(node, frame)?;
    if topic.contains('+') || topic.contains('#') {
        return Err("mqtt_publish topic must not contain MQTT wildcards".to_string());
    }
    let payload = mqtt_payload_bytes(node, frame)?;
    let qos = mqtt_qos(node.config.get("qos"));
    let qos_label = mqtt_qos_label(&qos);
    let qos_is_at_most_once = matches!(qos, QoS::AtMostOnce);
    let retain = node
        .config
        .get("retain")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let timeout_ms = node
        .config
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(2000)
        .clamp(100, 30_000);

    let client_id = connector
        .client_id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("aion-flow-{}", execution_id));
    let mut options = MqttOptions::new(client_id, host, port);
    options.set_keep_alive(Duration::from_secs(5));
    if let Some(secret_ref_id) = connector.secret_ref_id {
        let secret = state
            .storage
            .get_connector_secret(tenant_id, secret_ref_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| {
                "connector secret_ref_id does not reference an existing secret".to_string()
            })?;
        if secret.secret_type != ConnectorSecretType::MqttBasicAuth {
            return Err("mqtt_publish only supports mqtt_basic_auth connector secrets".to_string());
        }
        if let Some(username) = secret.username.as_deref() {
            options.set_credentials(username, secret.secret_value.as_str());
        } else {
            return Err("mqtt_publish mqtt_basic_auth secret requires username".to_string());
        }
    }

    let (client, mut connection) = MqttClient::new(options, 10);
    client
        .publish(topic.clone(), qos, retain, payload.clone())
        .map_err(|error| format!("mqtt publish enqueue failed: {error}"))?;

    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    let mut observed_publish = false;
    while std::time::Instant::now() < deadline {
        match connection.iter().next() {
            Some(Ok(MqttEvent::Outgoing(Outgoing::Publish(_)))) => {
                observed_publish = true;
                if qos_is_at_most_once {
                    break;
                }
            }
            Some(Ok(MqttEvent::Incoming(Incoming::PubAck(_))))
            | Some(Ok(MqttEvent::Incoming(Incoming::PubComp(_)))) => {
                observed_publish = true;
                break;
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(format!("mqtt publish failed: {error}")),
            None => break,
        }
    }

    let _ = client.disconnect();
    if !observed_publish {
        return Err("mqtt publish did not complete before timeout".to_string());
    }

    let result = json!({
        "published": true,
        "connector_id": connector.id,
        "connector_key": connector.connector_key,
        "topic": topic,
        "qos": qos_label,
        "retain": retain,
        "payload_bytes": payload.len(),
        "timeout_ms": timeout_ms,
    });
    record_flow_side_effect_event(
        state,
        "aion:FlowMqttPublished",
        EventSeverity::Info,
        execution_id,
        node,
        Some(connector.id),
        result.clone(),
    )?;
    Ok(result)
}

fn execute_http_forward(
    state: &AppState,
    tenant_id: Uuid,
    node: &FlowNodeState,
    frame: &ExecutionFrame,
    execution_id: Uuid,
) -> Result<Value, String> {
    let connector = execution_connector(
        state,
        tenant_id,
        node,
        "http_forward",
        IngestionConnectorType::Http,
    )?;
    if !connector.enabled {
        return Err("http_forward connector is disabled".to_string());
    }
    if connector.secret_ref_id.is_some() {
        return Err("http_forward does not use connector secrets in this milestone".to_string());
    }
    let endpoint_url = node
        .config
        .get("endpoint_url")
        .and_then(Value::as_str)
        .or(connector.endpoint.as_deref())
        .or(connector.broker_url.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "http_forward requires endpoint_url or connector endpoint".to_string())?;
    let method = node
        .config
        .get("method")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("POST")
        .to_ascii_uppercase();
    if !matches!(method.as_str(), "POST" | "PUT" | "PATCH") {
        return Err("http_forward method must be POST, PUT, or PATCH".to_string());
    }
    let timeout_ms = node
        .config
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .unwrap_or(3000)
        .clamp(100, 30_000);
    let payload = value_to_payload_bytes(&frame.payload).map_err(|error| error.to_string())?;
    let parsed = parse_http_url(endpoint_url)?;
    let response = perform_http_request(&parsed, &method, &payload, timeout_ms)?;
    let result = json!({
        "forwarded": true,
        "connector_id": connector.id,
        "connector_key": connector.connector_key,
        "endpoint_url": redact_url_string(endpoint_url),
        "method": method,
        "status_code": response.status_code,
        "response_body_preview": response.body_preview,
        "payload_bytes": payload.len(),
        "timeout_ms": timeout_ms,
    });
    record_flow_side_effect_event(
        state,
        "aion:FlowHttpForwarded",
        EventSeverity::Info,
        execution_id,
        node,
        Some(connector.id),
        result.clone(),
    )?;
    Ok(result)
}

fn execution_connector(
    state: &AppState,
    tenant_id: Uuid,
    node: &FlowNodeState,
    action: &str,
    expected_type: IngestionConnectorType,
) -> Result<IngestionConnector, String> {
    let connector_id = uuid_from_config(&node.config, "connector_id")
        .ok_or_else(|| format!("{action} requires config.connector_id"))?;
    let connector = state
        .storage
        .get_ingestion_connector(tenant_id, connector_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!("{action} connector_id does not reference a connector in the execution tenant")
        })?;
    if connector.connector_type != expected_type {
        return Err(format!(
            "{action} requires connector_type {:?}, got {:?}",
            expected_type, connector.connector_type
        ));
    }
    Ok(connector)
}

fn rendered_topic(node: &FlowNodeState, frame: &ExecutionFrame) -> Result<String, String> {
    let template = node
        .config
        .get("topic")
        .or_else(|| node.config.get("topic_template"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "mqtt_publish requires topic or topic_template".to_string())?;
    Ok(render_template(template, &frame.payload))
}

fn mqtt_payload_bytes(node: &FlowNodeState, frame: &ExecutionFrame) -> Result<Vec<u8>, String> {
    if let Some(template) = node.config.get("payload_template").and_then(Value::as_str) {
        return Ok(render_template(template, &frame.payload).into_bytes());
    }
    value_to_payload_bytes(&frame.payload).map_err(|error| error.to_string())
}

fn mqtt_qos(value: Option<&Value>) -> QoS {
    match value
        .and_then(Value::as_str)
        .unwrap_or("at_least_once")
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .as_str()
    {
        "0" | "at_most_once" => QoS::AtMostOnce,
        "2" | "exactly_once" => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}

fn mqtt_qos_label(qos: &QoS) -> &'static str {
    match qos {
        QoS::AtMostOnce => "at_most_once",
        QoS::AtLeastOnce => "at_least_once",
        QoS::ExactlyOnce => "exactly_once",
    }
}

fn parse_mqtt_broker_url(value: &str) -> Result<(String, u16), String> {
    let trimmed = value.trim();
    let without_scheme = trimmed.strip_prefix("mqtt://").ok_or_else(|| {
        format!("unsupported MQTT broker URL '{trimmed}'; expected mqtt://host:port")
    })?;
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    let host_port = host_port.split('@').next_back().unwrap_or(host_port);
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .map_err(|error| format!("invalid MQTT broker port in '{trimmed}': {error}"))?;
            (host.to_string(), port)
        }
        None => (host_port.to_string(), 1883),
    };
    if host.trim().is_empty() {
        return Err(format!("invalid MQTT broker URL '{trimmed}'"));
    }
    Ok((host, port))
}

struct ParsedHttpUrl {
    host: String,
    port: u16,
    path: String,
}

struct HttpForwardResponse {
    status_code: u16,
    body_preview: String,
}

fn parse_http_url(value: &str) -> Result<ParsedHttpUrl, String> {
    let trimmed = value.trim();
    let without_scheme = trimmed.strip_prefix("http://").ok_or_else(|| {
        "http_forward only supports http:// endpoints in this milestone".to_string()
    })?;
    if without_scheme.contains('@') {
        return Err("http_forward endpoint_url must not contain embedded credentials".to_string());
    }
    let (host_port, path) = match without_scheme.split_once('/') {
        Some((host_port, path)) => (host_port, format!("/{path}")),
        None => (without_scheme, "/".to_string()),
    };
    let (host, port) = match host_port.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .map_err(|error| format!("invalid HTTP endpoint port: {error}"))?;
            (host.to_string(), port)
        }
        None => (host_port.to_string(), 80),
    };
    if host.trim().is_empty() {
        return Err("http_forward endpoint host must not be empty".to_string());
    }
    Ok(ParsedHttpUrl { host, port, path })
}

fn perform_http_request(
    endpoint: &ParsedHttpUrl,
    method: &str,
    payload: &[u8],
    timeout_ms: u64,
) -> Result<HttpForwardResponse, String> {
    let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .map_err(|error| format!("http_forward connect failed: {error}"))?;
    let timeout = Some(Duration::from_millis(timeout_ms));
    stream
        .set_read_timeout(timeout)
        .map_err(|error| format!("http_forward set read timeout failed: {error}"))?;
    stream
        .set_write_timeout(timeout)
        .map_err(|error| format!("http_forward set write timeout failed: {error}"))?;
    let request = format!(
        "{method} {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        endpoint.path,
        endpoint.host,
        payload.len()
    );
    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(payload))
        .map_err(|error| format!("http_forward write failed: {error}"))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| format!("http_forward read failed: {error}"))?;
    let response_text = String::from_utf8_lossy(&response);
    let status_code = response_text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    let body = response_text
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .chars()
        .take(512)
        .collect::<String>();
    if !(200..300).contains(&status_code) {
        return Err(format!(
            "http_forward returned non-2xx status {status_code}"
        ));
    }
    Ok(HttpForwardResponse {
        status_code,
        body_preview: body,
    })
}

fn redact_url_value(value: Option<&Value>) -> Value {
    value
        .and_then(Value::as_str)
        .map(redact_url_string)
        .map(Value::String)
        .unwrap_or(Value::Null)
}

fn redact_url_string(value: &str) -> String {
    let mut redacted = value.to_string();
    if let Some((scheme, rest)) = value.split_once("://") {
        if let Some((userinfo, tail)) = rest.split_once('@') {
            let _ = userinfo;
            redacted = format!("{scheme}://<redacted>@{tail}");
        }
    }
    for key in [
        "token",
        "api_key",
        "access_key",
        "secret",
        "password",
        "credential",
    ] {
        redacted = redact_query_param(&redacted, key);
    }
    redacted
}

fn redact_query_param(input: &str, key: &str) -> String {
    let mut output = Vec::new();
    for part in input.split('&') {
        let lower = part.to_ascii_lowercase();
        let prefix = format!("{key}=");
        if lower.contains(&prefix) {
            if let Some((name, _)) = part.split_once('=') {
                output.push(format!("{name}=<redacted>"));
            } else {
                output.push(part.to_string());
            }
        } else {
            output.push(part.to_string());
        }
    }
    output.join("&")
}

fn record_flow_side_effect_event(
    state: &AppState,
    event_type: &str,
    severity: EventSeverity,
    execution_id: Uuid,
    node: &FlowNodeState,
    connector_id: Option<Uuid>,
    result: Value,
) -> Result<(), String> {
    record_event(
        state,
        EventDraft {
            event_type: event_type.to_string(),
            severity,
            source_entity_id: uuid_from_config(&node.config, "source_entity_id"),
            target_entity_id: uuid_from_config(&node.config, "target_entity_id"),
            message: Some(format!(
                "flow side effect executed by node {}",
                node.node_id
            )),
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: node
                .config
                .get("correlation_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            raw_message_id: None,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(json!({
                "source": "flow_execution",
                "execution_id": execution_id,
                "flow_node_id": node.node_id,
                "flow_node_kind": node.kind.clone(),
                "connector_id": connector_id,
                "external_side_effect": true,
                "result": redact_sensitive_json(&result),
            })),
        },
    )
    .map(|_| ())
    .map_err(|error| error.to_string())
}

fn sink_action_requested(authorization: &FlowExecutionAuthorization, action: &str) -> bool {
    if !authorization.side_effects_authorized {
        return false;
    }
    if authorization.requested_sink_actions.is_empty() {
        return true;
    }
    authorization
        .requested_sink_actions
        .iter()
        .any(|requested| sink_action_alias_matches(requested, action))
}

fn sink_action_alias_matches(requested: &str, action: &str) -> bool {
    let normalized = requested.trim().to_ascii_lowercase().replace('-', "_");
    let aliases = match action {
        "store_observation" => &[
            "store_observation",
            "internal_observation_store",
            "would_store_observation",
            "observation_store",
        ][..],
        "create_event" => &[
            "create_event",
            "event_create",
            "would_create_event",
            "event",
        ][..],
        "publish_mqtt" => &["publish_mqtt", "mqtt_publish", "would_publish_mqtt", "mqtt"][..],
        "forward_http" => &["forward_http", "http_forward", "would_forward_http", "http"][..],
        _ => &[action][..],
    };
    aliases.iter().any(|alias| normalized == *alias)
}

fn store_observation_previews(
    state: &AppState,
    tenant_id: Uuid,
    node: &FlowNodeState,
    frame: &ExecutionFrame,
    previews: &[Value],
    execution_id: Uuid,
) -> Result<Vec<Value>, String> {
    let mut stored = Vec::new();
    for preview in previews {
        let observation = observation_from_preview(tenant_id, node, frame, preview, execution_id)?;
        ensure_execution_entity(
            state,
            tenant_id,
            observation.producer_entity_id,
            "producer_entity_id",
        )?;
        ensure_execution_entity(
            state,
            tenant_id,
            observation.feature_of_interest_id,
            "feature_of_interest_id",
        )?;
        let stored_observation = state
            .storage
            .store_observation(observation)
            .map_err(|error| error.to_string())?;
        stored.push(
            serde_json::to_value(stored_observation)
                .map_err(|error| format!("failed to serialize stored observation: {error}"))?,
        );
    }
    Ok(stored)
}

fn ensure_execution_entity(
    state: &AppState,
    tenant_id: Uuid,
    entity_id: Uuid,
    field_name: &str,
) -> Result<(), String> {
    state
        .storage
        .get_entity(tenant_id, entity_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!("{field_name} does not reference an entity in the execution tenant")
        })?;
    Ok(())
}

fn observation_from_preview(
    tenant_id: Uuid,
    node: &FlowNodeState,
    frame: &ExecutionFrame,
    preview: &Value,
    execution_id: Uuid,
) -> Result<Observation, String> {
    let producer_entity_id = uuid_from_config(&node.config, "producer_entity_id")
        .or_else(|| uuid_from_config(&node.config, "source_entity_id"))
        .ok_or_else(|| {
            "producer_entity_id or source_entity_id is required for real internal observation storage".to_string()
        })?;
    let feature_of_interest_id = uuid_from_config(&node.config, "feature_of_interest_id")
        .or_else(|| uuid_from_value(preview.get("feature_of_interest_id")))
        .ok_or_else(|| {
            "feature_of_interest_id is required for real internal observation storage".to_string()
        })?;
    let observed_property = preview
        .get("observed_property")
        .and_then(Value::as_str)
        .or_else(|| node.config.get("observed_property").and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            "observed_property is required for real internal observation storage".to_string()
        })?
        .to_string();
    let value = preview
        .get("value")
        .cloned()
        .or_else(|| infer_single_field_value(&frame.payload))
        .unwrap_or_else(|| frame.payload.clone());
    let value = observation_value_from_json(&value);
    let unit = preview
        .get("unit")
        .and_then(optional_string_from_value)
        .or_else(|| node.config.get("unit").and_then(optional_string_from_value));
    let now = Utc::now();
    let observed_at = preview
        .get("time")
        .and_then(datetime_from_value)
        .or_else(|| node.config.get("observed_at").and_then(datetime_from_value))
        .unwrap_or(now);
    let metadata = json!({
        "source": "flow_execution",
        "execution_id": execution_id,
        "flow_node_id": node.node_id,
        "flow_node_kind": node.kind.clone(),
        "simulated": false,
        "safe_internal_side_effect": true,
        "payload_summary": summarize_payload(&frame.payload),
        "execution_metadata": frame.metadata.clone(),
    });
    Observation::new(
        tenant_id,
        producer_entity_id,
        feature_of_interest_id,
        observed_property,
        value,
        unit,
        observed_at,
        now,
        "flow_execution",
        frame
            .payload_format
            .clone()
            .unwrap_or_else(|| "flow_execution".to_string()),
        frame.raw_message_id,
        json!({
            "simulated_execution": false,
            "safe_internal_side_effect": true,
        }),
        metadata,
    )
    .map_err(|error| error.to_string())
}

fn build_event_preview(node: &FlowNodeState, frame: &ExecutionFrame, execution_id: Uuid) -> Value {
    json!({
        "event_type": node.config.get("event_type").cloned().unwrap_or_else(|| json!("aion:FlowEvent")),
        "severity": node.config.get("severity").cloned().unwrap_or_else(|| json!("info")),
        "message": node
            .config
            .get("message")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("simulated flow event from node {}", node.node_id)),
        "raw_message_id": frame.raw_message_id,
        "execution_id": execution_id,
        "payload_preview": summarize_payload(&frame.payload),
    })
}

fn create_flow_event(
    state: &AppState,
    node: &FlowNodeState,
    frame: &ExecutionFrame,
    execution_id: Uuid,
) -> Result<Value, String> {
    let event_type = node
        .config
        .get("event_type")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("aion:FlowEventCreated")
        .to_string();
    let severity = node
        .config
        .get("severity")
        .and_then(Value::as_str)
        .map(event_severity_from_str)
        .unwrap_or(EventSeverity::Info);
    let message = node
        .config
        .get("message")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| Some(format!("flow execution event from node {}", node.node_id)));
    let metadata = json!({
        "source": "flow_execution",
        "execution_id": execution_id,
        "flow_node_id": node.node_id,
        "flow_node_kind": node.kind.clone(),
        "simulated": false,
        "safe_internal_side_effect": true,
        "payload_summary": summarize_payload(&frame.payload),
        "execution_metadata": frame.metadata.clone(),
        "config": redact_sensitive_json(&node.config),
    });
    let event = record_event(
        state,
        EventDraft {
            event_type,
            severity,
            source_entity_id: uuid_from_config(&node.config, "source_entity_id"),
            target_entity_id: uuid_from_config(&node.config, "target_entity_id"),
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: node
                .config
                .get("correlation_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            raw_message_id: frame.raw_message_id,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(metadata),
        },
    )
    .map_err(|error| error.to_string())?;
    serde_json::to_value(event)
        .map_err(|error| format!("failed to serialize created event: {error}"))
}

fn uuid_from_config(config: &Value, key: &str) -> Option<Uuid> {
    uuid_from_value(config.get(key))
}

fn uuid_from_value(value: Option<&Value>) -> Option<Uuid> {
    value
        .and_then(Value::as_str)
        .and_then(|text| Uuid::parse_str(text.trim()).ok())
}

fn optional_string_from_value(value: &Value) -> Option<String> {
    if value.is_null() {
        None
    } else {
        value.as_str().map(ToOwned::to_owned)
    }
}

fn datetime_from_value(value: &Value) -> Option<DateTime<Utc>> {
    value
        .as_str()
        .and_then(|text| DateTime::parse_from_rfc3339(text.trim()).ok())
        .map(|datetime| datetime.with_timezone(&Utc))
}

fn observation_value_from_json(value: &Value) -> ObservationValue {
    if let Ok(observation_value) = serde_json::from_value::<ObservationValue>(value.clone()) {
        return observation_value;
    }
    if let Some(value) = value.as_f64() {
        ObservationValue::Number { value }
    } else if let Some(value) = value.as_bool() {
        ObservationValue::Bool { value }
    } else if let Some(value) = value.as_str() {
        ObservationValue::Text {
            value: value.to_string(),
        }
    } else {
        ObservationValue::Json {
            value: value.clone(),
        }
    }
}

fn event_severity_from_str(value: &str) -> EventSeverity {
    match value.trim().to_ascii_lowercase().as_str() {
        "debug" => EventSeverity::Debug,
        "warning" | "warn" => EventSeverity::Warning,
        "error" => EventSeverity::Error,
        "critical" => EventSeverity::Critical,
        _ => EventSeverity::Info,
    }
}

fn decode_measurements(
    decoder: &dyn PayloadDecoder,
    frame: &ExecutionFrame,
    node: &FlowNodeState,
    decoder_name: &str,
) -> Result<Vec<Value>, String> {
    let format = frame
        .payload_format
        .as_deref()
        .or(node.kind.as_deref())
        .unwrap_or(decoder_name);
    let input = DecodeInput {
        tenant_id: Uuid::nil(),
        device_key: node
            .config
            .get("device_key")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        format: PayloadFormat::from_str(format)
            .unwrap_or(PayloadFormat::Unknown(format.to_string())),
        content_type: node
            .config
            .get("content_type")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        payload: frame.payload_bytes.clone(),
        received_at: Utc::now(),
        config: node.config.get("mapping").cloned(),
    };

    decoder
        .decode(input)
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|measurement| {
            serde_json::to_value(measurement)
                .map_err(|error| format!("failed to serialize decoded measurement: {error}"))
        })
        .collect()
}

fn build_adjacency(edges: &[FlowEdgeDraft]) -> HashMap<String, Vec<ExecutionEdgeState>> {
    let mut adjacency = HashMap::new();
    for edge in edges {
        adjacency
            .entry(edge.source_node_id.clone())
            .or_insert_with(Vec::new)
            .push(ExecutionEdgeState {
                edge_id: edge.edge_id.clone(),
                label: edge.label.clone(),
                source_node_id: edge.source_node_id.clone(),
                target_node_id: edge.target_node_id.clone(),
                metadata: edge.metadata.clone(),
            });
    }
    adjacency
}

fn build_observation_preview(node: &FlowNodeState, frame: &ExecutionFrame) -> Vec<Value> {
    if !frame.observations_preview.is_empty() {
        return frame
            .observations_preview
            .iter()
            .map(|value| {
                let mut preview = value.clone();
                if let Some(object) = preview.as_object_mut() {
                    object.insert("raw_message_id".to_string(), json!(frame.raw_message_id));
                    object.insert("simulated".to_string(), json!(true));
                }
                preview
            })
            .collect();
    }

    let observed_property = node
        .config
        .get("observed_property")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| infer_single_field_name(&frame.payload));

    let Some(observed_property) = observed_property else {
        return vec![json!({
            "payload_preview": summarize_payload(&frame.payload),
            "raw_message_id": frame.raw_message_id,
            "simulated": true,
        })];
    };

    let value = lookup_field_value(&frame, &observed_property)
        .cloned()
        .or_else(|| infer_single_field_value(&frame.payload))
        .unwrap_or_else(|| frame.payload.clone());

    vec![json!({
        "observed_property": observed_property,
        "value": value,
        "unit": node.config.get("unit").cloned(),
        "feature_of_interest_id": node.config.get("feature_of_interest_id").cloned(),
        "raw_message_id": frame.raw_message_id,
        "simulated": true,
    })]
}

fn apply_simple_mapping(mapping: &Value, payload: &Value) -> Value {
    let Some(mapping_object) = mapping.as_object() else {
        return json!({
            "mapping": mapping,
            "input": payload,
        });
    };

    let mut output = Value::Object(serde_json::Map::new());
    for (target_key, source_value) in mapping_object {
        let mapped = resolve_mapping_value(source_value, payload);
        insert_path_value(&mut output, target_key, mapped);
    }
    output
}

fn resolve_mapping_value(spec: &Value, payload: &Value) -> Value {
    if let Some(source_key) = spec.as_str() {
        return lookup_value_in_payload(payload, source_key)
            .cloned()
            .unwrap_or(Value::Null);
    }

    let Some(object) = spec.as_object() else {
        return spec.clone();
    };

    if let Some(literal) = object.get("literal").or_else(|| object.get("value")) {
        return literal.clone();
    }

    if let Some(template) = object.get("template").and_then(Value::as_str) {
        return Value::String(render_template(template, payload));
    }

    let path = object
        .get("from")
        .or_else(|| object.get("path"))
        .and_then(Value::as_str);
    if let Some(path) = path {
        return lookup_value_in_payload(payload, path)
            .cloned()
            .or_else(|| object.get("default").cloned())
            .unwrap_or(Value::Null);
    }

    spec.clone()
}

fn insert_path_value(output: &mut Value, path: &str, value: Value) {
    let parts = path
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return;
    }

    let mut current = output;
    for part in &parts[..parts.len().saturating_sub(1)] {
        if !current.is_object() {
            *current = Value::Object(serde_json::Map::new());
        }
        let object = current.as_object_mut().expect("object checked above");
        current = object
            .entry((*part).to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
    }

    if !current.is_object() {
        *current = Value::Object(serde_json::Map::new());
    }
    if let Some(last) = parts.last() {
        current
            .as_object_mut()
            .expect("object checked above")
            .insert((*last).to_string(), value);
    }
}

fn render_template(template: &str, payload: &Value) -> String {
    let mut output = String::new();
    let mut remaining = template;
    while let Some(start) = remaining.find('{') {
        output.push_str(&remaining[..start]);
        let after_start = &remaining[start + 1..];
        let Some(end) = after_start.find('}') else {
            output.push_str(&remaining[start..]);
            return output;
        };
        let key = after_start[..end].trim();
        let replacement = lookup_value_in_payload(payload, key)
            .map(value_to_template_string)
            .unwrap_or_default();
        output.push_str(&replacement);
        remaining = &after_start[end + 1..];
    }
    output.push_str(remaining);
    output
}

fn value_to_template_string(value: &Value) -> String {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn evaluate_condition_from_config(config: &Value, frame: &ExecutionFrame) -> Result<bool, String> {
    evaluate_condition_object(config, frame)
}

fn edge_condition(metadata: &Option<Value>) -> Option<Value> {
    let metadata = metadata.as_ref()?;
    metadata
        .get("condition")
        .or_else(|| metadata.get("when"))
        .or_else(|| metadata.get("filter"))
        .cloned()
}

fn evaluate_edge_condition(
    condition: Option<&Value>,
    frame: &ExecutionFrame,
) -> Result<bool, String> {
    let Some(condition) = condition else {
        return Ok(true);
    };
    if let Some(value) = condition.as_bool() {
        return Ok(value);
    }
    if let Some(text) = condition.as_str() {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(true);
        }
        let parsed = serde_json::from_str::<Value>(trimmed)
            .map_err(|error| format!("invalid edge condition JSON: {error}"))?;
        return evaluate_condition_object(&parsed, frame);
    }
    evaluate_condition_object(condition, frame)
}

fn evaluate_condition_object(condition: &Value, frame: &ExecutionFrame) -> Result<bool, String> {
    if let Some(all) = condition.get("all") {
        let items = all
            .as_array()
            .ok_or_else(|| "condition all must be an array".to_string())?;
        for item in items {
            if !evaluate_condition_object(item, frame)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    if let Some(any) = condition.get("any") {
        let items = any
            .as_array()
            .ok_or_else(|| "condition any must be an array".to_string())?;
        for item in items {
            if evaluate_condition_object(item, frame)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }

    if let Some(not) = condition.get("not") {
        return Ok(!evaluate_condition_object(not, frame)?);
    }

    let field = condition
        .get("field")
        .or_else(|| condition.get("path"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "condition field is required".to_string())?;
    let operator = condition
        .get("operator")
        .or_else(|| condition.get("op"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("eq");
    let actual = lookup_field_value(frame, field);
    evaluate_operator(actual, operator, condition.get("value"))
}

fn evaluate_operator(
    actual: Option<&Value>,
    operator: &str,
    expected: Option<&Value>,
) -> Result<bool, String> {
    match operator {
        "exists" => Ok(actual.is_some()),
        "not_exists" | "missing" => Ok(actual.is_none()),
        "eq" => Ok(actual == expected),
        "neq" => Ok(actual != expected),
        "gt" | "gte" | "lt" | "lte" => compare_numeric(actual, expected, operator),
        "between" => compare_between(actual, expected),
        "in" => value_in(actual, expected),
        "contains" => contains_value(actual, expected),
        other => Err(format!("unsupported operator '{other}'")),
    }
}

fn compare_numeric(
    actual: Option<&Value>,
    expected: Option<&Value>,
    operator: &str,
) -> Result<bool, String> {
    let actual = actual
        .and_then(json_number)
        .ok_or_else(|| "actual value is not numeric".to_string())?;
    let expected = expected
        .and_then(json_number)
        .ok_or_else(|| "expected value is not numeric".to_string())?;
    Ok(match operator {
        "gt" => actual > expected,
        "gte" => actual >= expected,
        "lt" => actual < expected,
        "lte" => actual <= expected,
        _ => false,
    })
}

fn compare_between(actual: Option<&Value>, expected: Option<&Value>) -> Result<bool, String> {
    let actual = actual
        .and_then(json_number)
        .ok_or_else(|| "actual value is not numeric".to_string())?;
    let Some(expected) = expected else {
        return Err("between requires a value".to_string());
    };

    let (min, max, inclusive) = if let Some(values) = expected.as_array() {
        if values.len() != 2 {
            return Err("between array value must contain exactly two items".to_string());
        }
        let min =
            json_number(&values[0]).ok_or_else(|| "between min is not numeric".to_string())?;
        let max =
            json_number(&values[1]).ok_or_else(|| "between max is not numeric".to_string())?;
        (min, max, true)
    } else if let Some(object) = expected.as_object() {
        let min = object
            .get("min")
            .and_then(json_number)
            .ok_or_else(|| "between min is required and must be numeric".to_string())?;
        let max = object
            .get("max")
            .and_then(json_number)
            .ok_or_else(|| "between max is required and must be numeric".to_string())?;
        let inclusive = object
            .get("inclusive")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        (min, max, inclusive)
    } else {
        return Err("between value must be an array or object".to_string());
    };

    if inclusive {
        Ok(actual >= min && actual <= max)
    } else {
        Ok(actual > min && actual < max)
    }
}

fn value_in(actual: Option<&Value>, expected: Option<&Value>) -> Result<bool, String> {
    let Some(actual) = actual else {
        return Ok(false);
    };
    let Some(values) = expected.and_then(Value::as_array) else {
        return Err("in requires an array value".to_string());
    };
    Ok(values.iter().any(|value| value == actual))
}

fn contains_value(actual: Option<&Value>, expected: Option<&Value>) -> Result<bool, String> {
    let Some(actual) = actual else {
        return Ok(false);
    };
    let Some(expected) = expected else {
        return Err("contains requires a value".to_string());
    };
    if let (Some(actual), Some(expected)) = (actual.as_str(), expected.as_str()) {
        return Ok(actual.contains(expected));
    }
    if let Some(values) = actual.as_array() {
        return Ok(values.iter().any(|value| value == expected));
    }
    if let Some(object) = actual.as_object() {
        if let Some(expected) = expected.as_str() {
            return Ok(object.contains_key(expected));
        }
    }
    Err("contains is only supported for strings, arrays, and object keys".to_string())
}

fn lookup_field_value<'a>(frame: &'a ExecutionFrame, field: &str) -> Option<&'a Value> {
    lookup_value_in_payload(&frame.payload, field).or_else(|| {
        frame.observations_preview.iter().find_map(|observation| {
            let observed_property = observation
                .get("observed_property")
                .and_then(Value::as_str)?;
            if observed_property == field || observed_property.rsplit(':').next() == Some(field) {
                observation.get("value")
            } else {
                None
            }
        })
    })
}

fn lookup_value_in_payload<'a>(payload: &'a Value, field: &str) -> Option<&'a Value> {
    let mut current = payload;
    for segment in field.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn parse_jsonish_value(value: Option<&Value>) -> Result<Option<Value>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(text) = value.as_str() {
        serde_json::from_str(text)
            .map(Some)
            .map_err(|error| format!("invalid JSON config value: {error}"))
    } else {
        Ok(Some(value.clone()))
    }
}

fn frame_summary(frame: &ExecutionFrame) -> Value {
    json!({
        "payload_format": frame.payload_format.clone(),
        "raw_message_id": frame.raw_message_id,
        "payload_preview": summarize_payload(&frame.payload),
        "observations_preview_count": frame.observations_preview.len(),
        "metadata": frame.metadata.clone(),
    })
}

fn summarize_payload(payload: &Value) -> Value {
    match payload {
        Value::Object(map) => json!({
            "kind": "object",
            "keys": map.keys().cloned().collect::<Vec<_>>(),
        }),
        Value::Array(values) => json!({
            "kind": "array",
            "length": values.len(),
        }),
        Value::String(value) => json!({
            "kind": "string",
            "value": value,
        }),
        Value::Number(value) => json!({
            "kind": "number",
            "value": value,
        }),
        Value::Bool(value) => json!({
            "kind": "bool",
            "value": value,
        }),
        Value::Null => json!({
            "kind": "null",
        }),
    }
}

fn value_to_payload_bytes(value: &Value) -> Result<Vec<u8>, ApiError> {
    match value {
        Value::String(text) => Ok(text.as_bytes().to_vec()),
        _ => serde_json::to_vec(value)
            .map_err(|error| ApiError::bad_request(format!("invalid execution payload: {error}"))),
    }
}

fn raw_payload_value(payload: &[u8]) -> Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        String::from_utf8(payload.to_vec())
            .map(Value::String)
            .unwrap_or_else(|_| json!({"encoding": "binary", "byte_length": payload.len()}))
    })
}

fn config_kind(config: &Value) -> Option<String> {
    config
        .get("kind")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn infer_single_field_name(payload: &Value) -> Option<String> {
    let object = payload.as_object()?;
    if object.len() == 1 {
        object.keys().next().cloned()
    } else {
        None
    }
}

fn infer_single_field_value(payload: &Value) -> Option<Value> {
    let object = payload.as_object()?;
    if object.len() == 1 {
        object.values().next().cloned()
    } else {
        None
    }
}

fn json_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
}

fn default_execution_mode() -> String {
    "simulate".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluates_basic_filter_operators() {
        let frame = ExecutionFrame {
            payload: json!({"temperature": 32, "labels": ["a", "b"], "state": "ok"}),
            payload_bytes: br#"{"temperature":32}"#.to_vec(),
            payload_format: Some("application/json".to_string()),
            raw_message_id: None,
            observations_preview: Vec::new(),
            metadata: json!({}),
        };

        assert!(evaluate_condition_object(
            &json!({"field": "temperature", "operator": "gt", "value": 30}),
            &frame
        )
        .unwrap());
        assert!(evaluate_condition_object(
            &json!({"field": "state", "operator": "contains", "value": "o"}),
            &frame
        )
        .unwrap());
        assert!(evaluate_condition_object(
            &json!({"field": "labels", "operator": "contains", "value": "b"}),
            &frame
        )
        .unwrap());
        assert!(evaluate_condition_object(
            &json!({"field": "temperature", "operator": "exists"}),
            &frame
        )
        .unwrap());
    }

    #[test]
    fn applies_nested_mapping_templates_and_defaults() {
        let payload = json!({
            "device": {"id": "sensor-01"},
            "temperature": 31.5
        });
        let mapped = apply_simple_mapping(
            &json!({
                "entity.id": "device.id",
                "reading.value": {"from": "temperature"},
                "reading.unit": {"default": "Cel", "from": "missing.unit"},
                "topic": {"template": "devices/{device.id}/temperature"},
                "literal_value": {"literal": 42}
            }),
            &payload,
        );

        assert_eq!(mapped["entity"]["id"], "sensor-01");
        assert_eq!(mapped["reading"]["value"], 31.5);
        assert_eq!(mapped["reading"]["unit"], "Cel");
        assert_eq!(mapped["topic"], "devices/sensor-01/temperature");
        assert_eq!(mapped["literal_value"], 42);
    }

    #[test]
    fn evaluates_compound_rule_conditions_and_edge_conditions() {
        let frame = ExecutionFrame {
            payload: json!({
                "temperature": 32,
                "humidity": 35,
                "state": "critical",
                "labels": ["field", "pump"]
            }),
            payload_bytes: br#"{"temperature":32}"#.to_vec(),
            payload_format: Some("application/json".to_string()),
            raw_message_id: None,
            observations_preview: Vec::new(),
            metadata: json!({}),
        };

        assert!(evaluate_condition_object(
            &json!({
                "all": [
                    {"field": "temperature", "operator": "between", "value": [30, 40]},
                    {"field": "state", "operator": "in", "value": ["critical", "warning"]},
                    {"field": "labels", "operator": "contains", "value": "pump"}
                ]
            }),
            &frame
        )
        .unwrap());

        assert!(evaluate_condition_object(
            &json!({"not": {"field": "humidity", "operator": "gt", "value": 80}}),
            &frame
        )
        .unwrap());

        assert!(evaluate_edge_condition(
            Some(&json!({"field": "temperature", "operator": "gte", "value": 32})),
            &frame
        )
        .unwrap());
    }

    #[test]
    fn side_effect_action_aliases_match_internal_sink_actions() {
        assert!(sink_action_alias_matches(
            "internal_observation_store",
            "store_observation"
        ));
        assert!(sink_action_alias_matches(
            "would_create_event",
            "create_event"
        ));
        assert!(!sink_action_alias_matches(
            "mqtt_publish",
            "store_observation"
        ));
    }

    #[test]
    fn converts_plain_json_values_to_observation_values() {
        assert_eq!(
            observation_value_from_json(&json!(12.5)),
            ObservationValue::Number { value: 12.5 }
        );
        assert_eq!(
            observation_value_from_json(&json!(true)),
            ObservationValue::Bool { value: true }
        );
        assert_eq!(
            observation_value_from_json(&json!("ok")),
            ObservationValue::Text {
                value: "ok".to_string()
            }
        );
    }
}
