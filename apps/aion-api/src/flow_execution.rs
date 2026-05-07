use crate::{
    error::ApiError,
    flow_support::{
        analyze_flow, redact_sensitive_json, FlowEdgeDraft, FlowNodeDraft, FlowValidationIssue,
    },
    AppState,
};
use aion_payload::{
    CanonicalJsonDecoder, DecodeInput, PayloadDecoder, PayloadFormat, SenMlJsonDecoder,
    UltraLightDecoder,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct FlowExecutionRequest {
    pub sample_payload: Option<Value>,
    pub raw_message_id: Option<Uuid>,
    pub payload_format: Option<String>,
    pub metadata: Option<Value>,
    #[serde(default = "default_execution_mode")]
    pub mode: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FlowExecutionResponse {
    pub flow_id: Option<Uuid>,
    pub flow_key: Option<String>,
    pub execution_id: Uuid,
    pub simulated: bool,
    pub side_effects_performed: bool,
    pub valid: bool,
    pub validation_issues: Vec<FlowValidationIssue>,
    pub node_results: Vec<NodeExecutionResult>,
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

pub(crate) fn execute_flow(
    state: &AppState,
    tenant_id: Uuid,
    flow_id: Option<Uuid>,
    flow_key: Option<String>,
    nodes: &[FlowNodeDraft],
    edges: &[FlowEdgeDraft],
    request: &FlowExecutionRequest,
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
        validation_issues: analysis.validation_issues.clone(),
        node_results: Vec::new(),
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

    let mut visit_state = HashSet::new();
    for source_node_id in source_nodes {
        execute_node_path(
            &source_node_id,
            &node_map,
            &adjacency,
            Some(root_frame.clone()),
            None,
            &mut response,
            &mut visit_state,
        );
    }

    response.completed_at = Utc::now();
    Ok(response)
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
    node_id: &str,
    node_map: &HashMap<String, FlowNodeState>,
    adjacency: &HashMap<String, Vec<String>>,
    frame: Option<ExecutionFrame>,
    skip_reason: Option<&str>,
    response: &mut FlowExecutionResponse,
    visit_state: &mut HashSet<(String, bool)>,
) {
    let visit_key = (node_id.to_string(), frame.is_some());
    if !visit_state.insert(visit_key) {
        return;
    }

    let Some(node) = node_map.get(node_id) else {
        return;
    };

    let next = match frame {
        Some(frame) => execute_single_node(node, frame, response),
        None => {
            response.node_results.push(NodeExecutionResult {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                kind: node.kind.clone(),
                status: NodeExecutionStatus::Skipped,
                input_summary: json!({
                    "reason": skip_reason.unwrap_or("upstream node did not produce output"),
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
    for child in children {
        execute_node_path(
            &child,
            node_map,
            adjacency,
            next.clone(),
            if next.is_none() {
                Some(skip_reason.unwrap_or("upstream execution did not continue"))
            } else {
                None
            },
            response,
            visit_state,
        );
    }
}

fn execute_single_node(
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
                    "payload_format": frame.payload_format,
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
        "sink" | "dlq" => execute_sink_node(node, frame, response, input_summary),
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
                    "payload_format": frame.payload_format,
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
        Some("raw_message_store") => {
            let preview = json!({
                "payload": frame.payload,
                "payload_format": frame.payload_format,
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
            let preview = json!({
                "topic_template": node.config.get("topic_template").cloned(),
                "payload": summarize_payload(&frame.payload),
            });
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
            })
        }
        Some("http_forward") => {
            let preview = json!({
                "endpoint_url": redacted_config.get("endpoint_url").cloned(),
                "method": node.config.get("method").cloned().unwrap_or_else(|| json!("POST")),
                "payload": summarize_payload(&frame.payload),
            });
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
            })
        }
        Some("event_create") => {
            let preview = json!({
                "event_type": node.config.get("event_type").cloned().unwrap_or_else(|| json!("aion:FlowEvent")),
                "severity": node.config.get("severity").cloned().unwrap_or_else(|| json!("info")),
                "message": format!("simulated flow event from node {}", node.node_id),
                "raw_message_id": frame.raw_message_id,
                "payload_preview": summarize_payload(&frame.payload),
            });
            response.events_preview.push(preview.clone());
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

fn build_adjacency(edges: &[FlowEdgeDraft]) -> HashMap<String, Vec<String>> {
    let mut adjacency = HashMap::new();
    for edge in edges {
        adjacency
            .entry(edge.source_node_id.clone())
            .or_insert_with(Vec::new)
            .push(edge.target_node_id.clone());
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
    let mut output = serde_json::Map::new();
    for (target_key, source_value) in mapping_object {
        if let Some(source_key) = source_value.as_str() {
            output.insert(
                target_key.clone(),
                lookup_value_in_payload(payload, source_key)
                    .cloned()
                    .unwrap_or(Value::Null),
            );
        } else {
            output.insert(target_key.clone(), source_value.clone());
        }
    }
    Value::Object(output)
}

fn evaluate_condition_from_config(config: &Value, frame: &ExecutionFrame) -> Result<bool, String> {
    evaluate_condition_object(config, frame)
}

fn evaluate_condition_object(condition: &Value, frame: &ExecutionFrame) -> Result<bool, String> {
    let field = condition
        .get("field")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "condition field is required".to_string())?;
    let operator = condition
        .get("operator")
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
        "eq" => Ok(actual == expected),
        "neq" => Ok(actual != expected),
        "gt" | "gte" | "lt" | "lte" => compare_numeric(actual, expected, operator),
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
    Err("contains is only supported for strings and arrays".to_string())
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
        "payload_format": frame.payload_format,
        "raw_message_id": frame.raw_message_id,
        "payload_preview": summarize_payload(&frame.payload),
        "observations_preview_count": frame.observations_preview.len(),
        "metadata": frame.metadata,
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
}
