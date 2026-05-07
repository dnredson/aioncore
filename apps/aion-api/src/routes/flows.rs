use crate::{
    auth::{
        is_admin_all, principal_tenant_id, principal_tenant_or_default, require_any_scope,
        require_scope, require_scope_for_write, tenant_for_created_resource, AuthContext,
    },
    error::ApiError,
    flow_execution::{execute_flow, FlowExecutionRequest, FlowExecutionResponse},
    flow_support::{
        analyze_flow, FlowAnalysis, FlowEdgeDraft, FlowNodeDraft, FlowNodePlan, FlowPlannedSink,
        FlowReferencedConnector, FlowValidationIssue,
    },
    record_event, require_same_tenant_for_target_flow, state_for_tenant, AppState, AuthMode,
    EventDraft,
};
use aion_event::EventSeverity;
use aion_flow::{
    validate_nodes_and_edges, Flow, FlowEdge, FlowError, FlowNode, FlowNodePosition, FlowNodeType,
};
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::str::FromStr;
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/flows", post(create_flow).get(list_flows))
        .route("/flows/validate", post(validate_proposed_flow))
        .route("/flows/dry-run", post(dry_run_proposed_flow))
        .route("/flows/execute", post(execute_proposed_flow))
        .route(
            "/flows/:flow_id",
            get(get_flow).patch(update_flow).delete(delete_flow),
        )
        .route("/flows/:flow_id/validation", get(validate_stored_flow))
        .route("/flows/:flow_id/dry-run", post(dry_run_stored_flow))
        .route("/flows/:flow_id/execute", post(execute_stored_flow))
        .route("/flows/:flow_id/enable", put(enable_flow))
        .route("/flows/:flow_id/disable", put(disable_flow))
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateFlowRequest {
    pub flow_key: String,
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub nodes: Vec<FlowNodeRequest>,
    #[serde(default)]
    pub edges: Vec<FlowEdgeRequest>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateFlowRequest {
    pub flow_key: Option<String>,
    pub name: Option<String>,
    pub description: Option<Option<String>>,
    pub enabled: Option<bool>,
    pub nodes: Option<Vec<FlowNodeRequest>>,
    pub edges: Option<Vec<FlowEdgeRequest>>,
    pub metadata: Option<Option<Value>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FlowNodeRequest {
    pub node_id: String,
    pub node_type: String,
    pub name: Option<String>,
    pub config: Value,
    pub position: Option<FlowNodePositionRequest>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FlowNodePositionRequest {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FlowEdgeRequest {
    pub edge_id: String,
    pub source_node_id: String,
    pub target_node_id: String,
    pub label: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProposedFlowRequest {
    pub flow_key: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub nodes: Vec<ProposedFlowNodeRequest>,
    #[serde(default)]
    pub edges: Vec<ProposedFlowEdgeRequest>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProposedFlowNodeRequest {
    pub node_id: String,
    pub node_type: String,
    pub name: Option<String>,
    #[serde(default = "default_json_object")]
    pub config: Value,
    pub position: Option<FlowNodePositionRequest>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProposedFlowEdgeRequest {
    pub edge_id: Option<String>,
    pub source_node_id: String,
    pub target_node_id: String,
    pub label: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DryRunRequest {
    pub flow_key: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub nodes: Vec<ProposedFlowNodeRequest>,
    #[serde(default)]
    pub edges: Vec<ProposedFlowEdgeRequest>,
    pub sample_payload: Option<Value>,
    pub payload_format: Option<String>,
    pub source_node_id: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct StoredDryRunRequest {
    pub sample_payload: Option<Value>,
    pub payload_format: Option<String>,
    pub source_node_id: Option<String>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProposedFlowExecutionRequest {
    pub flow_key: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub nodes: Vec<ProposedFlowNodeRequest>,
    #[serde(default)]
    pub edges: Vec<ProposedFlowEdgeRequest>,
    #[serde(flatten)]
    pub execution: FlowExecutionRequest,
}

#[derive(Debug, Serialize)]
pub(crate) struct FlowValidationResponse {
    pub flow_id: Option<Uuid>,
    pub flow_key: Option<String>,
    pub valid: bool,
    pub validation_issues: Vec<FlowValidationIssue>,
    pub node_inventory: Vec<FlowNodePlan>,
    pub referenced_connectors: Vec<FlowReferencedConnector>,
    pub planned_sinks: Vec<FlowPlannedSink>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FlowDryRunResponse {
    pub execution_supported: bool,
    pub simulated: bool,
    pub flow_id: Option<Uuid>,
    pub flow_key: Option<String>,
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
    pub side_effects_performed: bool,
    pub sample_payload_provided: bool,
    pub payload_format: Option<String>,
    pub source_node_id: Option<String>,
}

async fn create_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<CreateFlowRequest>,
) -> Result<(StatusCode, Json<Flow>), ApiError> {
    require_scope_for_write(&state, &auth, "/flows", "flows:write")?;
    let tenant_id = tenant_for_created_resource(&state, &auth)?;
    let scoped_state = state_for_tenant(&state, tenant_id);
    let flow = Flow::new(
        scoped_state.tenant_id,
        request.flow_key,
        request.name,
        request.description,
        request.enabled,
        build_flow_nodes(request.nodes)?,
        build_flow_edges(request.edges),
        request.metadata,
        Utc::now(),
    )
    .map_err(map_flow_error)?;

    let flow = scoped_state.storage.create_flow(flow)?;
    record_flow_event(
        &scoped_state,
        "aion:FlowCreated",
        EventSeverity::Info,
        &flow,
        Some("flow created".to_string()),
    )?;
    Ok((StatusCode::CREATED, Json(flow)))
}

async fn validate_proposed_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<ProposedFlowRequest>,
) -> Result<Json<FlowValidationResponse>, ApiError> {
    require_any_scope(
        &state,
        &auth,
        "/flows/validate",
        &["flows:read", "flows:write"],
    )?;
    let tenant_id = principal_tenant_or_default(&state, &auth)?;
    let _ = (
        request.name.as_ref(),
        request.description.as_ref(),
        request.enabled,
        request.metadata.as_ref(),
    );
    let analysis = analyze_flow(
        &state_for_tenant(&state, tenant_id),
        tenant_id,
        &draft_nodes_from_requests(&request.nodes),
        &draft_edges_from_requests(&request.edges),
        None,
    )?;

    Ok(Json(validation_response(None, request.flow_key, analysis)))
}

async fn dry_run_proposed_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<DryRunRequest>,
) -> Result<Json<FlowDryRunResponse>, ApiError> {
    require_scope(&state, &auth, "/flows/dry-run", "flows:read")?;
    let tenant_id = principal_tenant_or_default(&state, &auth)?;
    let _ = (
        request.name.as_ref(),
        request.description.as_ref(),
        request.enabled,
        request.metadata.as_ref(),
    );
    let analysis = analyze_flow(
        &state_for_tenant(&state, tenant_id),
        tenant_id,
        &draft_nodes_from_requests(&request.nodes),
        &draft_edges_from_requests(&request.edges),
        request.source_node_id.as_deref(),
    )?;

    Ok(Json(dry_run_response(
        None,
        request.flow_key,
        request.sample_payload.is_some(),
        request.payload_format,
        request.source_node_id,
        analysis,
    )))
}

async fn execute_proposed_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<ProposedFlowExecutionRequest>,
) -> Result<Json<FlowExecutionResponse>, ApiError> {
    require_scope(&state, &auth, "/flows/execute", "flows:read")?;
    let tenant_id = principal_tenant_or_default(&state, &auth)?;
    let _ = (
        request.name.as_ref(),
        request.description.as_ref(),
        request.enabled,
    );

    Ok(Json(execute_flow(
        &state_for_tenant(&state, tenant_id),
        tenant_id,
        None,
        request.flow_key,
        &draft_nodes_from_requests(&request.nodes),
        &draft_edges_from_requests(&request.edges),
        &request.execution,
    )?))
}

async fn list_flows(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<Flow>>, ApiError> {
    require_scope(&state, &auth, "/flows", "flows:read")?;

    let flows = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_flows(state.tenant_id)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_flows()?
    } else {
        state.storage.list_flows(principal_tenant_id(&auth)?)?
    };

    Ok(Json(flows))
}

async fn get_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(flow_id): Path<Uuid>,
) -> Result<Json<Flow>, ApiError> {
    require_scope(&state, &auth, "/flows/:flow_id", "flows:read")?;
    Ok(Json(resolve_flow_for_read(
        &state,
        &auth,
        "/flows/:flow_id",
        flow_id,
    )?))
}

async fn validate_stored_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(flow_id): Path<Uuid>,
) -> Result<Json<FlowValidationResponse>, ApiError> {
    require_scope(&state, &auth, "/flows/:flow_id/validation", "flows:read")?;
    let flow = resolve_flow_for_read(&state, &auth, "/flows/:flow_id/validation", flow_id)?;
    let analysis = analyze_flow(
        &state_for_tenant(&state, flow.tenant_id),
        flow.tenant_id,
        &draft_nodes_from_flow(&flow),
        &draft_edges_from_flow(&flow),
        None,
    )?;

    Ok(Json(validation_response(
        Some(flow.id),
        Some(flow.flow_key),
        analysis,
    )))
}

async fn dry_run_stored_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(flow_id): Path<Uuid>,
    Json(request): Json<StoredDryRunRequest>,
) -> Result<Json<FlowDryRunResponse>, ApiError> {
    require_scope(&state, &auth, "/flows/:flow_id/dry-run", "flows:read")?;
    let flow = resolve_flow_for_read(&state, &auth, "/flows/:flow_id/dry-run", flow_id)?;
    let _ = request.metadata.as_ref();
    let analysis = analyze_flow(
        &state_for_tenant(&state, flow.tenant_id),
        flow.tenant_id,
        &draft_nodes_from_flow(&flow),
        &draft_edges_from_flow(&flow),
        request.source_node_id.as_deref(),
    )?;

    Ok(Json(dry_run_response(
        Some(flow.id),
        Some(flow.flow_key),
        request.sample_payload.is_some(),
        request.payload_format,
        request.source_node_id,
        analysis,
    )))
}

async fn execute_stored_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(flow_id): Path<Uuid>,
    Json(request): Json<FlowExecutionRequest>,
) -> Result<Json<FlowExecutionResponse>, ApiError> {
    require_scope(&state, &auth, "/flows/:flow_id/execute", "flows:read")?;
    let flow = resolve_flow_for_read(&state, &auth, "/flows/:flow_id/execute", flow_id)?;

    Ok(Json(execute_flow(
        &state_for_tenant(&state, flow.tenant_id),
        flow.tenant_id,
        Some(flow.id),
        Some(flow.flow_key.clone()),
        &draft_nodes_from_flow(&flow),
        &draft_edges_from_flow(&flow),
        &request,
    )?))
}

async fn update_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(flow_id): Path<Uuid>,
    Json(request): Json<UpdateFlowRequest>,
) -> Result<Json<Flow>, ApiError> {
    require_scope_for_write(&state, &auth, "/flows/:flow_id", "flows:write")?;
    let mut flow = require_same_tenant_for_target_flow(&state, &auth, "/flows/:flow_id", flow_id)?;

    if let Some(flow_key) = request.flow_key {
        flow.flow_key = flow_key;
    }
    if let Some(name) = request.name {
        flow.name = name;
    }
    if let Some(description) = request.description {
        flow.description = description;
    }
    if let Some(enabled) = request.enabled {
        flow.enabled = enabled;
    }
    if let Some(nodes) = request.nodes {
        flow.nodes = build_flow_nodes(nodes)?;
    }
    if let Some(edges) = request.edges {
        flow.edges = build_flow_edges(edges);
    }
    if let Some(metadata) = request.metadata {
        flow.metadata = metadata;
    }

    validate_nodes_and_edges(&flow.nodes, &flow.edges).map_err(map_flow_error)?;
    flow.updated_at = Utc::now();

    let scoped_state = state_for_tenant(&state, flow.tenant_id);
    let flow = scoped_state.storage.update_flow(flow)?;
    record_flow_event(
        &scoped_state,
        "aion:FlowUpdated",
        EventSeverity::Info,
        &flow,
        Some("flow updated".to_string()),
    )?;
    Ok(Json(flow))
}

async fn enable_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(flow_id): Path<Uuid>,
) -> Result<Json<Flow>, ApiError> {
    require_scope_for_write(&state, &auth, "/flows/:flow_id/enable", "flows:write")?;
    set_flow_enabled(state, auth, flow_id, true).await
}

async fn disable_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(flow_id): Path<Uuid>,
) -> Result<Json<Flow>, ApiError> {
    require_scope_for_write(&state, &auth, "/flows/:flow_id/disable", "flows:write")?;
    set_flow_enabled(state, auth, flow_id, false).await
}

async fn delete_flow(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(flow_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_scope_for_write(&state, &auth, "/flows/:flow_id", "flows:write")?;
    let flow = require_same_tenant_for_target_flow(&state, &auth, "/flows/:flow_id", flow_id)?;
    let scoped_state = state_for_tenant(&state, flow.tenant_id);
    scoped_state
        .storage
        .delete_flow(scoped_state.tenant_id, flow.id)?;
    record_flow_event(
        &scoped_state,
        "aion:FlowDeleted",
        EventSeverity::Info,
        &flow,
        Some("flow deleted".to_string()),
    )?;
    Ok(StatusCode::NO_CONTENT)
}

async fn set_flow_enabled(
    state: AppState,
    auth: AuthContext,
    flow_id: Uuid,
    enabled: bool,
) -> Result<Json<Flow>, ApiError> {
    let mut flow = require_same_tenant_for_target_flow(
        &state,
        &auth,
        if enabled {
            "/flows/:flow_id/enable"
        } else {
            "/flows/:flow_id/disable"
        },
        flow_id,
    )?;
    flow.set_enabled(enabled, Utc::now());
    let scoped_state = state_for_tenant(&state, flow.tenant_id);
    let flow = scoped_state.storage.update_flow(flow)?;
    record_flow_event(
        &scoped_state,
        if enabled {
            "aion:FlowEnabled"
        } else {
            "aion:FlowDisabled"
        },
        EventSeverity::Info,
        &flow,
        Some(if enabled {
            "flow enabled".to_string()
        } else {
            "flow disabled".to_string()
        }),
    )?;
    Ok(Json(flow))
}

fn build_flow_nodes(nodes: Vec<FlowNodeRequest>) -> Result<Vec<FlowNode>, ApiError> {
    nodes
        .into_iter()
        .map(|node| {
            Ok(FlowNode {
                node_id: node.node_id,
                node_type: FlowNodeType::from_str(&node.node_type).map_err(map_flow_error)?,
                name: node.name,
                config: node.config,
                position: node.position.map(|position| FlowNodePosition {
                    x: position.x,
                    y: position.y,
                }),
            })
        })
        .collect()
}

fn build_flow_edges(edges: Vec<FlowEdgeRequest>) -> Vec<FlowEdge> {
    edges
        .into_iter()
        .map(|edge| FlowEdge {
            edge_id: edge.edge_id,
            source_node_id: edge.source_node_id,
            target_node_id: edge.target_node_id,
            label: edge.label,
            metadata: edge.metadata,
        })
        .collect()
}

fn draft_nodes_from_requests(nodes: &[ProposedFlowNodeRequest]) -> Vec<FlowNodeDraft> {
    nodes
        .iter()
        .map(|node| {
            let _ = &node.position;
            FlowNodeDraft {
                node_id: node.node_id.clone(),
                node_type: node.node_type.clone(),
                name: node.name.clone(),
                config: node.config.clone(),
            }
        })
        .collect()
}

fn draft_edges_from_requests(edges: &[ProposedFlowEdgeRequest]) -> Vec<FlowEdgeDraft> {
    edges
        .iter()
        .map(|edge| {
            let _ = (&edge.label, &edge.metadata);
            FlowEdgeDraft {
                edge_id: edge.edge_id.clone(),
                source_node_id: edge.source_node_id.clone(),
                target_node_id: edge.target_node_id.clone(),
            }
        })
        .collect()
}

pub(crate) fn draft_nodes_from_flow(flow: &Flow) -> Vec<FlowNodeDraft> {
    flow.nodes
        .iter()
        .map(|node| FlowNodeDraft {
            node_id: node.node_id.clone(),
            node_type: flow_node_type_name(&node.node_type).to_string(),
            name: node.name.clone(),
            config: node.config.clone(),
        })
        .collect()
}

pub(crate) fn draft_edges_from_flow(flow: &Flow) -> Vec<FlowEdgeDraft> {
    flow.edges
        .iter()
        .map(|edge| FlowEdgeDraft {
            edge_id: Some(edge.edge_id.clone()),
            source_node_id: edge.source_node_id.clone(),
            target_node_id: edge.target_node_id.clone(),
        })
        .collect()
}

fn flow_node_type_name(node_type: &FlowNodeType) -> &'static str {
    match node_type {
        FlowNodeType::Source => "source",
        FlowNodeType::Decoder => "decoder",
        FlowNodeType::Transform => "transform",
        FlowNodeType::Filter => "filter",
        FlowNodeType::Rule => "rule",
        FlowNodeType::Sink => "sink",
        FlowNodeType::Dlq => "dlq",
    }
}

fn validation_response(
    flow_id: Option<Uuid>,
    flow_key: Option<String>,
    analysis: FlowAnalysis,
) -> FlowValidationResponse {
    FlowValidationResponse {
        flow_id,
        flow_key,
        valid: analysis.valid,
        validation_issues: analysis.validation_issues,
        node_inventory: analysis.node_plan,
        referenced_connectors: analysis.referenced_connectors,
        planned_sinks: analysis.planned_sinks,
    }
}

fn dry_run_response(
    flow_id: Option<Uuid>,
    flow_key: Option<String>,
    sample_payload_provided: bool,
    payload_format: Option<String>,
    source_node_id: Option<String>,
    analysis: FlowAnalysis,
) -> FlowDryRunResponse {
    FlowDryRunResponse {
        execution_supported: false,
        simulated: true,
        flow_id,
        flow_key,
        valid: analysis.valid,
        validation_issues: analysis.validation_issues,
        planned_path: analysis.planned_path,
        node_plan: analysis.node_plan,
        referenced_connectors: analysis.referenced_connectors,
        planned_sinks: analysis.planned_sinks,
        would_store_observation: analysis.would_store_observation,
        would_publish_mqtt: analysis.would_publish_mqtt,
        would_forward_http: analysis.would_forward_http,
        would_create_event: analysis.would_create_event,
        would_create_command: analysis.would_create_command,
        would_use_dlq: analysis.would_use_dlq,
        side_effects_performed: false,
        sample_payload_provided,
        payload_format,
        source_node_id,
    }
}

pub(crate) fn resolve_flow_for_read(
    state: &AppState,
    auth: &AuthContext,
    endpoint: &'static str,
    flow_id: Uuid,
) -> Result<Flow, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return state
            .storage
            .get_flow(state.tenant_id, flow_id)?
            .ok_or_else(ApiError::not_found);
    }

    if is_admin_all(auth) {
        return state
            .storage
            .get_flow_any_tenant(flow_id)?
            .ok_or_else(ApiError::not_found);
    }

    let tenant_id = principal_tenant_id(auth)?;
    match state.storage.get_flow(tenant_id, flow_id)? {
        Some(flow) => Ok(flow),
        None => {
            if state.storage.get_flow_any_tenant(flow_id)?.is_some() {
                Err(ApiError::forbidden(format!(
                    "principal tenant does not own the resource for {endpoint}"
                )))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
}

fn map_flow_error(error: FlowError) -> ApiError {
    ApiError::bad_request(error.to_string())
}

fn record_flow_event(
    state: &AppState,
    event_type: &str,
    severity: EventSeverity,
    flow: &Flow,
    message: Option<String>,
) -> Result<(), ApiError> {
    let _ = record_event(
        state,
        EventDraft {
            event_type: event_type.to_string(),
            severity,
            source_entity_id: None,
            target_entity_id: None,
            message,
            occurred_at: Utc::now(),
            observed_at: None,
            correlation_id: None,
            raw_message_id: None,
            observation_id: None,
            command_id: None,
            action_id: None,
            action_result_id: None,
            metadata: Some(json!({
                "flow_id": flow.id,
                "flow_key": flow.flow_key,
                "enabled": flow.enabled,
            })),
        },
    )?;
    Ok(())
}

fn default_json_object() -> Value {
    Value::Object(Default::default())
}
