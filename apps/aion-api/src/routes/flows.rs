use crate::{
    auth::{
        is_admin_all, principal_tenant_id, require_scope, require_scope_for_write,
        tenant_for_created_resource, AuthContext,
    },
    error::ApiError,
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
use serde::Deserialize;
use serde_json::{json, Value};
use std::str::FromStr;
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/flows", post(create_flow).get(list_flows))
        .route(
            "/flows/:flow_id",
            get(get_flow).patch(update_flow).delete(delete_flow),
        )
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

    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        return Ok(Json(
            state
                .storage
                .get_flow(state.tenant_id, flow_id)?
                .ok_or_else(ApiError::not_found)?,
        ));
    }

    if is_admin_all(&auth) {
        return Ok(Json(
            state
                .storage
                .get_flow_any_tenant(flow_id)?
                .ok_or_else(ApiError::not_found)?,
        ));
    }

    let tenant_id = principal_tenant_id(&auth)?;
    match state.storage.get_flow(tenant_id, flow_id)? {
        Some(flow) => Ok(Json(flow)),
        None => {
            if state.storage.get_flow_any_tenant(flow_id)?.is_some() {
                Err(ApiError::forbidden(
                    "principal tenant does not own the resource for /flows/:flow_id",
                ))
            } else {
                Err(ApiError::not_found())
            }
        }
    }
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
