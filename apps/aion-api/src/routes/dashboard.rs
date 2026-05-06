use crate::{
    auth::{is_admin_all, principal_tenant_id, require_scope, AuthContext},
    error::ApiError,
    routes::workers::{connector_runtime_status_from_spec, ConnectorWorkerRuntimeStatus},
    state_for_tenant,
    worker_support::{connector_worker_spec, ConnectorWorkerRuntimeState, IngestionWorkerKind},
    AppState, AuthMode,
};
use aion_entity::Entity;
use aion_observation::Observation;
use aion_storage::{EventFilter, IngestionConnector, IngestionConnectorType};
use axum::{
    extract::{Extension, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

const UNBOUNDED_QUERY_LIMIT: u32 = u32::MAX;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/dashboard/overview", get(get_dashboard_overview))
        .route(
            "/dashboard/timeseries/entities",
            get(list_dashboard_timeseries_entities),
        )
        .route(
            "/dashboard/connectors/overview",
            get(get_dashboard_connectors_overview),
        )
}

#[derive(Debug, Serialize)]
pub(crate) struct DashboardOverviewResponse {
    pub entities_count: usize,
    pub observations_count: usize,
    pub raw_messages_count: usize,
    pub events_count: usize,
    pub flows_count: usize,
    pub enabled_flows_count: usize,
    pub connectors_count: usize,
    pub enabled_connectors_count: usize,
    pub workers_running_count: usize,
    pub workers_degraded_count: usize,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DashboardTimeseriesEntitiesResponse {
    pub generated_at: DateTime<Utc>,
    pub entities: Vec<DashboardTimeseriesEntitySummary>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DashboardTimeseriesEntitySummary {
    pub entity_id: Uuid,
    pub entity_key: String,
    pub entity_type: String,
    pub display_name: Option<String>,
    pub observed_property_count: usize,
    pub observation_count: usize,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub properties: Vec<DashboardObservedPropertySummary>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DashboardObservedPropertySummary {
    pub observed_property: String,
    pub observation_count: usize,
    pub last_observed_at: Option<DateTime<Utc>>,
    pub units: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DashboardConnectorsOverviewResponse {
    pub generated_at: DateTime<Utc>,
    pub connectors: Vec<DashboardConnectorOverviewItem>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DashboardConnectorOverviewItem {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: aion_storage::ConnectorProfile,
    pub enabled: bool,
    pub status: String,
    pub readiness: String,
    pub broker_url: Option<String>,
    pub topic_filter: Option<String>,
    pub payload_format: Option<String>,
    pub worker_kind: IngestionWorkerKind,
    pub worker_status: ConnectorWorkerRuntimeState,
    pub running: bool,
    pub reconnecting: bool,
    pub degraded: bool,
    pub last_error: Option<String>,
    pub secret_configured: bool,
}

async fn get_dashboard_overview(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<DashboardOverviewResponse>, ApiError> {
    require_scope(&state, &auth, "/dashboard/overview", "dashboard:read")?;

    let entities_count = scoped_entities(&state, &auth)?.len();
    let observations_count = scoped_observations(&state, &auth)?.len();
    let raw_messages_count = scoped_raw_messages_count(&state, &auth)?;
    let events_count = scoped_events_count(&state, &auth)?;
    let flows = scoped_flows(&state, &auth)?;
    let connectors = scoped_connectors(&state, &auth)?;
    let connector_items = build_connector_overview_items(&state, connectors)?;

    Ok(Json(DashboardOverviewResponse {
        entities_count,
        observations_count,
        raw_messages_count,
        events_count,
        flows_count: flows.len(),
        enabled_flows_count: flows.iter().filter(|flow| flow.enabled).count(),
        connectors_count: connector_items.len(),
        enabled_connectors_count: connector_items.iter().filter(|item| item.enabled).count(),
        workers_running_count: connector_items.iter().filter(|item| item.running).count(),
        workers_degraded_count: connector_items.iter().filter(|item| item.degraded).count(),
        generated_at: Utc::now(),
    }))
}

async fn list_dashboard_timeseries_entities(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<DashboardTimeseriesEntitiesResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/dashboard/timeseries/entities",
        "dashboard:read",
    )?;

    let entities = scoped_entities(&state, &auth)?;
    let entity_map = entities
        .into_iter()
        .map(|entity| (entity.id, entity))
        .collect::<HashMap<_, _>>();
    let observations = scoped_observations(&state, &auth)?;
    let mut summaries = BTreeMap::<Uuid, DashboardTimeseriesEntitySummary>::new();

    for observation in observations {
        let Some(entity) = entity_map.get(&observation.feature_of_interest_id) else {
            continue;
        };

        let summary =
            summaries
                .entry(entity.id)
                .or_insert_with(|| DashboardTimeseriesEntitySummary {
                    entity_id: entity.id,
                    entity_key: entity.entity_key.clone(),
                    entity_type: entity.entity_type.clone(),
                    display_name: derive_entity_display_name(entity),
                    observed_property_count: 0,
                    observation_count: 0,
                    last_observed_at: None,
                    properties: Vec::new(),
                });

        summary.observation_count += 1;
        summary.last_observed_at = Some(
            summary
                .last_observed_at
                .map(|value| value.max(observation.observed_at))
                .unwrap_or(observation.observed_at),
        );

        match summary
            .properties
            .iter_mut()
            .find(|property| property.observed_property == observation.observed_property)
        {
            Some(property) => {
                property.observation_count += 1;
                property.last_observed_at = Some(
                    property
                        .last_observed_at
                        .map(|value| value.max(observation.observed_at))
                        .unwrap_or(observation.observed_at),
                );
                if let Some(unit) = observation.unit.as_deref() {
                    if !property.units.iter().any(|existing| existing == unit) {
                        property.units.push(unit.to_string());
                    }
                }
            }
            None => {
                let mut units = Vec::new();
                if let Some(unit) = observation.unit.as_deref() {
                    units.push(unit.to_string());
                }
                summary.properties.push(DashboardObservedPropertySummary {
                    observed_property: observation.observed_property.clone(),
                    observation_count: 1,
                    last_observed_at: Some(observation.observed_at),
                    units,
                });
            }
        }
    }

    let mut entities = summaries.into_values().collect::<Vec<_>>();
    for entity in &mut entities {
        entity
            .properties
            .sort_by(|left, right| left.observed_property.cmp(&right.observed_property));
        for property in &mut entity.properties {
            property.units.sort();
            property.units.dedup();
        }
        entity.observed_property_count = entity.properties.len();
    }
    entities.sort_by(|left, right| left.entity_key.cmp(&right.entity_key));

    Ok(Json(DashboardTimeseriesEntitiesResponse {
        generated_at: Utc::now(),
        entities,
    }))
}

async fn get_dashboard_connectors_overview(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<DashboardConnectorsOverviewResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/dashboard/connectors/overview",
        "dashboard:read",
    )?;

    Ok(Json(DashboardConnectorsOverviewResponse {
        generated_at: Utc::now(),
        connectors: build_connector_overview_items(&state, scoped_connectors(&state, &auth)?)?,
    }))
}

fn scoped_entities(state: &AppState, auth: &AuthContext) -> Result<Vec<Entity>, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        Ok(state.storage.list_entities(state.tenant_id)?)
    } else if is_admin_all(auth) {
        Ok(state.storage.list_all_entities()?)
    } else {
        Ok(state.storage.list_entities(principal_tenant_id(auth)?)?)
    }
}

fn scoped_observations(state: &AppState, auth: &AuthContext) -> Result<Vec<Observation>, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        Ok(state.storage.query_observations(
            state.tenant_id,
            None,
            None,
            None,
            None,
            UNBOUNDED_QUERY_LIMIT,
        )?)
    } else if is_admin_all(auth) {
        Ok(state.storage.list_all_observations()?)
    } else {
        Ok(state.storage.query_observations(
            principal_tenant_id(auth)?,
            None,
            None,
            None,
            None,
            UNBOUNDED_QUERY_LIMIT,
        )?)
    }
}

fn scoped_raw_messages_count(state: &AppState, auth: &AuthContext) -> Result<usize, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        Ok(state.storage.list_raw_messages(state.tenant_id)?.len())
    } else if is_admin_all(auth) {
        Ok(state.storage.list_all_raw_messages()?.len())
    } else {
        Ok(state
            .storage
            .list_raw_messages(principal_tenant_id(auth)?)?
            .len())
    }
}

fn scoped_events_count(state: &AppState, auth: &AuthContext) -> Result<usize, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        Ok(state
            .storage
            .query_events(state.tenant_id, EventFilter::default())?
            .len())
    } else if is_admin_all(auth) {
        Ok(state.storage.list_all_events()?.len())
    } else {
        Ok(state
            .storage
            .query_events(principal_tenant_id(auth)?, EventFilter::default())?
            .len())
    }
}

fn scoped_connectors(
    state: &AppState,
    auth: &AuthContext,
) -> Result<Vec<IngestionConnector>, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        Ok(state.storage.list_ingestion_connectors(state.tenant_id)?)
    } else if is_admin_all(auth) {
        Ok(state.storage.list_all_ingestion_connectors()?)
    } else {
        Ok(state
            .storage
            .list_ingestion_connectors(principal_tenant_id(auth)?)?)
    }
}

fn scoped_flows(state: &AppState, auth: &AuthContext) -> Result<Vec<aion_flow::Flow>, ApiError> {
    if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        Ok(state.storage.list_flows(state.tenant_id)?)
    } else if is_admin_all(auth) {
        Ok(state.storage.list_all_flows()?)
    } else {
        Ok(state.storage.list_flows(principal_tenant_id(auth)?)?)
    }
}

fn build_connector_overview_items(
    state: &AppState,
    connectors: Vec<IngestionConnector>,
) -> Result<Vec<DashboardConnectorOverviewItem>, ApiError> {
    let runtime_statuses = state
        .connector_worker_statuses
        .read()
        .map(|statuses| statuses.clone())
        .unwrap_or_default();
    let mut items = Vec::with_capacity(connectors.len());

    for connector in connectors {
        let runtime = runtime_status_for_connector(state, &runtime_statuses, &connector)?;
        let reconnecting = runtime.status == ConnectorWorkerRuntimeState::Reconnecting;
        let degraded = matches!(
            runtime.status,
            ConnectorWorkerRuntimeState::Degraded | ConnectorWorkerRuntimeState::Reconnecting
        );
        let running = runtime.status == ConnectorWorkerRuntimeState::Running;
        let status = connector_status_label(&connector, &runtime).to_string();

        items.push(DashboardConnectorOverviewItem {
            connector_id: connector.id,
            connector_key: connector.connector_key,
            connector_type: connector.connector_type,
            connector_profile: connector.connector_profile,
            enabled: connector.enabled,
            status: status.clone(),
            readiness: status,
            broker_url: redact_broker_url(runtime.broker_url.or(connector.broker_url)),
            topic_filter: runtime.topic_filter.or(connector.topic_filter),
            payload_format: runtime.payload_format.or(connector.payload_format),
            worker_kind: runtime.worker_kind,
            worker_status: runtime.status,
            running,
            reconnecting,
            degraded,
            last_error: runtime.last_error,
            secret_configured: connector.secret_ref_id.is_some(),
        });
    }

    items.sort_by(|left, right| left.connector_key.cmp(&right.connector_key));
    Ok(items)
}

fn runtime_status_for_connector(
    state: &AppState,
    runtime_statuses: &HashMap<Uuid, ConnectorWorkerRuntimeStatus>,
    connector: &IngestionConnector,
) -> Result<ConnectorWorkerRuntimeStatus, ApiError> {
    if let Some(runtime) = runtime_statuses.get(&connector.id) {
        return Ok(runtime.clone());
    }

    let scoped_state = state_for_tenant(state, connector.tenant_id);
    let spec = connector_worker_spec(&scoped_state, connector.clone())?;
    Ok(connector_runtime_status_from_spec(&spec))
}

fn connector_status_label(
    connector: &IngestionConnector,
    runtime: &ConnectorWorkerRuntimeStatus,
) -> &'static str {
    if !connector.enabled {
        "disabled"
    } else {
        match runtime.status {
            ConnectorWorkerRuntimeState::Planned => "planned",
            ConnectorWorkerRuntimeState::Starting => "starting",
            ConnectorWorkerRuntimeState::Running => "ready",
            ConnectorWorkerRuntimeState::Reconnecting => "reconnecting",
            ConnectorWorkerRuntimeState::Degraded => "degraded",
            ConnectorWorkerRuntimeState::Stopped => "stopped",
            ConnectorWorkerRuntimeState::Skipped => "skipped",
            ConnectorWorkerRuntimeState::Invalid | ConnectorWorkerRuntimeState::Error => "error",
            ConnectorWorkerRuntimeState::Unsupported => "unsupported",
        }
    }
}

fn derive_entity_display_name(entity: &Entity) -> Option<String> {
    string_value_from_jsonld_key(&entity.jsonld, "display_name")
        .or_else(|| string_value_from_jsonld_key(&entity.jsonld, "displayName"))
        .or_else(|| string_value_from_jsonld_key(&entity.jsonld, "aion:name"))
        .or_else(|| string_value_from_jsonld_key(&entity.jsonld, "name"))
}

fn string_value_from_jsonld_key(value: &Value, key: &str) -> Option<String> {
    let entry = value.as_object()?.get(key)?;
    first_jsonld_string(entry)
}

fn first_jsonld_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Array(values) => values.iter().find_map(first_jsonld_string),
        Value::Object(map) => map
            .get("@value")
            .and_then(first_jsonld_string)
            .or_else(|| map.get("value").and_then(first_jsonld_string))
            .or_else(|| {
                map.values().find_map(|entry| match entry {
                    Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
                    _ => None,
                })
            }),
        _ => None,
    }
}

fn redact_broker_url(value: Option<String>) -> Option<String> {
    value.map(|url| {
        if let Some(scheme_index) = url.find("://") {
            let authority_start = scheme_index + 3;
            if let Some(at_index) = url[authority_start..].find('@') {
                let authority_end = authority_start + at_index + 1;
                let mut redacted = String::with_capacity(url.len());
                redacted.push_str(&url[..authority_start]);
                redacted.push_str(&url[authority_end..]);
                return redacted;
            }
        }

        if let Some(at_index) = url.find('@') {
            return url[(at_index + 1)..].to_string();
        }

        url
    })
}
