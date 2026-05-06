use crate::{
    auth::{require_scope, AuthContext},
    error::ApiError,
    query_filters::event_filtering_helpers::{
        optional_metadata_evidence_matches, optional_metadata_string_matches,
        optional_raw_header_string_matches, optional_raw_smartsentinel_evidence_id_matches,
        optional_raw_smartsentinel_external_id_matches, optional_raw_smartsentinel_string_matches,
    },
    raw_message_response, AppState, RawMessageResponse,
};
use aion_event::Event;
use aion_observation::Observation;
use aion_raw_message::RawMessage;
use aion_storage::EventFilter;
use axum::{
    extract::{Extension, Query, State},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/provenance/search", get(search_provenance))
}

#[derive(Debug, Deserialize)]
struct ProvenanceSearchQuery {
    incident_id: Option<String>,
    alert_id: Option<String>,
    trace_id: Option<String>,
    run_id: Option<String>,
    workflow_id: Option<String>,
    cycle_id: Option<String>,
    correlation_id: Option<String>,
    snapshot_id: Option<String>,
    node_id: Option<String>,
    evidence_id: Option<String>,
    external_id: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct ProvenanceSearchResponse {
    matching_events: Vec<Event>,
    matching_raw_messages: Vec<RawMessageResponse>,
    matching_observations: Vec<Observation>,
    counts: ProvenanceSearchCounts,
    query: Value,
}

#[derive(Debug, Serialize)]
struct ProvenanceSearchCounts {
    matching_events: usize,
    matching_raw_messages: usize,
    matching_observations: usize,
}

async fn search_provenance(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<ProvenanceSearchQuery>,
) -> Result<Json<ProvenanceSearchResponse>, ApiError> {
    require_scope(&state, &auth, "/provenance/search", "provenance:read")?;
    let limit = query.limit.unwrap_or(100).min(1000);
    let events = state
        .storage
        .query_events(state.tenant_id, EventFilter::default())?
        .into_iter()
        .filter(|event| event_matches_provenance_search(event, &query))
        .take(limit as usize)
        .collect::<Vec<_>>();
    let raw_messages = state
        .storage
        .list_raw_messages(state.tenant_id)?
        .into_iter()
        .filter(|raw_message| raw_message_matches_provenance_search(raw_message, &query))
        .take(limit as usize)
        .map(raw_message_response)
        .collect::<Vec<_>>();
    let observations = state
        .storage
        .query_observations(state.tenant_id, None, None, None, None, limit)?
        .into_iter()
        .filter(|observation| observation_matches_provenance_search(observation, &query))
        .collect::<Vec<_>>();
    let counts = ProvenanceSearchCounts {
        matching_events: events.len(),
        matching_raw_messages: raw_messages.len(),
        matching_observations: observations.len(),
    };
    let query_metadata = provenance_search_query_metadata(&query, limit);

    Ok(Json(ProvenanceSearchResponse {
        matching_events: events,
        matching_raw_messages: raw_messages,
        matching_observations: observations,
        counts,
        query: query_metadata,
    }))
}

fn event_matches_provenance_search(event: &Event, query: &ProvenanceSearchQuery) -> bool {
    let metadata = event.metadata.as_ref();
    optional_metadata_string_matches(metadata, "incident_id", query.incident_id.as_deref())
        && optional_metadata_string_matches(metadata, "alert_id", query.alert_id.as_deref())
        && optional_metadata_string_matches(metadata, "trace_id", query.trace_id.as_deref())
        && optional_metadata_string_matches(metadata, "run_id", query.run_id.as_deref())
        && optional_metadata_string_matches(metadata, "workflow_id", query.workflow_id.as_deref())
        && optional_metadata_string_matches(metadata, "cycle_id", query.cycle_id.as_deref())
        && optional_metadata_string_matches(
            metadata,
            "correlation_id",
            query.correlation_id.as_deref(),
        )
        && optional_metadata_string_matches(metadata, "snapshot_id", query.snapshot_id.as_deref())
        && optional_metadata_string_matches(metadata, "node_id", query.node_id.as_deref())
        && optional_metadata_evidence_matches(
            metadata,
            query.evidence_id.as_deref(),
            query.external_id.as_deref(),
        )
}

fn raw_message_matches_provenance_search(
    raw_message: &RawMessage,
    query: &ProvenanceSearchQuery,
) -> bool {
    optional_raw_header_string_matches(raw_message, "snapshot_id", query.snapshot_id.as_deref())
        && optional_raw_header_string_matches(raw_message, "node_id", query.node_id.as_deref())
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "trace_id",
            query.trace_id.as_deref(),
        )
        && optional_raw_smartsentinel_string_matches(raw_message, "run_id", query.run_id.as_deref())
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "workflow_id",
            query.workflow_id.as_deref(),
        )
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "cycle_id",
            query.cycle_id.as_deref(),
        )
        && optional_raw_smartsentinel_string_matches(
            raw_message,
            "correlation_id",
            query.correlation_id.as_deref(),
        )
        && optional_raw_smartsentinel_evidence_id_matches(raw_message, query.evidence_id.as_deref())
        && optional_raw_smartsentinel_external_id_matches(raw_message, query.external_id.as_deref())
        && optional_raw_smartsentinel_external_id_matches(raw_message, query.incident_id.as_deref())
        && query.alert_id.is_none()
}

fn observation_matches_provenance_search(
    observation: &Observation,
    query: &ProvenanceSearchQuery,
) -> bool {
    optional_metadata_string_matches(
        Some(&observation.metadata),
        "trace_id",
        query.trace_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "run_id",
        query.run_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "workflow_id",
        query.workflow_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "cycle_id",
        query.cycle_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "correlation_id",
        query.correlation_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "snapshot_id",
        query.snapshot_id.as_deref(),
    ) && optional_metadata_string_matches(
        Some(&observation.metadata),
        "node_id",
        query.node_id.as_deref(),
    ) && optional_metadata_evidence_matches(
        Some(&observation.metadata),
        query.evidence_id.as_deref(),
        query.external_id.as_deref(),
    ) && query.incident_id.is_none()
        && query.alert_id.is_none()
}

fn provenance_search_query_metadata(query: &ProvenanceSearchQuery, limit: u32) -> Value {
    json!({
        "incident_id": query.incident_id.as_deref(),
        "alert_id": query.alert_id.as_deref(),
        "trace_id": query.trace_id.as_deref(),
        "run_id": query.run_id.as_deref(),
        "workflow_id": query.workflow_id.as_deref(),
        "cycle_id": query.cycle_id.as_deref(),
        "correlation_id": query.correlation_id.as_deref(),
        "snapshot_id": query.snapshot_id.as_deref(),
        "node_id": query.node_id.as_deref(),
        "evidence_id": query.evidence_id.as_deref(),
        "external_id": query.external_id.as_deref(),
        "limit": limit
    })
}
