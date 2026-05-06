use crate::{
    auth::{is_admin_all, principal_tenant_id, require_scope, AuthContext},
    ensure_action_exists, ensure_action_result_exists, ensure_command_exists, ensure_entity_exists,
    ensure_raw_message_exists,
    error::ApiError,
    evaluate_rules_for_event,
    query_filters::{optional_metadata_evidence_matches, optional_metadata_string_matches},
    AppState, AuthMode,
};
use aion_event::{Event, EventSeverity};
use aion_storage::EventFilter;
use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/events", post(create_event).get(query_events))
        .route("/events/:event_id", get(get_event))
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateEventRequest {
    pub event_type: String,
    pub severity: EventSeverity,
    pub source_entity_id: Option<Uuid>,
    pub target_entity_id: Option<Uuid>,
    pub message: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub observed_at: Option<DateTime<Utc>>,
    pub correlation_id: Option<String>,
    pub raw_message_id: Option<Uuid>,
    pub observation_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
    pub action_id: Option<Uuid>,
    pub action_result_id: Option<Uuid>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct EventQuery {
    pub source_entity_id: Option<Uuid>,
    pub target_entity_id: Option<Uuid>,
    pub event_type: Option<String>,
    pub severity: Option<EventSeverity>,
    pub command_id: Option<Uuid>,
    pub raw_message_id: Option<Uuid>,
    pub correlation_id: Option<String>,
    pub incident_id: Option<String>,
    pub alert_id: Option<String>,
    pub trace_id: Option<String>,
    pub run_id: Option<String>,
    pub workflow_id: Option<String>,
    pub cycle_id: Option<String>,
    pub evidence_id: Option<String>,
    pub external_id: Option<String>,
}

async fn create_event(
    State(state): State<AppState>,
    Json(request): Json<CreateEventRequest>,
) -> Result<(StatusCode, Json<Event>), ApiError> {
    if let Some(source_entity_id) = request.source_entity_id {
        ensure_entity_exists(&state, source_entity_id)?;
    }
    if let Some(target_entity_id) = request.target_entity_id {
        ensure_entity_exists(&state, target_entity_id)?;
    }
    if let Some(command_id) = request.command_id {
        ensure_command_exists(&state, command_id)?;
    }
    if let Some(action_id) = request.action_id {
        ensure_action_exists(&state, action_id)?;
    }
    if let Some(action_result_id) = request.action_result_id {
        ensure_action_result_exists(&state, action_result_id)?;
    }
    if let Some(raw_message_id) = request.raw_message_id {
        ensure_raw_message_exists(&state, raw_message_id)?;
    }

    let event = Event::new(
        state.tenant_id,
        request.event_type,
        request.severity,
        request.source_entity_id,
        request.target_entity_id,
        request.message,
        request.occurred_at,
        request.observed_at,
        request.correlation_id,
        request.raw_message_id,
        request.observation_id,
        request.command_id,
        request.action_id,
        request.action_result_id,
        request.metadata,
        Utc::now(),
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let event = state.storage.store_event(event)?;
    evaluate_rules_for_event(&state, &event, true)?;
    Ok((StatusCode::CREATED, Json(event)))
}

async fn get_event(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(event_id): Path<Uuid>,
) -> Result<Json<Event>, ApiError> {
    require_scope(&state, &auth, "/events/:event_id", "events:read")?;
    let event = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_event(state.tenant_id, event_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_event_any_tenant(event_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_event(tenant_id, event_id)? {
            Some(event) => event,
            None => {
                if state.storage.get_event_any_tenant(event_id)?.is_some() {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /events/:event_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(event))
}

async fn query_events(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<EventQuery>,
) -> Result<Json<Vec<Event>>, ApiError> {
    require_scope(&state, &auth, "/events", "events:read")?;
    let filter = EventFilter {
        source_entity_id: query.source_entity_id,
        target_entity_id: query.target_entity_id,
        event_type: query.event_type.clone(),
        severity: query.severity.clone(),
        command_id: query.command_id,
        raw_message_id: query.raw_message_id,
        correlation_id: query.correlation_id.clone(),
    };
    let events = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .query_events(state.tenant_id, filter.clone())?
    } else if is_admin_all(&auth) {
        state
            .storage
            .list_all_events()?
            .into_iter()
            .filter(|event| {
                filter
                    .source_entity_id
                    .map(|id| event.source_entity_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .target_entity_id
                    .map(|id| event.target_entity_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .event_type
                    .as_deref()
                    .map(|event_type| event.event_type == event_type)
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .severity
                    .as_ref()
                    .map(|severity| event.severity == *severity)
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .command_id
                    .map(|id| event.command_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .raw_message_id
                    .map(|id| event.raw_message_id == Some(id))
                    .unwrap_or(true)
            })
            .filter(|event| {
                filter
                    .correlation_id
                    .as_deref()
                    .map(|correlation_id| event.correlation_id.as_deref() == Some(correlation_id))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>()
    } else {
        state
            .storage
            .query_events(principal_tenant_id(&auth)?, filter.clone())?
    };
    let events = events
        .into_iter()
        .filter(|event| event_matches_metadata_filters(event, &query))
        .collect::<Vec<_>>();

    Ok(Json(events))
}

fn event_matches_metadata_filters(event: &Event, query: &EventQuery) -> bool {
    let metadata = event.metadata.as_ref();
    optional_metadata_string_matches(metadata, "incident_id", query.incident_id.as_deref())
        && optional_metadata_string_matches(metadata, "alert_id", query.alert_id.as_deref())
        && optional_metadata_string_matches(metadata, "trace_id", query.trace_id.as_deref())
        && optional_metadata_string_matches(metadata, "run_id", query.run_id.as_deref())
        && optional_metadata_string_matches(metadata, "workflow_id", query.workflow_id.as_deref())
        && optional_metadata_string_matches(metadata, "cycle_id", query.cycle_id.as_deref())
        && optional_metadata_evidence_matches(
            metadata,
            query.evidence_id.as_deref(),
            query.external_id.as_deref(),
        )
}
