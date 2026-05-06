use crate::{
    auth::{is_admin_all, principal_tenant_id, require_scope, AuthContext},
    error::ApiError,
    query_filters::{
        optional_raw_header_string_matches, optional_raw_smartsentinel_string_matches,
    },
    AppState, AuthMode,
};
use aion_raw_message::{NormalizationStatus, RawMessage, RawMessageSource};
use axum::{
    extract::{Extension, Path, Query, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/raw-messages", get(query_raw_messages))
        .route("/raw-messages/:raw_message_id", get(get_raw_message))
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawMessageQuery {
    pub producer_entity_id: Option<Uuid>,
    pub feature_of_interest_id: Option<Uuid>,
    pub payload_format: Option<String>,
    pub trace_id: Option<String>,
    pub run_id: Option<String>,
    pub workflow_id: Option<String>,
    pub cycle_id: Option<String>,
    pub correlation_id: Option<String>,
    pub snapshot_id: Option<String>,
    pub node_id: Option<String>,
    pub connector_id: Option<Uuid>,
    pub connector_key: Option<String>,
    pub connector_profile: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct RawMessageResponse {
    pub id: Uuid,
    pub raw_message_id: Uuid,
    pub source_type: RawMessageSource,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub payload_format: Option<String>,
    pub connector_id: Option<Uuid>,
    pub connector_key: Option<String>,
    pub connector_profile: Option<String>,
    pub source_endpoint: Option<String>,
    pub topic_or_path: Option<String>,
    pub producer_entity_id: Option<Uuid>,
    pub feature_of_interest_id: Option<Uuid>,
    pub received_at: DateTime<Utc>,
    pub normalization_status: NormalizationStatus,
    pub normalization_error: Option<String>,
    pub decoder_metadata: Value,
    pub payload: Value,
}

async fn get_raw_message(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(raw_message_id): Path<Uuid>,
) -> Result<Json<RawMessageResponse>, ApiError> {
    require_scope(
        &state,
        &auth,
        "/raw-messages/:raw_message_id",
        "raw-messages:read",
    )?;
    let raw_message = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state
            .storage
            .get_raw_message(state.tenant_id, raw_message_id)?
            .ok_or_else(ApiError::not_found)?
    } else if is_admin_all(&auth) {
        state
            .storage
            .get_raw_message_any_tenant(raw_message_id)?
            .ok_or_else(ApiError::not_found)?
    } else {
        let tenant_id = principal_tenant_id(&auth)?;
        match state.storage.get_raw_message(tenant_id, raw_message_id)? {
            Some(raw_message) => raw_message,
            None => {
                if state
                    .storage
                    .get_raw_message_any_tenant(raw_message_id)?
                    .is_some()
                {
                    return Err(ApiError::forbidden(
                        "principal tenant does not own the resource for /raw-messages/:raw_message_id",
                    ));
                }
                return Err(ApiError::not_found());
            }
        }
    };

    Ok(Json(raw_message_response(raw_message)))
}

async fn query_raw_messages(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Query(query): Query<RawMessageQuery>,
) -> Result<Json<Vec<RawMessageResponse>>, ApiError> {
    require_scope(&state, &auth, "/raw-messages", "raw-messages:read")?;
    let raw_messages = if matches!(auth.mode, AuthMode::Dev | AuthMode::Disabled) {
        state.storage.list_raw_messages(state.tenant_id)?
    } else if is_admin_all(&auth) {
        state.storage.list_all_raw_messages()?
    } else {
        state
            .storage
            .list_raw_messages(principal_tenant_id(&auth)?)?
    }
    .into_iter()
    .filter(|raw_message| {
        query
            .producer_entity_id
            .map(|id| raw_message_uuid_header(raw_message, "producer_entity_id") == Some(id))
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .feature_of_interest_id
            .map(|id| raw_message_uuid_header(raw_message, "feature_of_interest_id") == Some(id))
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .payload_format
            .as_deref()
            .map(|payload_format| {
                raw_message_string_header(raw_message, "payload_format")
                    .map(|value| value.eq_ignore_ascii_case(payload_format))
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .connector_id
            .map(|id| raw_message_uuid_header(raw_message, "connector_id") == Some(id))
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .connector_key
            .as_deref()
            .map(|connector_key| {
                raw_message_string_header(raw_message, "connector_key")
                    .map(|value| value == connector_key)
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    })
    .filter(|raw_message| {
        query
            .connector_profile
            .as_deref()
            .map(|connector_profile| {
                raw_message_string_header(raw_message, "connector_profile")
                    .map(|value| value.eq_ignore_ascii_case(connector_profile))
                    .unwrap_or(false)
            })
            .unwrap_or(true)
    })
    .filter(|raw_message| raw_message_matches_provenance_filters(raw_message, &query))
    .map(raw_message_response)
    .collect::<Vec<_>>();

    Ok(Json(raw_messages))
}

fn raw_message_matches_provenance_filters(
    raw_message: &RawMessage,
    query: &RawMessageQuery,
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
}

pub(crate) fn raw_message_response(raw_message: RawMessage) -> RawMessageResponse {
    let protocol = raw_message_string_header(&raw_message, "protocol");
    let payload_format = raw_message_string_header(&raw_message, "payload_format")
        .or(raw_message.decoder_hint.clone());
    let producer_entity_id = raw_message_uuid_header(&raw_message, "producer_entity_id");
    let feature_of_interest_id = raw_message_uuid_header(&raw_message, "feature_of_interest_id");
    let connector_id = raw_message_uuid_header(&raw_message, "connector_id");
    let connector_key = raw_message_string_header(&raw_message, "connector_key");
    let connector_profile = raw_message_string_header(&raw_message, "connector_profile");
    let source_endpoint = raw_message_string_header(&raw_message, "source_endpoint");
    let topic_or_path = raw_message_string_header(&raw_message, "topic_or_path")
        .or_else(|| raw_message.source_ref.clone());
    let decoder_metadata = raw_message
        .headers
        .get("decoder_metadata")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let payload = raw_payload_value(&raw_message.payload);

    RawMessageResponse {
        id: raw_message.id,
        raw_message_id: raw_message.id,
        source_type: raw_message.source_type,
        protocol,
        content_type: raw_message.content_type,
        payload_format,
        connector_id,
        connector_key,
        connector_profile,
        source_endpoint,
        topic_or_path,
        producer_entity_id,
        feature_of_interest_id,
        received_at: raw_message.received_at,
        normalization_status: raw_message.normalization_status,
        normalization_error: raw_message.normalization_error,
        decoder_metadata,
        payload,
    }
}

fn raw_message_string_header(raw_message: &RawMessage, key: &str) -> Option<String> {
    raw_message
        .headers
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn raw_message_uuid_header(raw_message: &RawMessage, key: &str) -> Option<Uuid> {
    raw_message
        .headers
        .get(key)
        .and_then(Value::as_str)
        .and_then(|value| value.parse().ok())
}

fn raw_payload_value(payload: &[u8]) -> Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        String::from_utf8(payload.to_vec())
            .map(Value::String)
            .unwrap_or_else(|_| json!({"encoding": "binary", "byte_length": payload.len()}))
    })
}
