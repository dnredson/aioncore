use crate::{
    auth::{require_scope, AuthContext},
    connector_event_metadata, decoded_ingest_metadata, decoder_for_format, ensure_entity_exists,
    error::ApiError,
    evaluate_rules_for_observation, get_connector, is_ttn_uplink_payload_format, merge_json_object,
    metadata_with_connector, payload_format_requires_mapping, payload_to_bytes,
    record_ingest_event, record_ingest_event_optional, record_ttn_device_mapping_event, AppState,
};
use aion_event::EventSeverity;
use aion_observation::Observation;
use aion_payload::{DecodeInput, PayloadFormat};
use aion_raw_message::{RawMessage, RawMessageSource};
use aion_storage::{ConnectorProfile, IngestionConnector, TtnDeviceMapping};
use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    routing::post,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::str::FromStr;
use uuid::Uuid;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/ingest/http", post(ingest_http))
        .route(
            "/ingestion/connectors/:connector_id/ingest",
            post(ingest_http_for_connector),
        )
}

#[derive(Debug, Deserialize)]
pub(crate) struct HttpIngestRequest {
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Uuid,
    pub payload_format: String,
    pub protocol: String,
    pub content_type: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
    pub payload: Value,
    pub mapping: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectorHttpIngestRequest {
    pub producer_entity_id: Option<Uuid>,
    pub feature_of_interest_id: Option<Uuid>,
    pub payload_format: Option<String>,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
    pub payload: Value,
    pub mapping: Option<Value>,
}

#[derive(Debug, Serialize)]
pub(crate) struct HttpIngestResponse {
    pub raw_message_id: Uuid,
    pub observations: Vec<Observation>,
}

#[derive(Debug, Clone)]
struct ResolvedTtnDeviceMapping {
    mapping_id: Uuid,
    ttn_device_id: String,
    ttn_application_id: Option<String>,
    mapping_resolution: &'static str,
}

#[derive(Debug, Clone)]
struct TtnUplinkIds {
    device_id: String,
    application_id: Option<String>,
}

enum TtnDeviceMappingResolution {
    Resolved {
        mapping: TtnDeviceMapping,
        resolution: &'static str,
    },
    Missing,
    Ambiguous(String),
}

async fn ingest_http_for_connector(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(connector_id): Path<Uuid>,
    Json(request): Json<ConnectorHttpIngestRequest>,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    require_scope(
        &state,
        &auth,
        "/ingestion/connectors/:connector_id/ingest",
        "ingestion:write",
    )?;
    let connector = get_connector(&state, connector_id)?;
    if !connector.enabled {
        return Err(ApiError::bad_request("ingestion connector is disabled"));
    }

    let payload_format = request
        .payload_format
        .or_else(|| connector.payload_format.clone())
        .ok_or_else(|| ApiError::bad_request("payload_format is required"))?;
    let protocol = request
        .protocol
        .or_else(|| connector.protocol.clone())
        .unwrap_or_else(|| "http".to_string());
    let content_type = request
        .content_type
        .or_else(|| connector.content_type.clone());

    let mut producer_entity_id = request
        .producer_entity_id
        .or(connector.default_producer_entity_id);
    let mut feature_of_interest_id = request
        .feature_of_interest_id
        .or(connector.default_feature_of_interest_id);
    let mut resolved_ttn_mapping = None;

    if connector.connector_profile == ConnectorProfile::TtnV3
        && is_ttn_uplink_payload_format(&payload_format)
        && (producer_entity_id.is_none() || feature_of_interest_id.is_none())
    {
        let ttn_ids = extract_ttn_uplink_ids(&request.payload)?;
        match resolve_ttn_device_mapping(&state, connector.id, &ttn_ids)? {
            TtnDeviceMappingResolution::Resolved {
                mapping,
                resolution,
            } => {
                if producer_entity_id.is_none() {
                    producer_entity_id = Some(mapping.producer_entity_id);
                }
                if feature_of_interest_id.is_none() {
                    feature_of_interest_id = mapping.feature_of_interest_id;
                }
                resolved_ttn_mapping = Some(ResolvedTtnDeviceMapping {
                    mapping_id: mapping.id,
                    ttn_device_id: mapping.ttn_device_id.clone(),
                    ttn_application_id: mapping.ttn_application_id.clone(),
                    mapping_resolution: resolution,
                });
                record_ttn_device_mapping_event(
                    &state,
                    "aion:TtnDeviceMappingResolved",
                    &mapping,
                    Some("TTN device mapping resolved".to_string()),
                )?;
            }
            TtnDeviceMappingResolution::Missing if producer_entity_id.is_none() => {
                return fail_ttn_device_mapping_resolution(
                    &state,
                    &connector,
                    &payload_format,
                    &protocol,
                    content_type.clone(),
                    &request.payload,
                    &ttn_ids,
                    "ttn_device_mapping_missing",
                    "aion:TtnDeviceMappingMissing",
                );
            }
            TtnDeviceMappingResolution::Ambiguous(error) if producer_entity_id.is_none() => {
                return fail_ttn_device_mapping_resolution(
                    &state,
                    &connector,
                    &payload_format,
                    &protocol,
                    content_type.clone(),
                    &request.payload,
                    &ttn_ids,
                    &error,
                    "aion:TtnDeviceMappingAmbiguous",
                );
            }
            TtnDeviceMappingResolution::Missing | TtnDeviceMappingResolution::Ambiguous(_) => {}
        }
    }

    let producer_entity_id = producer_entity_id
        .ok_or_else(|| ApiError::bad_request("producer_entity_id is required"))?;
    let feature_of_interest_id = feature_of_interest_id
        .ok_or_else(|| ApiError::bad_request("feature_of_interest_id is required"))?;

    let request = HttpIngestRequest {
        producer_entity_id,
        feature_of_interest_id,
        payload_format,
        protocol,
        content_type,
        observed_at: request.observed_at,
        payload: request.payload,
        mapping: request.mapping,
    };

    ingest_http_resolved(&state, request, Some(connector), resolved_ttn_mapping).await
}

async fn ingest_http(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<HttpIngestRequest>,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    require_scope(&state, &auth, "/ingest/http", "ingestion:write")?;
    ingest_http_resolved(&state, request, None, None).await
}

async fn ingest_http_resolved(
    state: &AppState,
    request: HttpIngestRequest,
    connector: Option<IngestionConnector>,
    resolved_ttn_mapping: Option<ResolvedTtnDeviceMapping>,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    ensure_entity_exists(state, request.producer_entity_id)?;
    ensure_entity_exists(state, request.feature_of_interest_id)?;

    let received_at = Utc::now();
    let payload_bytes = payload_to_bytes(&request.payload);
    let profile = state
        .storage
        .get_payload_profile(state.tenant_id, request.producer_entity_id)?;
    let mapping_source = if request.mapping.is_some() {
        "request"
    } else if profile
        .as_ref()
        .and_then(|profile| profile.attribute_mapping.as_ref())
        .is_some()
    {
        "payload_profile"
    } else {
        "none"
    };
    let connector_metadata = connector.as_ref().map(connector_event_metadata);
    let source_ref = connector
        .as_ref()
        .and_then(|connector| connector.http_path.clone())
        .unwrap_or_else(|| "/ingest/http".to_string());
    let mut headers = json!({
        "protocol": request.protocol,
        "payload_format": request.payload_format,
        "producer_entity_id": request.producer_entity_id,
        "feature_of_interest_id": request.feature_of_interest_id,
        "source_endpoint": connector
            .as_ref()
            .and_then(|connector| connector.endpoint.clone())
            .or_else(|| Some(source_ref.clone())),
        "topic_or_path": source_ref,
        "decoder_metadata": {
            "decoder": request.payload_format,
            "mapping_source": mapping_source
        }
    });
    if let (Some(object), Some(metadata)) = (headers.as_object_mut(), connector_metadata.clone()) {
        object.insert("connector".to_string(), metadata.clone());
        object.insert("connector_id".to_string(), metadata["connector_id"].clone());
        object.insert(
            "connector_key".to_string(),
            metadata["connector_key"].clone(),
        );
        object.insert(
            "connector_profile".to_string(),
            metadata["connector_profile"].clone(),
        );
    }

    let mut raw_message = RawMessage::new(
        state.tenant_id,
        RawMessageSource::Http,
        Some(source_ref),
        Some(request.producer_entity_id.to_string()),
        Some(request.payload_format.clone()),
        request.content_type.clone(),
        Some(request.producer_entity_id),
        Some(request.feature_of_interest_id),
        Some(request.payload_format.clone()),
        headers,
        payload_bytes.clone(),
        received_at,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    raw_message = state.storage.store_raw_message(raw_message)?;

    let decoder_config = request
        .mapping
        .or_else(|| profile.and_then(|profile| profile.attribute_mapping))
        .or_else(|| {
            if is_ttn_uplink_payload_format(&request.payload_format) {
                connector
                    .as_ref()
                    .and_then(|connector| connector.metadata.clone())
            } else {
                None
            }
        });
    if payload_format_requires_mapping(&request.payload_format) && decoder_config.is_none() {
        let message = format!(
            "{} payloads require request mapping or producer PayloadProfile attribute_mapping",
            request.payload_format
        );
        state
            .storage
            .mark_raw_message_failed(state.tenant_id, raw_message.id, &message)?;
        record_ingest_event(
            state,
            "aion:PayloadIngestionFailed",
            EventSeverity::Error,
            request.producer_entity_id,
            request.feature_of_interest_id,
            raw_message.id,
            Some(message.clone()),
            metadata_with_connector(
                json!({
                    "payload_format": request.payload_format,
                    "reason": "missing_mapping"
                }),
                connector_metadata.clone(),
            ),
        )?;
        return Err(ApiError::bad_request(message));
    }

    let decoder = match decoder_for_format(&request.payload_format) {
        Ok(decoder) => decoder,
        Err(err) => {
            state
                .storage
                .mark_raw_message_failed(state.tenant_id, raw_message.id, &err.message)?;
            record_ingest_event(
                state,
                "aion:PayloadIngestionFailed",
                EventSeverity::Error,
                request.producer_entity_id,
                request.feature_of_interest_id,
                raw_message.id,
                Some(err.message.clone()),
                metadata_with_connector(
                    json!({
                        "payload_format": request.payload_format,
                        "reason": "unsupported_payload_format"
                    }),
                    connector_metadata.clone(),
                ),
            )?;
            return Err(err);
        }
    };
    let decode_result = decoder.decode(DecodeInput {
        tenant_id: state.tenant_id,
        device_key: Some(request.producer_entity_id.to_string()),
        format: PayloadFormat::from_str(&request.payload_format).unwrap(),
        content_type: request.content_type,
        payload: payload_bytes,
        received_at: request.observed_at.unwrap_or(received_at),
        config: decoder_config,
    });

    let decoded = match decode_result {
        Ok(decoded) => decoded,
        Err(err) => {
            state.storage.mark_raw_message_failed(
                state.tenant_id,
                raw_message.id,
                err.message(),
            )?;
            record_ingest_event(
                state,
                "aion:PayloadIngestionFailed",
                EventSeverity::Error,
                request.producer_entity_id,
                request.feature_of_interest_id,
                raw_message.id,
                Some(err.message().to_string()),
                metadata_with_connector(
                    json!({
                        "payload_format": request.payload_format,
                        "reason": "decoder_error"
                    }),
                    connector_metadata.clone(),
                ),
            )?;
            return Err(ApiError::bad_request(err.to_string()));
        }
    };

    let ingest_metadata = decoded_ingest_metadata(&decoded);
    let mut observations = Vec::with_capacity(decoded.len());
    for measurement in decoded {
        let observation = Observation::new(
            state.tenant_id,
            request.producer_entity_id,
            request.feature_of_interest_id,
            measurement.observed_property,
            measurement.value,
            measurement.unit,
            measurement.time,
            received_at,
            request.protocol.clone(),
            request.payload_format.clone(),
            Some(raw_message.id),
            connector_metadata.clone().unwrap_or_else(|| json!({})),
            measurement.metadata,
        )
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let observation = state.storage.store_observation(observation)?;
        evaluate_rules_for_observation(state, &observation, true)?;
        observations.push(observation);
    }

    state
        .storage
        .mark_raw_message_normalized(state.tenant_id, raw_message.id)?;
    let mut payload_event_metadata = json!({
        "payload_format": request.payload_format,
        "observation_count": observations.len()
    });
    merge_json_object(&mut payload_event_metadata, ingest_metadata);
    if let Some(mapping) = resolved_ttn_mapping {
        merge_json_object(
            &mut payload_event_metadata,
            json!({
                "ttn_mapping_id": mapping.mapping_id,
                "mapping_resolution": mapping.mapping_resolution,
                "ttn_device_id": mapping.ttn_device_id,
                "ttn_application_id": mapping.ttn_application_id
            }),
        );
    }
    record_ingest_event(
        state,
        "aion:PayloadIngested",
        EventSeverity::Info,
        request.producer_entity_id,
        request.feature_of_interest_id,
        raw_message.id,
        Some("Payload ingested and normalized".to_string()),
        metadata_with_connector(payload_event_metadata, connector_metadata),
    )?;

    Ok((
        StatusCode::CREATED,
        Json(HttpIngestResponse {
            raw_message_id: raw_message.id,
            observations,
        }),
    ))
}

fn extract_ttn_uplink_ids(payload: &Value) -> Result<TtnUplinkIds, ApiError> {
    let payload = payload_as_json_value(payload)?;
    let end_device_ids = payload
        .get("end_device_ids")
        .and_then(Value::as_object)
        .ok_or_else(|| ApiError::bad_request("TTN uplink payload is missing end_device_ids"))?;
    let device_id = end_device_ids
        .get("device_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ApiError::bad_request("TTN uplink payload is missing device_id"))?;
    let application_id = end_device_ids
        .get("application_ids")
        .and_then(Value::as_object)
        .and_then(|application_ids| application_ids.get("application_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    Ok(TtnUplinkIds {
        device_id,
        application_id,
    })
}

fn payload_as_json_value(payload: &Value) -> Result<Value, ApiError> {
    if let Some(payload) = payload.as_str() {
        serde_json::from_str(payload)
            .map_err(|err| ApiError::bad_request(format!("invalid TTN uplink JSON: {err}")))
    } else {
        Ok(payload.clone())
    }
}

fn resolve_ttn_device_mapping(
    state: &AppState,
    connector_id: Uuid,
    ttn_ids: &TtnUplinkIds,
) -> Result<TtnDeviceMappingResolution, ApiError> {
    let mappings = state
        .storage
        .list_ttn_device_mappings(state.tenant_id, connector_id)?;
    let enabled_matches = mappings
        .into_iter()
        .filter(|mapping| mapping.enabled && mapping.ttn_device_id == ttn_ids.device_id)
        .collect::<Vec<_>>();

    if let Some(application_id) = ttn_ids.application_id.as_deref() {
        let exact = enabled_matches
            .iter()
            .filter(|mapping| mapping.ttn_application_id.as_deref() == Some(application_id))
            .cloned()
            .collect::<Vec<_>>();
        if exact.len() > 1 {
            return Ok(TtnDeviceMappingResolution::Ambiguous(format!(
                "ambiguous_exact_application_mapping: multiple enabled TTN mappings found for connector {connector_id}, device '{}', application '{}'",
                ttn_ids.device_id, application_id
            )));
        }
        if let Some(mapping) = exact.into_iter().next() {
            return Ok(TtnDeviceMappingResolution::Resolved {
                mapping,
                resolution: "exact_application_match",
            });
        }
    }

    let fallback = enabled_matches
        .into_iter()
        .filter(|mapping| mapping.ttn_application_id.is_none())
        .collect::<Vec<_>>();
    if fallback.len() > 1 {
        return Ok(TtnDeviceMappingResolution::Ambiguous(format!(
            "ambiguous_fallback_device_mapping: multiple enabled fallback TTN mappings found for connector {connector_id}, device '{}'",
            ttn_ids.device_id
        )));
    }
    if let Some(mapping) = fallback.into_iter().next() {
        return Ok(TtnDeviceMappingResolution::Resolved {
            mapping,
            resolution: "fallback_device_match",
        });
    }

    Ok(TtnDeviceMappingResolution::Missing)
}

fn fail_ttn_device_mapping_resolution(
    state: &AppState,
    connector: &IngestionConnector,
    payload_format: &str,
    protocol: &str,
    content_type: Option<String>,
    payload: &Value,
    ttn_ids: &TtnUplinkIds,
    mapping_resolution_error: &str,
    event_type: &'static str,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    let received_at = Utc::now();
    let source_ref = connector
        .http_path
        .clone()
        .or_else(|| connector.endpoint.clone())
        .or_else(|| connector.topic_filter.clone())
        .unwrap_or_else(|| "/ingestion/connectors/{connector_id}/ingest".to_string());
    let connector_metadata = Some(connector_event_metadata(connector));
    let mut headers = json!({
        "protocol": protocol,
        "payload_format": payload_format,
        "source_endpoint": connector.endpoint.clone().unwrap_or_else(|| source_ref.clone()),
        "topic_or_path": source_ref,
        "decoder_metadata": {
            "decoder": payload_format,
            "mapping_source": "ttn_device_mapping",
            "reason": mapping_resolution_error
        },
        "ttn_device_id": ttn_ids.device_id,
        "ttn_application_id": ttn_ids.application_id
    });
    if let Some(object) = headers.as_object_mut() {
        let metadata = connector_metadata.clone().unwrap_or_else(|| json!({}));
        object.insert("connector".to_string(), metadata.clone());
        object.insert("connector_id".to_string(), metadata["connector_id"].clone());
        object.insert(
            "connector_key".to_string(),
            metadata["connector_key"].clone(),
        );
        object.insert(
            "connector_profile".to_string(),
            metadata["connector_profile"].clone(),
        );
    }

    let mut raw_message = RawMessage::new(
        state.tenant_id,
        RawMessageSource::Http,
        Some(source_ref),
        Some(ttn_ids.device_id.clone()),
        Some(payload_format.to_string()),
        content_type,
        None,
        None,
        Some(payload_format.to_string()),
        headers,
        payload_to_bytes(payload),
        received_at,
    )
    .map_err(|err| ApiError::bad_request(err.to_string()))?;
    raw_message = state.storage.store_raw_message(raw_message)?;

    let message = format!(
        "TTN device mapping resolution failed for connector {} and device {}: {}",
        connector.id, ttn_ids.device_id, mapping_resolution_error
    );
    state
        .storage
        .mark_raw_message_failed(state.tenant_id, raw_message.id, &message)?;
    let event_metadata = metadata_with_connector(
        json!({
            "payload_format": payload_format,
            "reason": mapping_resolution_error,
            "ttn_device_id": ttn_ids.device_id,
            "ttn_application_id": ttn_ids.application_id,
            "connector_id": connector.id,
            "mapping_resolution_error": mapping_resolution_error
        }),
        connector_metadata,
    );
    record_ingest_event_optional(
        state,
        event_type,
        EventSeverity::Error,
        None,
        None,
        Some(raw_message.id),
        Some(message.clone()),
        event_metadata.clone(),
    )?;
    record_ingest_event_optional(
        state,
        "aion:PayloadIngestionFailed",
        EventSeverity::Error,
        None,
        None,
        Some(raw_message.id),
        Some(message.clone()),
        event_metadata,
    )?;

    Err(ApiError::bad_request(message))
}
