use crate::{
    auth::{principal_tenant_or_default, require_scope, AuthContext},
    connector_event_metadata, decoded_ingest_metadata, decoder_for_format, ensure_entity_exists,
    error::ApiError,
    evaluate_rules_for_observation, get_connector, is_ttn_uplink_payload_format, merge_json_object,
    metadata_with_connector, payload_format_requires_mapping, payload_to_bytes,
    record_ingest_event, record_ingest_event_optional, record_ttn_device_mapping_event,
    state_for_tenant, AppState,
};
use aion_event::EventSeverity;
use aion_observation::Observation;
use aion_payload::{DecodeInput, PayloadFormat, ReliableIngestionEnvelope};
use aion_raw_message::{RawMessage, RawMessageSource};
use aion_storage::{ConnectorProfile, IngestionConnector, StorageError, TtnDeviceMapping};
use aion_sync::{SyncSession, SyncSessionStatus};
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

const MAX_RELIABLE_BATCH_ITEMS: usize = 1_000;

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/ingest/http", post(ingest_http))
        .route("/ingest/reliable", post(ingest_reliable))
        .route("/ingest/batch", post(ingest_batch))
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

#[derive(Debug, Deserialize)]
pub(crate) struct ReliableHttpIngestRequest {
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Uuid,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub mapping: Option<Value>,
    #[serde(flatten)]
    pub envelope: ReliableIngestionEnvelope,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReliableIngestResponse {
    pub raw_message_id: Uuid,
    pub duplicate: bool,
    pub idempotency_key: Option<String>,
    pub observations_created: usize,
    pub event_id: Option<Uuid>,
    pub payload_format: String,
    pub source_system: Option<String>,
    pub sync_session_id: Option<String>,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BatchReliableIngestRequest {
    pub batch_id: Option<String>,
    pub sync_session_id: Option<String>,
    pub source_system: Option<String>,
    pub source_id: Option<String>,
    pub connectivity_state: Option<String>,
    pub continue_on_error: Option<bool>,
    pub external_flow_id: Option<String>,
    pub external_flow_name: Option<String>,
    pub metadata: Option<Value>,
    pub items: Vec<BatchReliableIngestItemRequest>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct BatchReliableIngestItemRequest {
    pub producer_entity_id: Uuid,
    pub feature_of_interest_id: Uuid,
    pub protocol: Option<String>,
    pub content_type: Option<String>,
    pub mapping: Option<Value>,
    #[serde(flatten)]
    pub envelope: ReliableIngestionEnvelope,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchReliableIngestResponse {
    pub batch_id: Option<String>,
    pub sync_session_id: Option<String>,
    pub source_system: Option<String>,
    pub received_at: DateTime<Utc>,
    pub total_items: usize,
    pub accepted_count: usize,
    pub duplicate_count: usize,
    pub failed_count: usize,
    pub observations_created: usize,
    pub stopped_early: bool,
    pub results: Vec<BatchReliableIngestItemResult>,
    pub event_id: Option<Uuid>,
    pub sync_session_record_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchReliableIngestItemResult {
    pub index: usize,
    pub status: &'static str,
    pub duplicate: bool,
    pub idempotency_key: Option<String>,
    pub raw_message_id: Option<Uuid>,
    pub observations_created: usize,
    pub error: Option<BatchReliableIngestItemError>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchReliableIngestItemError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone)]
struct ReliableIngestionContext {
    source_ref: String,
    envelope: ReliableIngestionEnvelope,
    extra_metadata: Value,
}

#[derive(Debug)]
struct IngestOutcome {
    raw_message: RawMessage,
    observations: Vec<Observation>,
    event_id: Option<Uuid>,
    duplicate: bool,
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

    let outcome =
        ingest_http_resolved(&state, request, Some(connector), resolved_ttn_mapping, None).await?;
    Ok((
        StatusCode::CREATED,
        Json(HttpIngestResponse {
            raw_message_id: outcome.raw_message.id,
            observations: outcome.observations,
        }),
    ))
}

async fn ingest_http(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<HttpIngestRequest>,
) -> Result<(StatusCode, Json<HttpIngestResponse>), ApiError> {
    require_scope(&state, &auth, "/ingest/http", "ingestion:write")?;
    let outcome = ingest_http_resolved(&state, request, None, None, None).await?;
    Ok((
        StatusCode::CREATED,
        Json(HttpIngestResponse {
            raw_message_id: outcome.raw_message.id,
            observations: outcome.observations,
        }),
    ))
}

async fn ingest_reliable(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<ReliableHttpIngestRequest>,
) -> Result<(StatusCode, Json<ReliableIngestResponse>), ApiError> {
    require_scope(&state, &auth, "/ingest/reliable", "ingestion:write")?;
    let tenant_id = principal_tenant_or_default(&state, &auth)?;
    let scoped_state = state_for_tenant(&state, tenant_id);
    ingest_reliable_scoped(&scoped_state, request, "/ingest/reliable", json!({})).await
}

async fn ingest_batch(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(request): Json<BatchReliableIngestRequest>,
) -> Result<(StatusCode, Json<BatchReliableIngestResponse>), ApiError> {
    require_scope(&state, &auth, "/ingest/batch", "batches:write")?;
    if request.items.is_empty() {
        return Err(ApiError::bad_request(
            "batch items must not be empty for /ingest/batch",
        ));
    }
    if request.items.len() > MAX_RELIABLE_BATCH_ITEMS {
        return Err(ApiError::bad_request(format!(
            "batch items must not exceed {MAX_RELIABLE_BATCH_ITEMS} for /ingest/batch"
        )));
    }

    let tenant_id = principal_tenant_or_default(&state, &auth)?;
    let scoped_state = state_for_tenant(&state, tenant_id);
    let received_at = Utc::now();
    let continue_on_error = request.continue_on_error.unwrap_or(true);
    let total_items = request.items.len();
    let mut accepted_count = 0usize;
    let mut duplicate_count = 0usize;
    let mut failed_count = 0usize;
    let mut observations_created = 0usize;
    let mut stopped_early = false;
    let mut results = Vec::with_capacity(total_items);

    for (index, item) in request.items.iter().cloned().enumerate() {
        let merged_request = merge_batch_item_request(&request, item);
        let idempotency_key = merged_request.envelope.idempotency_key.clone();
        let batch_metadata = batch_item_extra_metadata(&request);
        match ingest_reliable_scoped(
            &scoped_state,
            merged_request,
            "/ingest/batch",
            batch_metadata,
        )
        .await
        {
            Ok((_, Json(response))) => {
                if response.duplicate {
                    duplicate_count += 1;
                    results.push(BatchReliableIngestItemResult {
                        index,
                        status: "duplicate",
                        duplicate: true,
                        idempotency_key: response.idempotency_key,
                        raw_message_id: Some(response.raw_message_id),
                        observations_created: 0,
                        error: None,
                    });
                } else {
                    accepted_count += 1;
                    observations_created += response.observations_created;
                    results.push(BatchReliableIngestItemResult {
                        index,
                        status: "accepted",
                        duplicate: false,
                        idempotency_key: response.idempotency_key,
                        raw_message_id: Some(response.raw_message_id),
                        observations_created: response.observations_created,
                        error: None,
                    });
                }
            }
            Err(error) => {
                failed_count += 1;
                results.push(BatchReliableIngestItemResult {
                    index,
                    status: "failed",
                    duplicate: false,
                    idempotency_key,
                    raw_message_id: None,
                    observations_created: 0,
                    error: Some(BatchReliableIngestItemError {
                        code: api_error_code(&error).to_string(),
                        message: error.message.clone(),
                    }),
                });
                if !continue_on_error {
                    stopped_early = true;
                    break;
                }
            }
        }
    }

    let event_metadata = json!({
        "batch_id": request.batch_id,
        "sync_session_id": request.sync_session_id,
        "source_system": request.source_system,
        "source_id": request.source_id,
        "total_items": total_items,
        "accepted_count": accepted_count,
        "duplicate_count": duplicate_count,
        "failed_count": failed_count,
        "observations_created": observations_created,
        "stopped_early": stopped_early
    });
    let event = record_ingest_event_optional(
        &scoped_state,
        "aion:ReliableBatchIngested",
        EventSeverity::Info,
        None,
        None,
        None,
        Some("Reliable batch ingestion processed".to_string()),
        event_metadata,
    )?;
    let sync_session_record = record_batch_sync_session(
        &scoped_state,
        &request,
        total_items,
        accepted_count,
        duplicate_count,
        failed_count,
        observations_created,
        received_at,
    )?;

    Ok((
        StatusCode::OK,
        Json(BatchReliableIngestResponse {
            batch_id: request.batch_id,
            sync_session_id: request.sync_session_id,
            source_system: request.source_system,
            received_at,
            total_items,
            accepted_count,
            duplicate_count,
            failed_count,
            observations_created,
            stopped_early,
            results,
            event_id: Some(event.id),
            sync_session_record_id: sync_session_record.as_ref().map(|session| session.id),
        }),
    ))
}

fn record_batch_sync_session(
    state: &AppState,
    request: &BatchReliableIngestRequest,
    total_items: usize,
    accepted_count: usize,
    duplicate_count: usize,
    failed_count: usize,
    observations_created: usize,
    received_at: DateTime<Utc>,
) -> Result<Option<SyncSession>, ApiError> {
    let sync_session_id = match request
        .sync_session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => value,
        None => return Ok(None),
    };

    let mut session = match state
        .storage
        .get_sync_session_by_external_id(state.tenant_id, sync_session_id)?
    {
        Some(session) => session,
        None => {
            let session = SyncSession::new(
                state.tenant_id,
                sync_session_id.to_string(),
                request.source_system.clone(),
                request.source_id.clone(),
                None,
                None,
                Some(SyncSessionStatus::Receiving),
                request.connectivity_state.clone(),
                None,
                request.metadata.clone(),
                received_at,
            )
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
            state.storage.create_sync_session(session)?
        }
    };

    if session.source_system.is_none() {
        session.source_system = request.source_system.clone();
    }
    if session.source_id.is_none() {
        session.source_id = request.source_id.clone();
    }
    if let Some(metadata) = request.metadata.clone() {
        session.metadata = metadata;
    }
    session.apply_batch_result(
        request.batch_id.clone(),
        total_items as u64,
        accepted_count as u64,
        duplicate_count as u64,
        failed_count as u64,
        observations_created as u64,
        request.connectivity_state.clone(),
        received_at,
    );
    Ok(Some(state.storage.update_sync_session(session)?))
}

async fn ingest_reliable_scoped(
    state: &AppState,
    request: ReliableHttpIngestRequest,
    source_ref: &str,
    extra_metadata: Value,
) -> Result<(StatusCode, Json<ReliableIngestResponse>), ApiError> {
    let tenant_id = state.tenant_id;
    let payload_format = request
        .envelope
        .payload_format
        .clone()
        .or_else(|| infer_payload_format(&request.envelope.payload))
        .ok_or_else(|| ApiError::bad_request("payload_format is required or must be inferable"))?;
    let source_system = request.envelope.source_system.clone();
    let sync_session_id = request.envelope.sync_session_id.clone();
    let idempotency_key = request.envelope.idempotency_key.clone();

    if let Some(idempotency_key) = idempotency_key.as_deref() {
        if let Some(existing) = state
            .storage
            .find_raw_message_by_idempotency_key(tenant_id, idempotency_key)?
        {
            return Ok((
                StatusCode::OK,
                Json(ReliableIngestResponse {
                    raw_message_id: existing.id,
                    duplicate: true,
                    idempotency_key: Some(idempotency_key.to_string()),
                    observations_created: 0,
                    event_id: None,
                    payload_format,
                    source_system,
                    sync_session_id,
                    received_at: existing.received_at,
                }),
            ));
        }
    }

    let outcome = ingest_http_resolved(
        state,
        HttpIngestRequest {
            producer_entity_id: request.producer_entity_id,
            feature_of_interest_id: request.feature_of_interest_id,
            payload_format: payload_format.clone(),
            protocol: request.protocol.unwrap_or_else(|| "http".to_string()),
            content_type: request.content_type,
            observed_at: request.envelope.observed_at,
            payload: request.envelope.payload.clone(),
            mapping: request.mapping,
        },
        None,
        None,
        Some(ReliableIngestionContext {
            source_ref: source_ref.to_string(),
            envelope: request.envelope,
            extra_metadata,
        }),
    )
    .await?;

    Ok((
        if outcome.duplicate {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(ReliableIngestResponse {
            raw_message_id: outcome.raw_message.id,
            duplicate: outcome.duplicate,
            idempotency_key,
            observations_created: outcome.observations.len(),
            event_id: outcome.event_id,
            payload_format,
            source_system,
            sync_session_id,
            received_at: outcome.raw_message.received_at,
        }),
    ))
}

async fn ingest_http_resolved(
    state: &AppState,
    request: HttpIngestRequest,
    connector: Option<IngestionConnector>,
    resolved_ttn_mapping: Option<ResolvedTtnDeviceMapping>,
    reliable: Option<ReliableIngestionContext>,
) -> Result<IngestOutcome, ApiError> {
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
    let source_ref = reliable
        .as_ref()
        .map(|reliable| reliable.source_ref.clone())
        .or_else(|| {
            connector
                .as_ref()
                .and_then(|connector| connector.http_path.clone())
        })
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
    let reliable_metadata = reliable
        .as_ref()
        .map(reliable_context_metadata)
        .unwrap_or_else(|| json!({}));
    merge_json_object(&mut headers, reliable_metadata.clone());

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
    raw_message.idempotency_key = reliable
        .as_ref()
        .and_then(|reliable| reliable.envelope.idempotency_key.clone());

    raw_message = match state.storage.store_raw_message(raw_message) {
        Ok(raw_message) => raw_message,
        Err(StorageError::Conflict) => {
            let Some(idempotency_key) = reliable
                .as_ref()
                .and_then(|reliable| reliable.envelope.idempotency_key.as_deref())
            else {
                return Err(StorageError::Conflict.into());
            };
            let Some(existing) = state
                .storage
                .find_raw_message_by_idempotency_key(state.tenant_id, idempotency_key)?
            else {
                return Err(StorageError::Conflict.into());
            };
            return Ok(IngestOutcome {
                raw_message: existing,
                observations: Vec::new(),
                event_id: None,
                duplicate: true,
            });
        }
        Err(err) => return Err(err.into()),
    };

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
                merge_metadata(
                    json!({
                        "payload_format": request.payload_format,
                        "reason": "missing_mapping"
                    }),
                    reliable_metadata.clone(),
                ),
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
                    merge_metadata(
                        json!({
                            "payload_format": request.payload_format,
                            "reason": "unsupported_payload_format"
                        }),
                        reliable_metadata.clone(),
                    ),
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
                    merge_metadata(
                        json!({
                            "payload_format": request.payload_format,
                            "reason": "decoder_error"
                        }),
                        reliable_metadata.clone(),
                    ),
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
        "observation_count": observations.len(),
        "duplicate": false
    });
    merge_json_object(&mut payload_event_metadata, ingest_metadata);
    merge_json_object(&mut payload_event_metadata, reliable_metadata);
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
    let event = record_ingest_event(
        state,
        "aion:PayloadIngested",
        EventSeverity::Info,
        request.producer_entity_id,
        request.feature_of_interest_id,
        raw_message.id,
        Some("Payload ingested and normalized".to_string()),
        metadata_with_connector(payload_event_metadata, connector_metadata),
    )?;
    Ok(IngestOutcome {
        raw_message,
        observations,
        event_id: Some(event.id),
        duplicate: false,
    })
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

fn merge_metadata(mut base: Value, extra: Value) -> Value {
    merge_json_object(&mut base, extra);
    base
}

fn reliable_context_metadata(reliable: &ReliableIngestionContext) -> Value {
    let mut metadata = reliable_metadata(&reliable.envelope);
    merge_json_object(&mut metadata, reliable.extra_metadata.clone());
    metadata
}

fn reliable_metadata(envelope: &ReliableIngestionEnvelope) -> Value {
    let mut metadata = json!({});
    if let Some(object) = metadata.as_object_mut() {
        insert_optional_value(
            object,
            "external.source_system",
            envelope.source_system.clone(),
        );
        insert_optional_value(object, "external.source_id", envelope.source_id.clone());
        insert_optional_value(
            object,
            "external.idempotency_key",
            envelope.idempotency_key.clone(),
        );
        insert_optional_value(
            object,
            "external.flow_id",
            envelope.external_flow_id.clone(),
        );
        insert_optional_value(
            object,
            "external.flow_name",
            envelope.external_flow_name.clone(),
        );
        insert_optional_value(
            object,
            "external.flowfile_uuid",
            envelope.external_flowfile_uuid.clone(),
        );
        insert_optional_value(
            object,
            "external.process_group_id",
            envelope.external_process_group_id.clone(),
        );
        insert_optional_value(
            object,
            "external.processor_id",
            envelope.external_processor_id.clone(),
        );
        insert_optional_value(
            object,
            "external.provenance_uri",
            envelope.external_provenance_uri.clone(),
        );
        insert_optional_value(
            object,
            "external.sync_session_id",
            envelope.sync_session_id.clone(),
        );
        insert_optional_value(
            object,
            "external.connectivity_state",
            envelope.connectivity_state.clone(),
        );
        insert_optional_value(
            object,
            "external.payload_hash",
            envelope.payload_hash.clone(),
        );
        if let Some(edge_sequence) = envelope.edge_sequence {
            object.insert("external.edge_sequence".to_string(), json!(edge_sequence));
        }
        if let Some(observed_at) = envelope.observed_at {
            object.insert("external.observed_at".to_string(), json!(observed_at));
        }
        if let Some(stored_at_edge) = envelope.stored_at_edge {
            object.insert("external.stored_at_edge".to_string(), json!(stored_at_edge));
        }
        if let Some(sent_at) = envelope.sent_at {
            object.insert("external.sent_at".to_string(), json!(sent_at));
        }
        if let Some(replay_count) = envelope.replay_count {
            object.insert("external.replay_count".to_string(), json!(replay_count));
        }
        if let Some(retry_count) = envelope.retry_count {
            object.insert("external.retry_count".to_string(), json!(retry_count));
        }
        if let Some(metadata_value) = envelope.metadata.clone() {
            object.insert("external.metadata".to_string(), metadata_value);
        }
    }
    metadata
}

fn merge_batch_item_request(
    batch: &BatchReliableIngestRequest,
    mut item: BatchReliableIngestItemRequest,
) -> ReliableHttpIngestRequest {
    item.envelope.source_system = item
        .envelope
        .source_system
        .or_else(|| batch.source_system.clone());
    item.envelope.source_id = item.envelope.source_id.or_else(|| batch.source_id.clone());
    item.envelope.sync_session_id = item
        .envelope
        .sync_session_id
        .or_else(|| batch.sync_session_id.clone());
    item.envelope.connectivity_state = item
        .envelope
        .connectivity_state
        .or_else(|| batch.connectivity_state.clone());
    item.envelope.external_flow_id = item
        .envelope
        .external_flow_id
        .or_else(|| batch.external_flow_id.clone());
    item.envelope.external_flow_name = item
        .envelope
        .external_flow_name
        .or_else(|| batch.external_flow_name.clone());
    item.envelope.metadata =
        merge_optional_json_metadata(batch.metadata.clone(), item.envelope.metadata);

    ReliableHttpIngestRequest {
        producer_entity_id: item.producer_entity_id,
        feature_of_interest_id: item.feature_of_interest_id,
        protocol: item.protocol,
        content_type: item.content_type,
        mapping: item.mapping,
        envelope: item.envelope,
    }
}

fn merge_optional_json_metadata(batch: Option<Value>, item: Option<Value>) -> Option<Value> {
    match (batch, item) {
        (None, None) => None,
        (Some(batch), None) => Some(batch),
        (None, Some(item)) => Some(item),
        (Some(mut batch), Some(item)) => {
            merge_json_object(&mut batch, item.clone());
            if batch.is_object() {
                Some(batch)
            } else {
                Some(item)
            }
        }
    }
}

fn batch_item_extra_metadata(batch: &BatchReliableIngestRequest) -> Value {
    let mut metadata = json!({});
    if let Some(object) = metadata.as_object_mut() {
        insert_optional_value(object, "external.batch_id", batch.batch_id.clone());
    }
    metadata
}

fn api_error_code(error: &ApiError) -> &'static str {
    match error.status {
        StatusCode::BAD_REQUEST => "bad_request",
        StatusCode::UNAUTHORIZED => "unauthorized",
        StatusCode::FORBIDDEN => "forbidden",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::CONFLICT => "conflict",
        _ => "internal_error",
    }
}

fn insert_optional_value(
    object: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<String>,
) {
    if let Some(value) = value {
        object.insert(key.to_string(), json!(value));
    }
}

fn infer_payload_format(payload: &Value) -> Option<String> {
    if let Some(payload) = payload.as_object() {
        if payload.contains_key("end_device_ids") && payload.contains_key("uplink_message") {
            return Some("ttn-uplink-json".to_string());
        }
        if payload.contains_key("observations")
            || (payload.contains_key("observed_property") && payload.contains_key("value"))
        {
            return Some("canonical-json".to_string());
        }
    }

    let Some(entries) = payload.as_array() else {
        return payload
            .as_str()
            .filter(|value| value.contains('|'))
            .map(|_| "ultralight".to_string());
    };

    entries
        .iter()
        .all(|entry| {
            entry
                .as_object()
                .map(|entry| {
                    entry.contains_key("n")
                        && (entry.contains_key("v")
                            || entry.contains_key("vs")
                            || entry.contains_key("vb")
                            || entry.contains_key("vd"))
                })
                .unwrap_or(false)
        })
        .then(|| "senml-json".to_string())
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
