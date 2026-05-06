use crate::{
    error::ApiError,
    metadata_with_connector, mqtt_ingest, record_connector_worker_event,
    routes::workers::{
        connector_runtime_status_from_spec, connector_worker_action, connector_workers_status,
        ConnectorWorkerReconcileAction, ConnectorWorkerRuntimeStatus,
        ReconcileConnectorWorkersResponse,
    },
    AppState, ReadyWorkerPlanSummary, StartupError,
};
use aion_event::EventSeverity;
use aion_storage::{
    ConnectorProfile, ConnectorSecretType, IngestionConnector, IngestionConnectorType,
};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::HashSet, env};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionWorkerKind {
    HttpListener,
    MqttSubscriber,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestionWorkerSpecStatus {
    Planned,
    Skipped,
    Invalid,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct IngestionWorkerValidationIssue {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct IngestionWorkerSpec {
    pub connector_id: Uuid,
    pub connector_key: String,
    pub connector_type: IngestionConnectorType,
    pub connector_profile: ConnectorProfile,
    pub enabled: bool,
    pub worker_kind: IngestionWorkerKind,
    pub broker_url: Option<String>,
    pub client_id: Option<String>,
    pub topic_filter: Option<String>,
    pub http_path: Option<String>,
    pub payload_format: Option<String>,
    pub content_type: Option<String>,
    pub secret_ref_id: Option<Uuid>,
    pub status: IngestionWorkerSpecStatus,
    pub validation_issues: Vec<IngestionWorkerValidationIssue>,
    pub metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct IngestionWorkerPlan {
    pub specs: Vec<IngestionWorkerSpec>,
    pub planned_workers: usize,
    pub skipped_workers: usize,
    pub invalid_workers: usize,
    pub unsupported_workers: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorWorkerConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectorWorkerEnvValues {
    pub enabled: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorWorkerRuntimeState {
    Planned,
    Starting,
    Running,
    Reconnecting,
    Degraded,
    Stopped,
    Skipped,
    Invalid,
    Error,
    Unsupported,
}

#[derive(Debug)]
pub(crate) struct ConnectorWorkerHandle {
    pub(crate) signature: ConnectorWorkerSignature,
    pub(crate) task: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConnectorWorkerSignature {
    pub(crate) broker_url: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) topic_filter: Option<String>,
    pub(crate) payload_format: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) secret_ref_id: Option<Uuid>,
    pub(crate) connector_profile: ConnectorProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectorWorkerStartDecision {
    StartMqtt,
    Skip,
    Invalid,
    Unsupported,
    PlannedOnly,
}

impl ConnectorWorkerConfig {
    pub fn from_env() -> Result<Self, StartupError> {
        Self::from_env_values(ConnectorWorkerEnvValues {
            enabled: env::var("AIONCORE_CONNECTOR_WORKERS_ENABLED").ok(),
        })
    }

    pub fn from_env_values(values: ConnectorWorkerEnvValues) -> Result<Self, StartupError> {
        Ok(Self {
            enabled: parse_bool_env_value(
                values.enabled.as_deref(),
                false,
                "AIONCORE_CONNECTOR_WORKERS_ENABLED",
            )?,
        })
    }
}

pub(crate) async fn start_connector_workers(
    state: AppState,
    config: ConnectorWorkerConfig,
) -> Result<(), StartupError> {
    set_connector_workers_enabled(&state, config.enabled);
    reconcile_connector_workers(state, true)
        .await
        .map(|_| ())
        .map_err(|err| StartupError::backend_initialization(err.message))
}

pub(crate) fn build_ready_worker_plan_summary(state: &AppState) -> ReadyWorkerPlanSummary {
    build_ingestion_worker_plan(state)
        .map(|plan| ReadyWorkerPlanSummary {
            planned_workers: plan.planned_workers,
            invalid_workers: plan.invalid_workers,
            unsupported_workers: plan.unsupported_workers,
        })
        .unwrap_or(ReadyWorkerPlanSummary {
            planned_workers: 0,
            invalid_workers: 0,
            unsupported_workers: 0,
        })
}

pub(crate) fn build_ingestion_worker_plan(
    state: &AppState,
) -> Result<IngestionWorkerPlan, ApiError> {
    let specs = state
        .storage
        .list_ingestion_connectors(state.tenant_id)?
        .into_iter()
        .map(|connector| connector_worker_spec(state, connector))
        .collect::<Result<Vec<_>, _>>()?;
    let planned_workers = specs
        .iter()
        .filter(|spec| spec.status == IngestionWorkerSpecStatus::Planned)
        .count();
    let skipped_workers = specs
        .iter()
        .filter(|spec| spec.status == IngestionWorkerSpecStatus::Skipped)
        .count();
    let invalid_workers = specs
        .iter()
        .filter(|spec| spec.status == IngestionWorkerSpecStatus::Invalid)
        .count();
    let unsupported_workers = specs
        .iter()
        .filter(|spec| spec.status == IngestionWorkerSpecStatus::Unsupported)
        .count();

    Ok(IngestionWorkerPlan {
        specs,
        planned_workers,
        skipped_workers,
        invalid_workers,
        unsupported_workers,
    })
}

pub(crate) fn connector_worker_start_decision(
    spec: &IngestionWorkerSpec,
) -> ConnectorWorkerStartDecision {
    match spec.status {
        IngestionWorkerSpecStatus::Skipped => ConnectorWorkerStartDecision::Skip,
        IngestionWorkerSpecStatus::Invalid => ConnectorWorkerStartDecision::Invalid,
        IngestionWorkerSpecStatus::Unsupported => ConnectorWorkerStartDecision::Unsupported,
        IngestionWorkerSpecStatus::Planned => match (&spec.worker_kind, &spec.connector_profile) {
            (IngestionWorkerKind::MqttSubscriber, ConnectorProfile::GenericAionMqtt)
            | (IngestionWorkerKind::MqttSubscriber, ConnectorProfile::GenericMqtt)
            | (IngestionWorkerKind::MqttSubscriber, ConnectorProfile::TtnV3) => {
                ConnectorWorkerStartDecision::StartMqtt
            }
            (IngestionWorkerKind::Unsupported, _) => ConnectorWorkerStartDecision::Unsupported,
            _ => ConnectorWorkerStartDecision::PlannedOnly,
        },
    }
}

pub(crate) async fn reconcile_connector_workers_after_mutation(state: &AppState) {
    if let Err(err) = reconcile_connector_workers(state.clone(), true).await {
        let _ = record_connector_worker_event(
            state,
            "aion:ConnectorWorkerReconcileFailed",
            EventSeverity::Error,
            Some("Connector worker reconciliation failed".to_string()),
            json!({
                "error": err.message
            }),
        );
    }
}

pub(crate) async fn reconcile_connector_workers(
    state: AppState,
    start_network: bool,
) -> Result<ReconcileConnectorWorkersResponse, ApiError> {
    let plan = build_ingestion_worker_plan(&state)?;
    let workers_enabled = connector_workers_enabled(&state);
    let mut actions = Vec::new();
    let now = Utc::now();

    if !workers_enabled {
        stop_all_connector_workers(&state, now, &mut actions)?;
        apply_connector_worker_plan_statuses(&state, &plan, now, false, &mut actions)?;
        let status = connector_workers_status(&state)?;
        return Ok(ReconcileConnectorWorkersResponse {
            connector_workers: status.connector_workers,
            actions,
            workers: status.workers,
        });
    }

    for spec in &plan.specs {
        reconcile_connector_worker_spec(&state, spec, start_network, now, &mut actions).await?;
    }

    stop_workers_missing_from_plan(&state, &plan, now, &mut actions)?;

    let status = connector_workers_status(&state)?;
    Ok(ReconcileConnectorWorkersResponse {
        connector_workers: status.connector_workers,
        actions,
        workers: status.workers,
    })
}

fn apply_connector_worker_plan_statuses(
    state: &AppState,
    plan: &IngestionWorkerPlan,
    reconciled_at: DateTime<Utc>,
    emit_skip_events: bool,
    actions: &mut Vec<ConnectorWorkerReconcileAction>,
) -> Result<(), ApiError> {
    for spec in &plan.specs {
        let mut status = connector_runtime_status_from_spec(spec);
        status.last_reconciled_at = Some(reconciled_at);
        set_connector_worker_runtime_status(state, status);

        if emit_skip_events
            && connector_worker_start_decision(spec) == ConnectorWorkerStartDecision::Skip
            && spec.enabled
            && spec.connector_profile == ConnectorProfile::TtnV3
        {
            record_connector_worker_event(
                state,
                "aion:ConnectorWorkerSkipped",
                EventSeverity::Warning,
                Some(
                    "TTN v3 connector worker skipped because TTN decoding is future work"
                        .to_string(),
                ),
                connector_worker_event_metadata(spec, Some("ttn_decoding_not_implemented")),
            )?;
            actions.push(connector_worker_action(
                spec,
                "skipped",
                Some("TTN v3 decoding is not implemented yet"),
            ));
        }
    }

    Ok(())
}

async fn reconcile_connector_worker_spec(
    state: &AppState,
    spec: &IngestionWorkerSpec,
    start_network: bool,
    reconciled_at: DateTime<Utc>,
    actions: &mut Vec<ConnectorWorkerReconcileAction>,
) -> Result<(), ApiError> {
    let decision = connector_worker_start_decision(spec);
    match decision {
        ConnectorWorkerStartDecision::StartMqtt => {
            let signature = connector_worker_signature(spec);
            let existing = remove_connector_worker_handle_if_changed_or_finished(
                state,
                spec.connector_id,
                &signature,
            );

            match existing {
                ExistingConnectorWorker::Same => {
                    update_connector_worker_runtime_status(state, spec.connector_id, |worker| {
                        worker.last_reconciled_at = Some(reconciled_at);
                    });
                    actions.push(connector_worker_action(spec, "unchanged", None));
                }
                ExistingConnectorWorker::Stopped { reason } => {
                    let restart_count = connector_worker_restart_count(state, spec.connector_id);
                    start_connector_worker_from_spec(
                        state,
                        spec,
                        signature,
                        start_network,
                        reconciled_at,
                        restart_count + 1,
                    )
                    .await?;
                    let action = if reason == "config_changed" {
                        "restarted"
                    } else {
                        "started"
                    };
                    record_connector_worker_event(
                        state,
                        if action == "restarted" {
                            "aion:ConnectorWorkerRestarted"
                        } else {
                            "aion:ConnectorWorkerStarted"
                        },
                        EventSeverity::Info,
                        Some(format!("Connector worker {action}")),
                        connector_worker_event_metadata(spec, Some(reason)),
                    )?;
                    actions.push(connector_worker_action(spec, action, Some(reason)));
                }
                ExistingConnectorWorker::None => {
                    let restart_count = connector_worker_restart_count(state, spec.connector_id);
                    start_connector_worker_from_spec(
                        state,
                        spec,
                        signature,
                        start_network,
                        reconciled_at,
                        restart_count,
                    )
                    .await?;
                    record_connector_worker_event(
                        state,
                        "aion:ConnectorWorkerStarted",
                        EventSeverity::Info,
                        Some("Connector worker started".to_string()),
                        connector_worker_event_metadata(spec, None),
                    )?;
                    actions.push(connector_worker_action(spec, "started", None));
                }
            }
        }
        ConnectorWorkerStartDecision::Skip => {
            let stopped = stop_connector_worker_if_running(
                state,
                spec,
                reconciled_at,
                "connector_not_startable",
            )?;
            let mut status = connector_runtime_status_from_spec(spec);
            if stopped {
                status.status = ConnectorWorkerRuntimeState::Stopped;
                status.stopped_at = Some(reconciled_at);
            }
            status.last_reconciled_at = Some(reconciled_at);
            set_connector_worker_runtime_status(state, status);
            actions.push(connector_worker_action(spec, "skipped", None));
        }
        ConnectorWorkerStartDecision::Invalid | ConnectorWorkerStartDecision::Unsupported => {
            stop_connector_worker_if_running(state, spec, reconciled_at, "invalid_or_unsupported")?;
            let mut status = connector_runtime_status_from_spec(spec);
            status.last_reconciled_at = Some(reconciled_at);
            set_connector_worker_runtime_status(state, status);
            actions.push(connector_worker_action(
                spec,
                if decision == ConnectorWorkerStartDecision::Invalid {
                    "invalid"
                } else {
                    "unsupported"
                },
                None,
            ));
        }
        ConnectorWorkerStartDecision::PlannedOnly => {
            stop_connector_worker_if_running(state, spec, reconciled_at, "not_runtime_worker")?;
            let mut status = connector_runtime_status_from_spec(spec);
            status.last_reconciled_at = Some(reconciled_at);
            set_connector_worker_runtime_status(state, status);
            actions.push(connector_worker_action(spec, "planned", None));
        }
    }

    Ok(())
}

enum ExistingConnectorWorker {
    None,
    Same,
    Stopped { reason: &'static str },
}

fn remove_connector_worker_handle_if_changed_or_finished(
    state: &AppState,
    connector_id: Uuid,
    expected_signature: &ConnectorWorkerSignature,
) -> ExistingConnectorWorker {
    let Ok(mut handles) = state.connector_worker_handles.write() else {
        return ExistingConnectorWorker::None;
    };

    let Some(handle) = handles.get(&connector_id) else {
        return ExistingConnectorWorker::None;
    };

    if handle.task.is_finished() {
        handles.remove(&connector_id);
        return ExistingConnectorWorker::Stopped { reason: "finished" };
    }

    if &handle.signature == expected_signature {
        return ExistingConnectorWorker::Same;
    }

    if let Some(handle) = handles.remove(&connector_id) {
        handle.task.abort();
    }
    ExistingConnectorWorker::Stopped {
        reason: "config_changed",
    }
}

async fn start_connector_worker_from_spec(
    state: &AppState,
    spec: &IngestionWorkerSpec,
    signature: ConnectorWorkerSignature,
    start_network: bool,
    started_at: DateTime<Utc>,
    restart_count: u32,
) -> Result<(), ApiError> {
    let mut status = connector_runtime_status_from_spec(spec);
    status.status = if start_network {
        ConnectorWorkerRuntimeState::Starting
    } else {
        ConnectorWorkerRuntimeState::Planned
    };
    status.started_at = if start_network {
        Some(started_at)
    } else {
        None
    };
    status.restart_count = restart_count;
    status.last_reconciled_at = Some(started_at);
    status.last_error = if start_network {
        None
    } else {
        Some("network start skipped by test/dry-run mode".to_string())
    };
    set_connector_worker_runtime_status(state, status);

    if !start_network {
        return Ok(());
    }

    let connector_metadata = mqtt_ingest::MqttConnectorMetadata {
        connector_id: spec.connector_id,
        connector_key: spec.connector_key.clone(),
        connector_profile: spec.connector_profile.clone(),
    };
    let mqtt_config = if let Some(secret_ref_id) = spec.secret_ref_id {
        let secret = state
            .storage
            .get_connector_secret(state.tenant_id, secret_ref_id)?
            .ok_or_else(|| {
                ApiError::bad_request(
                    "connector secret_ref_id does not reference an existing connector secret",
                )
            })?;
        if secret.secret_type != ConnectorSecretType::MqttBasicAuth {
            return Err(ApiError::bad_request(
                "dynamic MQTT connector workers currently support only mqtt_basic_auth secrets",
            ));
        }
        mqtt_ingest::MqttIngestConfig::for_connector_with_basic_auth(
            spec.broker_url.clone().unwrap_or_default(),
            spec.client_id
                .clone()
                .unwrap_or_else(|| format!("aioncore-connector-{}", spec.connector_id)),
            spec.topic_filter.clone().unwrap_or_default(),
            spec.payload_format.clone(),
            spec.content_type.clone(),
            secret.username,
            secret.secret_value,
            connector_metadata,
        )
    } else {
        mqtt_ingest::MqttIngestConfig::for_connector(
            spec.broker_url.clone().unwrap_or_default(),
            spec.client_id
                .clone()
                .unwrap_or_else(|| format!("aioncore-connector-{}", spec.connector_id)),
            spec.topic_filter.clone().unwrap_or_default(),
            spec.payload_format.clone(),
            spec.content_type.clone(),
            connector_metadata,
        )
    };

    match mqtt_ingest::start_connector_worker(state.clone(), mqtt_config).await {
        Ok(task) => {
            if let Ok(mut handles) = state.connector_worker_handles.write() {
                handles.insert(spec.connector_id, ConnectorWorkerHandle { signature, task });
            }
            Ok(())
        }
        Err(err) => {
            let message = err.to_string();
            update_connector_worker_runtime_status(state, spec.connector_id, |worker| {
                worker.status = ConnectorWorkerRuntimeState::Error;
                worker.last_error = Some(message.clone());
                worker.last_failed_ingest_at = Some(Utc::now());
            });
            record_connector_worker_event(
                state,
                "aion:ConnectorWorkerReconcileFailed",
                EventSeverity::Error,
                Some("Connector worker failed to start".to_string()),
                metadata_with_connector(
                    json!({
                        "reason": "start_failed",
                        "error": message
                    }),
                    Some(connector_worker_event_metadata(spec, None)),
                ),
            )?;
            Ok(())
        }
    }
}

fn stop_connector_worker_if_running(
    state: &AppState,
    spec: &IngestionWorkerSpec,
    stopped_at: DateTime<Utc>,
    reason: &'static str,
) -> Result<bool, ApiError> {
    let handle = state
        .connector_worker_handles
        .write()
        .ok()
        .and_then(|mut handles| handles.remove(&spec.connector_id));

    let Some(handle) = handle else {
        return Ok(false);
    };

    handle.task.abort();
    update_connector_worker_runtime_status(state, spec.connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Stopped;
        worker.connected = false;
        worker.subscribed = false;
        worker.stopped_at = Some(stopped_at);
        worker.last_reconciled_at = Some(stopped_at);
    });
    record_connector_worker_event(
        state,
        "aion:ConnectorWorkerStopped",
        EventSeverity::Info,
        Some("Connector worker stopped".to_string()),
        connector_worker_event_metadata(spec, Some(reason)),
    )?;

    Ok(true)
}

fn stop_all_connector_workers(
    state: &AppState,
    stopped_at: DateTime<Utc>,
    actions: &mut Vec<ConnectorWorkerReconcileAction>,
) -> Result<(), ApiError> {
    let handles = state
        .connector_worker_handles
        .write()
        .map(|mut handles| handles.drain().collect::<Vec<_>>())
        .unwrap_or_default();
    for (connector_id, handle) in handles {
        handle.task.abort();
        update_connector_worker_runtime_status(state, connector_id, |worker| {
            worker.status = ConnectorWorkerRuntimeState::Stopped;
            worker.connected = false;
            worker.subscribed = false;
            worker.stopped_at = Some(stopped_at);
            worker.last_reconciled_at = Some(stopped_at);
            actions.push(ConnectorWorkerReconcileAction {
                connector_id,
                connector_key: worker.connector_key.clone(),
                action: "stopped".to_string(),
                reason: Some("connector_workers_disabled".to_string()),
            });
        });
    }
    Ok(())
}

fn stop_workers_missing_from_plan(
    state: &AppState,
    plan: &IngestionWorkerPlan,
    stopped_at: DateTime<Utc>,
    actions: &mut Vec<ConnectorWorkerReconcileAction>,
) -> Result<(), ApiError> {
    let planned_ids = plan
        .specs
        .iter()
        .map(|spec| spec.connector_id)
        .collect::<HashSet<_>>();
    let stale_ids = state
        .connector_worker_handles
        .read()
        .map(|handles| {
            handles
                .keys()
                .copied()
                .filter(|id| !planned_ids.contains(id))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    for connector_id in stale_ids {
        let handle = state
            .connector_worker_handles
            .write()
            .ok()
            .and_then(|mut handles| handles.remove(&connector_id));
        if let Some(handle) = handle {
            handle.task.abort();
        }
        update_connector_worker_runtime_status(state, connector_id, |worker| {
            worker.status = ConnectorWorkerRuntimeState::Stopped;
            worker.connected = false;
            worker.subscribed = false;
            worker.stopped_at = Some(stopped_at);
            worker.last_reconciled_at = Some(stopped_at);
            actions.push(ConnectorWorkerReconcileAction {
                connector_id,
                connector_key: worker.connector_key.clone(),
                action: "stopped".to_string(),
                reason: Some("connector_removed_from_plan".to_string()),
            });
        });
    }

    Ok(())
}

pub(crate) fn connector_worker_signature(spec: &IngestionWorkerSpec) -> ConnectorWorkerSignature {
    ConnectorWorkerSignature {
        broker_url: spec.broker_url.clone(),
        client_id: spec.client_id.clone(),
        topic_filter: spec.topic_filter.clone(),
        payload_format: spec.payload_format.clone(),
        content_type: spec.content_type.clone(),
        secret_ref_id: spec.secret_ref_id,
        connector_profile: spec.connector_profile.clone(),
    }
}

fn connector_worker_restart_count(state: &AppState, connector_id: Uuid) -> u32 {
    state
        .connector_worker_statuses
        .read()
        .ok()
        .and_then(|statuses| {
            statuses
                .get(&connector_id)
                .map(|status| status.restart_count)
        })
        .unwrap_or(0)
}

fn connector_worker_event_metadata(spec: &IngestionWorkerSpec, reason: Option<&str>) -> Value {
    let mut metadata = json!({
        "connector_id": spec.connector_id,
        "connector_key": spec.connector_key,
        "connector_type": spec.connector_type,
        "connector_profile": spec.connector_profile,
        "worker_kind": spec.worker_kind,
        "broker_url": spec.broker_url,
        "topic_filter": spec.topic_filter,
        "payload_format": spec.payload_format,
        "secret_ref_id": spec.secret_ref_id,
        "secret_configured": spec.secret_ref_id.is_some()
    });
    if let (Some(object), Some(reason)) = (metadata.as_object_mut(), reason) {
        object.insert("reason".to_string(), json!(reason));
    }
    metadata
}

pub(crate) fn connector_workers_enabled(state: &AppState) -> bool {
    state
        .connector_workers_enabled
        .read()
        .map(|guard| *guard)
        .unwrap_or(false)
}

pub(crate) fn set_connector_workers_enabled(state: &AppState, enabled: bool) {
    if let Ok(mut guard) = state.connector_workers_enabled.write() {
        *guard = enabled;
    }
}

pub(crate) fn connector_worker_spec(
    state: &AppState,
    connector: IngestionConnector,
) -> Result<IngestionWorkerSpec, ApiError> {
    let mut validation_issues = Vec::new();
    let worker_kind = match &connector.connector_type {
        IngestionConnectorType::Http => IngestionWorkerKind::HttpListener,
        IngestionConnectorType::Mqtt => IngestionWorkerKind::MqttSubscriber,
        IngestionConnectorType::Future => IngestionWorkerKind::Unsupported,
    };

    let status = if !connector.enabled {
        IngestionWorkerSpecStatus::Skipped
    } else {
        if let Some(secret_ref_id) = connector.secret_ref_id {
            match state
                .storage
                .get_connector_secret(state.tenant_id, secret_ref_id)?
            {
                Some(secret) if secret.secret_type != ConnectorSecretType::MqttBasicAuth => {
                    validation_issues.push(worker_issue(
                        "unsupported_secret_type",
                        "dynamic MQTT connector workers currently support only mqtt_basic_auth secrets",
                    ));
                }
                Some(_) => {}
                None => validation_issues.push(worker_issue(
                    "missing_secret_ref",
                    "connector secret_ref_id does not reference an existing connector secret",
                )),
            }
        }
        match &connector.connector_type {
            IngestionConnectorType::Http => {
                if connector.connector_profile == ConnectorProfile::TtnV3 {
                    validation_issues.push(worker_issue(
                        "invalid_connector_type",
                        "TTN v3 connector workers require connector_type mqtt",
                    ));
                }
                if connector
                    .http_path
                    .as_deref()
                    .or(connector.endpoint.as_deref())
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    validation_issues.push(worker_issue(
                        "missing_http_path",
                        "HTTP connectors require http_path or endpoint before a listener can be planned",
                    ));
                }
                if validation_issues.is_empty() {
                    IngestionWorkerSpecStatus::Planned
                } else {
                    IngestionWorkerSpecStatus::Invalid
                }
            }
            IngestionConnectorType::Mqtt => {
                if connector
                    .broker_url
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    validation_issues.push(worker_issue(
                        "missing_broker_url",
                        "MQTT connectors require broker_url before a subscriber can be planned",
                    ));
                }
                if connector
                    .topic_filter
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    validation_issues.push(worker_issue(
                        "missing_topic_filter",
                        "MQTT connectors require topic_filter before a subscriber can be planned",
                    ));
                } else if connector.connector_profile == ConnectorProfile::TtnV3
                    && !connector
                        .topic_filter
                        .as_deref()
                        .map(crate::is_plausible_ttn_topic_filter)
                        .unwrap_or(false)
                {
                    validation_issues.push(worker_issue(
                        "implausible_ttn_topic_filter",
                        "TTN v3 topic_filter should look like v3/{application_id}/devices/+/up",
                    ));
                }
                if connector.connector_profile == ConnectorProfile::TtnV3
                    && !connector
                        .payload_format
                        .as_deref()
                        .map(crate::is_ttn_uplink_payload_format)
                        .unwrap_or(false)
                {
                    validation_issues.push(worker_issue(
                        "unsupported_ttn_payload_format",
                        "TTN v3 connector workers require payload_format = ttn-uplink-json in this milestone",
                    ));
                }
                if validation_issues.iter().any(|issue| {
                    matches!(
                        issue.code.as_str(),
                        "missing_broker_url"
                            | "missing_topic_filter"
                            | "invalid_connector_type"
                            | "implausible_ttn_topic_filter"
                            | "missing_secret_ref"
                            | "unsupported_secret_type"
                            | "unsupported_ttn_payload_format"
                    )
                }) {
                    IngestionWorkerSpecStatus::Invalid
                } else {
                    IngestionWorkerSpecStatus::Planned
                }
            }
            IngestionConnectorType::Future => {
                validation_issues.push(worker_issue(
                    "unsupported_connector_type",
                    "future connector types do not have runtime worker support yet",
                ));
                IngestionWorkerSpecStatus::Unsupported
            }
        }
    };

    Ok(IngestionWorkerSpec {
        connector_id: connector.id,
        connector_key: connector.connector_key,
        connector_type: connector.connector_type,
        connector_profile: connector.connector_profile,
        enabled: connector.enabled,
        worker_kind,
        broker_url: connector.broker_url,
        client_id: connector.client_id,
        topic_filter: connector.topic_filter,
        http_path: connector.http_path.or(connector.endpoint),
        payload_format: connector.payload_format,
        content_type: connector.content_type,
        secret_ref_id: connector.secret_ref_id,
        status,
        validation_issues,
        metadata: connector.metadata,
    })
}

fn worker_issue(
    code: impl Into<String>,
    message: impl Into<String>,
) -> IngestionWorkerValidationIssue {
    IngestionWorkerValidationIssue {
        code: code.into(),
        message: message.into(),
    }
}

pub(crate) fn set_connector_worker_runtime_status(
    state: &AppState,
    status: ConnectorWorkerRuntimeStatus,
) {
    if let Ok(mut statuses) = state.connector_worker_statuses.write() {
        statuses.insert(status.connector_id, status);
    }
}

pub(crate) fn update_connector_worker_runtime_status(
    state: &AppState,
    connector_id: Uuid,
    update: impl FnOnce(&mut ConnectorWorkerRuntimeStatus),
) {
    if let Ok(mut statuses) = state.connector_worker_statuses.write() {
        if let Some(status) = statuses.get_mut(&connector_id) {
            update(status);
        }
    }
}

pub(crate) fn mark_connector_worker_starting(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Starting;
        worker.connected = false;
        worker.subscribed = false;
        worker.last_error = None;
        worker.started_at = worker.started_at.or_else(|| Some(Utc::now()));
    });
}

pub(crate) fn mark_connector_worker_connected(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Degraded;
        worker.connected = true;
        worker.last_error = None;
    });
}

pub(crate) fn mark_connector_worker_subscribed(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        if worker.reconnect_attempts > 0 {
            worker.last_reconnect_at = Some(Utc::now());
        }
        worker.status = ConnectorWorkerRuntimeState::Running;
        worker.connected = true;
        worker.subscribed = true;
        worker.last_error = None;
        worker.next_reconnect_at = None;
    });
}

pub(crate) fn mark_connector_worker_failure(state: &AppState, connector_id: Uuid, message: String) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Degraded;
        worker.connected = false;
        worker.subscribed = false;
        worker.last_error = Some(message);
        worker.last_disconnect_at = Some(Utc::now());
        worker.last_failed_ingest_at = Some(Utc::now());
    });
}

pub(crate) fn mark_connector_worker_reconnect_scheduled(
    state: &AppState,
    connector_id: Uuid,
    message: String,
    delay: std::time::Duration,
) -> DateTime<Utc> {
    let next_reconnect_at =
        Utc::now() + Duration::from_std(delay).unwrap_or_else(|_| Duration::seconds(60));
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.status = ConnectorWorkerRuntimeState::Reconnecting;
        worker.connected = false;
        worker.subscribed = false;
        worker.reconnect_attempts = worker.reconnect_attempts.saturating_add(1);
        worker.last_error = Some(message);
        worker.next_reconnect_at = Some(next_reconnect_at);
    });
    next_reconnect_at
}

pub(crate) fn mark_connector_worker_message(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.last_message_at = Some(Utc::now());
    });
}

pub(crate) fn mark_connector_worker_ingest_success(state: &AppState, connector_id: Uuid) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.last_successful_ingest_at = Some(Utc::now());
        worker.last_error = None;
    });
}

pub(crate) fn mark_connector_worker_ingest_failed(
    state: &AppState,
    connector_id: Uuid,
    message: String,
) {
    update_connector_worker_runtime_status(state, connector_id, |worker| {
        worker.last_failed_ingest_at = Some(Utc::now());
        worker.last_error = Some(message);
        if worker.status == ConnectorWorkerRuntimeState::Running {
            worker.status = ConnectorWorkerRuntimeState::Degraded;
        }
    });
}

fn parse_bool_env_value(
    value: Option<&str>,
    default: bool,
    variable_name: &str,
) -> Result<bool, StartupError> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(default),
        Some(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        Some(other) => Err(StartupError::backend_initialization(format!(
            "invalid boolean value '{other}' for {variable_name}"
        ))),
    }
}
