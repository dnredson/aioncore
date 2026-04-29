# Ingestion Model

AionCore ingestion is payload-agnostic. HTTP and MQTT ingestion both preserve the raw message first and then normalize supported payloads into canonical observations.

## Core Rule

Raw messages must always be stored before normalization.

This preserves auditability and enables later replay when decoders, payload profiles, or entity registrations change.

## HTTP Ingestion

Current HTTP endpoint:

```text
POST /ingest/http
```

HTTP ingestion accepts a producer entity, a feature of interest, a payload format, and the payload itself. The current runtime stores the raw message, selects the decoder, produces canonical observations, and then updates raw-message status.

The legacy endpoint remains available and does not require a registered connector. This is the default ingestion mode for simple local deployments.

HTTP payload formats supported by the local runtime:

- `senml-json`
- `ultralight`
- `canonical-json`

UltraLight HTTP ingestion can use the producer entity payload profile mapping when one is stored.

HTTP flow:

```text
Receive request
  -> verify referenced entities
  -> store raw message
  -> select decoder
  -> decode payload
  -> write canonical observations
  -> update raw message status
  -> emit ingestion events
```

## Ingestion Connector Registry

AionCore has an `IngestionConnector` registry. An ingestion connector describes where data comes from; a connector profile describes how to interpret source-specific semantics.

Connector fields include:

- `connector_key`
- `connector_type`: `http`, `mqtt`, or `future`
- `connector_profile`: `generic-aion-mqtt`, `generic-mqtt`, `ttn-v3`, or `custom`
- runtime defaults such as `protocol`, `endpoint`, `broker_url`, `client_id`, `topic_filter`, `http_path`, `payload_format`, `content_type`, default producer entity, and default feature of interest
- `enabled`
- metadata and timestamps

Connector registry endpoints:

```text
POST /ingestion/connectors
GET /ingestion/connectors
GET /ingestion/connectors/{connector_id}
PUT /ingestion/connectors/{connector_id}/enable
PUT /ingestion/connectors/{connector_id}/disable
GET /ingestion/connectors/{connector_id}/status
GET /ingestion/workers/plan
```

Connector-aware HTTP ingestion:

```text
POST /ingestion/connectors/{connector_id}/ingest
```

When this endpoint is used, missing request values can be resolved from connector defaults, including payload format, content type, protocol, producer entity, and feature of interest. Existing `POST /ingest/http` continues to work without a connector.

Raw messages and ingestion events created through connector-aware ingestion include connector metadata such as connector ID, connector key, connector profile, source endpoint, and topic/path. The current persisted raw-message schema stores this metadata in raw-message headers.

Connector status is currently derived from registry state:

- disabled connectors report `disabled`
- enabled HTTP connectors report `ready`
- enabled MQTT connectors report `planned`, `ready`, `degraded`, `skipped`, or `error` depending on connector-worker runtime state
- future connectors report `unsupported`

The registry is supported by in-memory storage and PostgreSQL persistence. Durable connector configuration allows startup logic to reload enabled connectors and start source-specific workers when connector workers are enabled.

The existing environment-variable MQTT configuration remains the default runtime MQTT connector path. It is independent of connector-based MQTT workers.

## Worker Planner

The worker planner is a read-only inspection endpoint:

```text
GET /ingestion/workers/plan
```

It reads registered ingestion connectors and returns the worker specifications AionCore would intend to run. It does not start workers, open network connections, subscribe to MQTT topics, or call external services.

Planned worker specs include connector identity, connector type/profile, worker kind, source settings, payload defaults, status, validation issues, and connector metadata.

Worker kinds:

- `http_listener`
- `mqtt_subscriber`
- `unsupported`

Spec status values:

- `planned`: connector is enabled and has enough configuration for a worker.
- `skipped`: connector is disabled.
- `invalid`: connector is enabled but missing required fields.
- `unsupported`: connector type is not supported by the current runtime planner.

Enabled HTTP connectors plan `http_listener` specs when they have `http_path` or `endpoint`. Enabled MQTT connectors plan `mqtt_subscriber` specs when they have `broker_url` and `topic_filter`. TTN v3 connectors also plan MQTT subscriber specs, but include a validation note when their payload format is not implemented by the current decoder path.

`GET /ready` includes a cheap worker-plan summary with planned, invalid, and unsupported counts. Connector plan issues do not make readiness fail in this milestone.

## Connector-Based Worker Runtime

Dynamic connector workers are opt-in:

```text
AIONCORE_CONNECTOR_WORKERS_ENABLED=true|false
```

The default is `false`. When disabled, AionCore does not start connector-based workers and `/ingestion/workers/plan` remains read-only.

When enabled, startup reads the planner and starts one MQTT subscriber worker for each valid enabled MQTT connector whose profile is:

- `generic-aion-mqtt`
- `generic-mqtt`

Each connector worker uses the connector `broker_url`, `client_id`, `topic_filter`, `payload_format`, and `content_type` defaults. Raw messages and ingestion events include `connector_id`, `connector_key`, and `connector_profile`.

Connector worker runtime state is exposed through:

```text
GET /ingestion/workers/status
```

Manual reconciliation is exposed through:

```text
POST /ingestion/workers/reconcile
```

Status values include:

- `planned`
- `starting`
- `running`
- `stopped`
- `degraded`
- `skipped`
- `invalid`
- `error`
- `unsupported`

Worker status entries include connector identity, worker kind, source configuration, last error, message/ingest timestamps, `started_at`, `stopped_at`, `restart_count`, and `last_reconciled_at`.

`GET /ready` includes a `connector_workers` summary with:

- `enabled`
- `total`
- `running`
- `stopped`
- `degraded`
- `skipped`
- `invalid`
- `errors`

Connector workers do not replace the env-var MQTT worker. If `AIONCORE_MQTT_ENABLED=true` and `AIONCORE_CONNECTOR_WORKERS_ENABLED=true`, both worker families may run.

TTN v3 connectors are planned but skipped by the dynamic runtime because TTN uplink decoding is not implemented yet. No network connection is attempted for TTN v3 connector workers in this milestone.

## Worker Manager And Reconciliation

The runtime connector worker manager tracks active connector MQTT worker tasks by `connector_id`. Reconciliation compares the current worker plan with the tracked runtime workers.

Reconciliation is triggered after:

- `POST /ingestion/connectors`
- `PATCH /ingestion/connectors/{connector_id}`
- `PUT /ingestion/connectors/{connector_id}/enable`
- `PUT /ingestion/connectors/{connector_id}/disable`
- `POST /ingestion/workers/reconcile`

Connector updates can change operational fields such as display name, enabled state, protocol, endpoint, broker URL, client ID, topic filter, HTTP path, payload format, content type, default entity IDs, and metadata. Immutable identity fields are not part of the update request: `id`, `tenant_id`, `connector_key`, `connector_type`, and `connector_profile`.

During reconciliation:

- valid enabled `generic-aion-mqtt` and `generic-mqtt` MQTT connectors start a worker if none is running;
- disabled connectors stop any tracked worker;
- invalid connectors remain stopped and report validation errors;
- TTN v3 connectors remain skipped until TTN decoding is implemented;
- if a tracked worker's relevant signature differs from the planned connector configuration, the worker is restarted.

Relevant signature fields are broker URL, client ID, topic filter, payload format, content type, and connector profile. Public connector updates trigger reconciliation, so changing one of those fields restarts the tracked connector worker when dynamic workers are enabled. If an update saves invalid worker configuration, reconciliation stops any tracked worker and reports `invalid` status with validation details.

Connector MQTT workers automatically retry broker disconnects and event-loop failures. The retry policy is a bounded exponential backoff starting at 1 second and capped at 60 seconds. While waiting, worker status is `reconnecting` and includes:

- `reconnect_attempts`
- `last_disconnect_at`
- `last_reconnect_at`
- `next_reconnect_at`
- `last_error`

Successful resubscription returns the worker to `running`. The environment-variable MQTT worker keeps its existing behavior and is not changed by connector-worker reconnect handling.

Worker lifecycle events include:

- `aion:IngestionConnectorUpdated`
- `aion:ConnectorWorkerStarted`
- `aion:ConnectorWorkerStopped`
- `aion:ConnectorWorkerRestarted`
- `aion:ConnectorWorkerSkipped`
- `aion:ConnectorWorkerReconcileFailed`
- `aion:ConnectorWorkerDisconnected`
- `aion:ConnectorWorkerReconnectScheduled`
- `aion:ConnectorWorkerReconnected`

## MQTT Ingestion

MQTT ingestion is now available as an opt-in local-runtime foundation. It remains disabled unless `AIONCORE_MQTT_ENABLED=true`.

When MQTT is disabled, the API does not connect to a broker and `/ready` reports `mqtt.enabled = false`.

MVP topic convention:

```text
aioncore/{producer_entity_id}/{feature_of_interest_id}/data
```

The topic segments are URL-decoded before UUID parsing. If the topic cannot be parsed, AionCore still preserves the raw message when possible and records a failed-ingestion event.

For every MQTT message, AionCore validates that:

- the topic has the expected shape;
- `producer_entity_id` exists;
- `feature_of_interest_id` exists.

Rejected MQTT messages preserve the raw payload when feasible, emit `aion:MqttMessageRejected` or `aion:PayloadIngestionFailed`, and do not create observations.

MQTT payload formats supported by this milestone:

- `senml-json`
- `ultralight`
- `canonical-json`

MQTT format selection:

- explicit `AIONCORE_MQTT_PAYLOAD_FORMAT`
- default `canonical-json` when nothing is configured

UltraLight MQTT ingestion uses the stored producer entity payload profile mapping when one exists. Inline mapping is not supported over raw MQTT in this milestone.

SenML MQTT payloads can decode without a mapping. UltraLight MQTT payloads require a stored `PayloadProfile.attribute_mapping` for the producer entity. If the mapping is missing, the raw message is marked failed and the event explains that UltraLight requires the producer payload profile mapping.

Optional broker credentials:

- `AIONCORE_MQTT_USERNAME`
- `AIONCORE_MQTT_PASSWORD`

These credentials authenticate the AionCore worker to the broker. They are not per-device authorization and do not replace future device-level MQTT authentication.

MQTT readiness state is exposed through `/ready`:

- `enabled`
- `connected`
- `subscribed`
- `broker_url`
- `topic_filter`
- `last_error`
- `last_message_at`
- `last_successful_ingest_at`
- `last_failed_ingest_at`

When MQTT is enabled, readiness is only ready after the worker is connected and subscribed. If the worker cannot connect or subscribe, the API can still serve HTTP requests, but `/ready` returns not ready with MQTT details.

MQTT flow:

```text
Broker receives message
  -> AionCore MQTT worker subscribes
  -> worker stores raw message
  -> worker decodes payload
  -> worker writes canonical observations
  -> worker updates raw-message status
  -> worker emits ingestion events
```

Connector-based MQTT workers support one worker per enabled generic MQTT connector so multiple controlled brokers can run side by side. Provider-specific profiles such as TTN v3 are modeled but not dynamically consumed yet.

## The Things Stack Profile

`connector_profile = "ttn-v3"` is accepted as a connector profile for The Things Network / The Things Stack MQTT uplinks.

Example TTN connector semantics:

- `broker_url`: `mqtt://eu1.cloud.thethings.network:1883` or another regional/tenant endpoint
- `topic_filter`: `v3/{application_id}/devices/+/up`
- `payload_format`: `ttn-uplink-json`

Full TTN uplink decoding is not implemented yet. Future TTN adapter behavior should:

- map TTN `device_id` to producer entities;
- map `decoded_payload` fields to canonical observations;
- preserve uplink metadata such as RSSI, SNR, gateway IDs, frame counters, and timestamps;
- support topic differences across tenant, application, and regional endpoint conventions.

## Limitations

- Connector registry persistence is available for in-memory and PostgreSQL storage.
- Dynamic MQTT workers are implemented only for `generic-aion-mqtt` and `generic-mqtt` connector profiles and must be explicitly enabled.
- TTN/The Things Stack live connectivity and full uplink decoding are not implemented yet.
- Secrets storage is not implemented yet.
- Connector authentication is not implemented yet.
- MQTT broker username/password authentication is supported for the worker only.
- Per-device MQTT authorization is not implemented yet.
- TLS/mTLS is not implemented yet.
- MQTT command publishing is not implemented yet.
- Production broker scaling is not implemented yet.
- The env-var MQTT worker still has no reconnect fallback; if it is enabled and the broker cannot be reached, readiness reports MQTT as not ready.
- HTTP and MQTT ingest into the same canonical-observation model, but the runtime still defaults to in-memory storage unless PostgreSQL is selected explicitly.
