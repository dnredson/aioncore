# Ingestion Model

AionCore ingestion is payload-agnostic. HTTP and MQTT ingestion both preserve the raw message first and then normalize supported payloads into canonical observations.

Future optional edge/fog collection is described in [Aion Edge Adapter Model](EDGE_ADAPTER_MODEL.md). That adapter model does not replace AionCore server-side ingestion connectors.

Optional reliable upstream flow-engine compatibility, including NiFi and MiNiFi envelope and provenance conventions, is described in [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md). That model does not change current ingestion behavior and does not add a NiFi dependency.

## Core Rule

Raw messages must always be stored before normalization.

This preserves auditability and enables later replay when decoders, payload profiles, or entity registrations change.

For external reliable-ingestion runtimes such as NiFi, MiNiFi, SmartSentinel, or future edge adapters, AionCore should preserve external provenance in raw-message headers or equivalent metadata rather than replacing core ingest timestamps or source semantics.

## HTTP Ingestion

Current HTTP endpoint:

```text
POST /ingest/http
POST /ingest/reliable
POST /ingest/batch
```

HTTP ingestion accepts a producer entity, a feature of interest, a payload format, and the payload itself. The current runtime stores the raw message, selects the decoder, produces canonical observations, and then updates raw-message status.

The legacy endpoint remains available and does not require a registered connector. This is the default ingestion mode for simple local deployments.

Reliable HTTP ingestion is additive:

- `POST /ingest/reliable` accepts the reliable envelope contract documented in [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md)
- it preserves upstream provenance in `RawMessage.headers` and `Event.metadata`
- it applies tenant-scoped idempotency-key lookup when `idempotency_key` is present
- it returns `duplicate = true` with the existing `raw_message_id` instead of creating duplicate raw messages or observations
- it does not change existing `POST /ingest/http` behavior

Batch reliable ingestion is also additive:

- `POST /ingest/batch` accepts multiple reliable-ingestion items in one request
- items are processed sequentially and independently
- item results are returned individually as `accepted`, `duplicate`, or `failed`
- batch-level provenance such as `source_system`, `source_id`, `sync_session_id`, `connectivity_state`, `external_flow_id`, `external_flow_name`, and `metadata` can be inherited by items when item fields are absent
- tenant-scoped idempotency applies per item and remains isolated across tenants
- `continue_on_error` defaults to `true`; when `false`, processing stops after the first failed item
- the runtime does not create a global transaction for the full batch
- the runtime emits `aion:ReliableBatchIngested` as a batch-level audit event

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

Reliable HTTP flow adds:

```text
Receive reliable envelope
  -> resolve tenant-scoped idempotency key when present
  -> if duplicate: return existing raw_message_id with duplicate=true
  -> otherwise store raw message first
  -> decode payload
  -> write canonical observations
  -> preserve external.* provenance metadata on raw message and event records
```

Batch reliable HTTP flow adds:

```text
Receive reliable batch
  -> validate item count and batch shape
  -> resolve authenticated tenant
  -> for each item in order:
       -> inherit batch-level provenance when item fields are absent
       -> resolve tenant-scoped idempotency key when present
       -> if duplicate: return existing raw_message_id with duplicate=true for that item
       -> otherwise store raw message first, decode, normalize, and emit existing item-level events
  -> optionally stop early on first failure when continue_on_error=false
  -> emit aion:ReliableBatchIngested batch-level audit event
```

## Ingestion Connector Registry

AionCore has an `IngestionConnector` registry. An ingestion connector describes where data comes from; a connector profile describes how to interpret source-specific semantics.

Connector fields include:

- `connector_key`
- `connector_type`: `http`, `mqtt`, or `future`
- `connector_profile`: `generic-aion-mqtt`, `generic-mqtt`, `ttn-v3`, or `custom`
- runtime defaults such as `protocol`, `endpoint`, `broker_url`, `client_id`, `topic_filter`, `http_path`, `payload_format`, `content_type`, default producer entity, and default feature of interest
- optional `secret_ref_id` pointing to a connector secret
- `enabled`
- metadata and timestamps

Connector registry endpoints:

```text
POST /ingestion/connectors
GET /ingestion/connectors
GET /ingestion/connectors/{connector_id}
PATCH /ingestion/connectors/{connector_id}
PUT /ingestion/connectors/{connector_id}/enable
PUT /ingestion/connectors/{connector_id}/disable
GET /ingestion/connectors/{connector_id}/status
GET /ingestion/connectors/{connector_id}/validate
GET /ingestion/workers/plan
```

Connector secret endpoints:

```text
POST /secrets/connectors
GET /secrets/connectors
GET /secrets/connectors/{secret_id}
DELETE /secrets/connectors/{secret_id}
```

Connector secrets are tenant-scoped credential references for connector workers. A secret includes `secret_key`, `secret_type`, optional `username`, write-only `secret_value`, metadata, and timestamps. API responses never include `secret_value`. Connector records store only `secret_ref_id`, not raw usernames/passwords beyond non-secret connector fields.

TTN device mapping endpoints:

```text
POST /ingestion/connectors/{connector_id}/ttn-device-mappings
GET /ingestion/connectors/{connector_id}/ttn-device-mappings
GET /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}
PATCH /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}
DELETE /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}
PUT /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}/enable
PUT /ingestion/connectors/{connector_id}/ttn-device-mappings/{mapping_id}/disable
```

TTN device mappings are explicit tenant-scoped rules for `ttn-v3` connectors. A mapping links a connector, optional TTN application ID, and TTN device ID to an existing `producer_entity_id` and optional `feature_of_interest_id`. They do not create entities.

Mapping updates can change `ttn_application_id`, `ttn_device_id`, `producer_entity_id`, `feature_of_interest_id`, `enabled`, and `metadata`. Mapping identity fields are immutable: `id`, `tenant_id`, and `connector_id`. Deletes remove the mapping rule from future resolution and do not delete entities, raw messages, observations, or events.

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

## Connector Validation

Connector validation is exposed through:

```text
GET /ingestion/connectors/{connector_id}/validate
GET /ingestion/connectors/{connector_id}/ttn-live-readiness-plan
POST /ingestion/connectors/{connector_id}/ttn-live-validate
```

For `connector_profile = "ttn-v3"`, validation is deterministic and non-network. It does not connect to TTN, authenticate to a broker, subscribe to topics, verify accounts, or validate TLS/mTLS. The response includes:

- `connector_id`
- `connector_key`
- `valid`
- `readiness`: `ready`, `degraded`, or `invalid`
- `issues`
- `warnings`
- `detected_profile`
- `expected_topic_shape`
- `mapping_count`
- `enabled_mapping_count`
- `has_secret_ref`
- `secret_configured`
- `secret_type`
- `payload_format_supported`
- `operator_hints`
- `generated_at`

Blocking TTN validation issues include:

- `invalid_connector_type`: TTN v3 connectors must be MQTT connectors.
- `missing_broker_url`: no broker URL is configured.
- `missing_topic_filter`: no topic filter is configured.
- `implausible_ttn_topic_filter`: topic filter does not look like a The Things Stack uplink topic.
- `unsupported_ttn_payload_format`: payload format is not `ttn-uplink-json`.
- `secret_ref_not_found`: `secret_ref_id` does not reference an existing connector secret.
- `incompatible_secret_type`: referenced secret is not compatible with TTN MQTT basic auth.
- `missing_secret_username`: referenced `mqtt_basic_auth` secret does not include a username.
- `missing_secret_value`: referenced secret has no internal secret value.

The accepted TTN topic shape is intentionally simple and non-network: the topic filter should contain `v3/`, `/devices/`, and `/up`, such as `v3/{application_id}/devices/+/up`.

TTN validation warnings include:

- `missing_ttn_device_mappings`: no TTN device mappings exist for the connector.
- `no_enabled_ttn_device_mappings`: mappings exist, but none are enabled.
- `missing_secret_ref`: broker URL looks like a public TTN/The Things Stack endpoint and no connector secret reference is set.
- `connector_disabled`: the connector is disabled.

TTN credential diagnostics are redacted. Validation may report `secret_configured`, `secret_type`, and whether username/type/value shape is usable, but it never returns `secret_value`. Public TTN/The Things Stack MQTT endpoints commonly require credentials. AionCore expects those credentials to be stored through connector secrets, typically with `secret_type = "mqtt_basic_auth"`, a deployment/application-specific MQTT username, and a password or API token stored as the write-only `secret_value`.

TTN validation responses include generic `operator_hints`:

- public TTN/The Things Stack MQTT brokers typically require authentication;
- usernames are usually application-specific and may include tenant or deployment context;
- passwords or API tokens should be stored as connector secrets;
- topic filters should match the application/device uplink topic shape;
- this milestone performs no live credential or broker verification.

Validation readiness is derived from issues and warnings:

- `ready`: no issues, connector enabled, and at least one enabled mapping exists.
- `degraded`: no issues, but warnings are present, no enabled mapping exists, or the connector is disabled.
- `invalid`: at least one blocking issue is present.

Non-TTN connectors return the same response shape with a `profile_specific_validation_unavailable` warning. `/ready` does not fail because of TTN validation warnings or invalid connector configuration; readiness remains focused on storage and runtime health.

TTN live readiness planning is a dry run for future opt-in live validation. It never opens a socket, resolves DNS, authenticates, subscribes, validates credentials, or contacts TTN/The Things Stack. The response includes:

- `connector_id`
- `connector_key`
- `dry_run = true`
- `can_attempt_live_validation`
- `readiness`: `ready`, `degraded`, or `invalid`
- `checks`
- `blockers`
- `warnings`
- `required_operator_steps`
- `safe_to_connect`
- `generated_at`

Each check includes `check_key`, `description`, `status` (`pass`, `warn`, `fail`, or `skipped`), optional `reason`, and `future_live_check`.

Dry-run checks include:

- `connector_profile_is_ttn_v3`
- `connector_type_is_mqtt`
- `broker_url_present`
- `topic_filter_present`
- `topic_filter_plausibly_ttn`
- `payload_format_is_ttn_uplink_json`
- `secret_ref_present`
- `secret_ref_resolves`
- `secret_type_is_mqtt_basic_auth`
- `secret_username_present`
- `secret_value_present_internally`
- `at_least_one_enabled_ttn_mapping`
- `no_network_call_performed`

For live-readiness planning, missing broker URL, topic filter, compatible `mqtt_basic_auth` connector secret, `secret_ref_id`, `payload_format = "ttn-uplink-json"`, and enabled TTN device mappings are blockers. A disabled TTN connector is not safe to connect and returns an operator step to enable it. Non-TTN connectors return a not-applicable plan with `safe_to_connect = false`.

TTN live validation preflight is explicit opt-in through `POST /ingestion/connectors/{connector_id}/ttn-live-validate`. It is not called by `/ready`, worker planning, connector reconciliation, startup, or normal tests. The handler first runs the dry-run readiness plan. If `safe_to_connect = false`, it returns `result = "skipped"`, includes dry-run blockers in `errors`, and does not open a network connection.

The live validation request body supports:

- `timeout_seconds`: optional, defaults to 5, capped at 60.
- `expect_message`: optional, defaults to `false`.
- `client_id_suffix`: optional suffix appended to the configured or generated MQTT client ID.
- `dry_run_only`: optional, defaults to `false`.

When `dry_run_only = true`, the endpoint returns the live-validation response shape without a network attempt. When `expect_message = false`, live preflight success means AionCore connected to the configured MQTT broker and completed the subscription request. When `expect_message = true`, success also requires at least one matching MQTT publish before the timeout. Any received publish is used only to set `message_received = true`; it is not stored as a raw message, not decoded, not normalized into observations, and not returned as raw payload.

The live validation response includes:

- connector identity;
- `attempted_live_connection`;
- `dry_run_passed`;
- `connected`;
- `subscribed`;
- `message_received`;
- redacted or safe broker URL;
- topic filter;
- duration and timestamps;
- `result`: `success`, `failed`, or `skipped`;
- `errors`;
- `warnings`;
- dry-run plan summary;
- `secret_exposed = false`.

Security behavior:

- `secret_value` is never returned.
- Passwords/tokens are resolved internally only after the dry-run plan passes.
- Error text is sanitized before being returned.
- Preflight payloads are not persisted and are not sent to the normal ingestion pipeline.
- Events, when emitted, include only connector IDs, safe broker/topic metadata, result flags, and issue codes.

TTN live preflight events are:

- `aion:TtnLiveValidationStarted`
- `aion:TtnLiveValidationSucceeded`
- `aion:TtnLiveValidationFailed`
- `aion:TtnLiveValidationSkipped`

Optional live tests are ignored by default. Run them only when explicitly configured:

```powershell
$env:AIONCORE_TEST_TTN_LIVE = "1"
$env:AIONCORE_TEST_TTN_BROKER_URL = "mqtt://example:1883"
$env:AIONCORE_TEST_TTN_TOPIC_FILTER = "v3/demo-app/devices/+/up"
$env:AIONCORE_TEST_TTN_USERNAME = "demo-app@tenant"
$env:AIONCORE_TEST_TTN_PASSWORD = "replace-with-token"
$env:AIONCORE_TEST_TTN_APPLICATION_ID = "demo-app"
$env:AIONCORE_TEST_TTN_DEVICE_ID = "soil-node-01"
cargo test -p aion-api opt_in_ttn_live_validate_can_connect_when_env_is_configured -- --ignored
```

The current preflight supports the same plain `mqtt://host:port` URL shape as the local MQTT worker foundation. Production TLS/mTLS hardening remains future work.

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

Enabled HTTP connectors plan `http_listener` specs when they have `http_path` or `endpoint`. Enabled MQTT connectors plan `mqtt_subscriber` specs when they have `broker_url` and `topic_filter`. TTN v3 connectors also plan MQTT subscriber specs, but include validation issues for unsupported payload formats and implausible TTN topic filters.

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

Each connector worker uses the connector `broker_url`, `client_id`, `topic_filter`, `payload_format`, and `content_type` defaults. When `secret_ref_id` points to a `mqtt_basic_auth` connector secret, the dynamic MQTT worker resolves the secret internally and applies broker username/password authentication. Raw messages and ingestion events include `connector_id`, `connector_key`, and `connector_profile`, but never include secret values.

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
- `reconnecting`
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

Connector updates can change operational fields such as display name, enabled state, protocol, endpoint, broker URL, client ID, topic filter, HTTP path, payload format, content type, secret reference, default entity IDs, and metadata. Immutable identity fields are not part of the update request: `id`, `tenant_id`, `connector_key`, `connector_type`, and `connector_profile`.

During reconciliation:

- valid enabled `generic-aion-mqtt` and `generic-mqtt` MQTT connectors start a worker if none is running;
- disabled connectors stop any tracked worker;
- invalid connectors remain stopped and report validation errors;
- TTN v3 connectors with `payload_format = "ttn-uplink-json"` can plan/start MQTT subscriber workers when broker and TTN topic settings are valid;
- if a tracked worker's relevant signature differs from the planned connector configuration, the worker is restarted.

Relevant signature fields are broker URL, client ID, topic filter, payload format, content type, and connector profile. Public connector updates trigger reconciliation, so changing one of those fields restarts the tracked connector worker when dynamic workers are enabled. If an update saves invalid worker configuration, reconciliation stops any tracked worker and reports `invalid` status with validation details.

Connector MQTT workers automatically retry broker disconnects and event-loop failures. The retry policy is a bounded exponential backoff starting at 1 second and capped at 60 seconds. While waiting, worker status is `reconnecting` and includes:

- `reconnect_attempts`
- `last_disconnect_at`
- `last_reconnect_at`
- `next_reconnect_at`
- `last_error`

Successful resubscription returns the worker to `running`. The environment-variable MQTT worker keeps its existing behavior and is not changed by connector-worker reconnect handling.

If a connector references a missing secret or a currently unsupported secret type, reconciliation marks the worker invalid and reports a validation issue. Deleting a secret clears connector references in the local storage behavior and prevents future worker starts from using that secret.

Worker lifecycle events include:

- `aion:IngestionConnectorUpdated`
- `aion:ConnectorSecretCreated`
- `aion:ConnectorSecretDeleted`
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
- `ttn-uplink-json`

MQTT format selection:

- explicit `AIONCORE_MQTT_PAYLOAD_FORMAT`
- default `canonical-json` when nothing is configured

UltraLight MQTT ingestion uses the stored producer entity payload profile mapping when one exists. Inline mapping is not supported over raw MQTT in this milestone.

SenML MQTT payloads can decode without a mapping. UltraLight MQTT payloads require a stored `PayloadProfile.attribute_mapping` for the producer entity. If the mapping is missing, the raw message is marked failed and the event explains that UltraLight requires the producer payload profile mapping.

Optional broker credentials:

- `AIONCORE_MQTT_USERNAME`
- `AIONCORE_MQTT_PASSWORD`

These credentials authenticate the AionCore worker to the broker. They are not per-device authorization and do not replace future device-level MQTT authentication.

Dynamic connector-worker broker authentication uses connector secret references instead of connector fields. Only `mqtt_basic_auth` is consumed by the current MQTT worker. `token`, `api_key`, and `custom` are persisted for future adapters but are not applied to MQTT connections yet.

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

Connector-based MQTT workers support one worker per enabled MQTT connector for `generic-aion-mqtt`, `generic-mqtt`, and `ttn-v3` profiles when the connector has valid broker and topic settings. TTN v3 workers remain local-runtime plumbing only in this milestone; tests use sample JSON payloads and do not require a live TTN account or public broker.

## The Things Stack Profile

`connector_profile = "ttn-v3"` is accepted as a connector profile for The Things Network / The Things Stack MQTT uplinks.

Example TTN connector semantics:

- `broker_url`: `mqtt://eu1.cloud.thethings.network:1883` or another regional/tenant endpoint
- `topic_filter`: `v3/{application_id}/devices/+/up`
- `payload_format`: `ttn-uplink-json`

`payload_format = "ttn-uplink-json"` decodes common The Things Stack v3 uplink JSON. The decoder reads:

- `end_device_ids.device_id`
- `end_device_ids.application_ids.application_id`
- `uplink_message.decoded_payload`
- `uplink_message.received_at`
- `uplink_message.f_port`
- `uplink_message.f_cnt`
- `uplink_message.frm_payload`
- `uplink_message.rx_metadata`
- `uplink_message.settings`
- root `received_at`

When `uplink_message.decoded_payload` is an object, each primitive top-level key becomes one canonical observation:

- number values become numeric observations;
- string values become text observations;
- boolean values become boolean observations;
- nested objects and arrays are skipped for now and listed in measurement metadata.

Observed properties use the `ttn:` prefix. For example, `decoded_payload.temperature` becomes `ttn:temperature`.

Observation time prefers `uplink_message.received_at`, then root `received_at`, then the ingestion time. Unit mapping can be supplied in connector metadata:

```json
{
  "unit_mapping": {
    "temperature": "Cel",
    "soil_moisture": "%"
  }
}
```

The decoder does not auto-create entities. Producer and feature resolution order for connector-aware TTN ingestion is:

1. Explicit request `producer_entity_id` / `feature_of_interest_id`.
2. Connector default producer/feature IDs.
3. Enabled TTN device mapping for connector + device ID fills any producer or feature ID that is still missing.

Mapping resolution within step 3 is deterministic:

1. If the uplink has `end_device_ids.application_ids.application_id`, AionCore first looks for one enabled mapping with matching connector ID, device ID, and application ID.
2. If no exact application match exists, AionCore looks for one enabled fallback mapping with matching connector ID and device ID where `ttn_application_id` is absent.

An application-specific mapping and a fallback mapping can coexist for the same connector/device. The application-specific mapping is preferred when the uplink application ID matches. Duplicate enabled exact mappings for the same connector/device/application are rejected. Duplicate enabled fallback mappings for the same connector/device are rejected so fallback resolution cannot become ambiguous. Disabled mappings are ignored by resolution.

If no producer entity can be resolved for a TTN uplink, AionCore still stores the raw message, marks it failed, emits `aion:TtnDeviceMappingMissing` and `aion:PayloadIngestionFailed`, and creates no observations. If mapping data is ambiguous, AionCore stores the raw message, marks it failed, emits `aion:TtnDeviceMappingAmbiguous` and `aion:PayloadIngestionFailed`, and creates no observations. If a mapping resolves successfully, `aion:TtnDeviceMappingResolved` is emitted and the successful `PayloadIngested` event metadata includes:

- `ttn_mapping_id`
- `mapping_resolution`: `exact_application_match` or `fallback_device_match`
- `ttn_device_id`
- `ttn_application_id`

TTN device and application IDs are preserved in observation and `PayloadIngested` event metadata.

Missing or ambiguous mapping failure event metadata includes:

- `ttn_device_id`
- `ttn_application_id`
- `connector_id`
- `mapping_resolution_error`

TTN mapping events include:

- `aion:TtnDeviceMappingCreated`
- `aion:TtnDeviceMappingUpdated`
- `aion:TtnDeviceMappingDeleted`
- `aion:TtnDeviceMappingEnabled`
- `aion:TtnDeviceMappingDisabled`
- `aion:TtnDeviceMappingResolved`
- `aion:TtnDeviceMappingMissing`
- `aion:TtnDeviceMappingAmbiguous`

Future TTN adapter behavior should:

- map `decoded_payload` fields to canonical observations;
- preserve uplink metadata such as RSSI, SNR, gateway IDs, frame counters, and timestamps;
- support topic differences across tenant, application, and regional endpoint conventions.
- validate live TTN broker details and support production TLS/mTLS where required.

TTN connector validation in this milestone is deliberately limited to local configuration diagnostics. It does not prove that a TTN account exists, credentials are correct, a broker is reachable, or a subscription will succeed.

## Limitations

- `POST /ingest/reliable` now implements the documented reliable-envelope and tenant-scoped idempotency foundation for generic HTTP ingestion.
- Connector-aware reliable ingestion is not implemented yet.
- Batch and backfill ingestion APIs are not implemented yet.
- Replay execution is not implemented yet.
- Automatic DLQ routing is not implemented yet.
- Connector registry persistence is available for in-memory and PostgreSQL storage.
- Dynamic MQTT workers are implemented for `generic-aion-mqtt`, `generic-mqtt`, and `ttn-v3` connector profiles and must be explicitly enabled.
- TTN/The Things Stack live validation is limited to an explicit MQTT connection/subscription preflight. It does not validate account semantics beyond broker authentication/subscription behavior and does not ingest messages.
- Connector secret references are local-development friendly and are not a production secret manager.
- Connector secret values are persisted by the configured storage backend, but encryption, KMS, and Vault integration are not implemented yet.
- Dynamic MQTT connector authentication supports only `mqtt_basic_auth` secrets.
- TTN v3 uplink JSON decoding is local and sample-payload based; live TTN account integration is not implemented.
- TTN entity auto-provisioning is not implemented; device-to-entity association requires explicit TTN device mappings or request/default entity IDs.
- TTN downlinks are not implemented yet.
- MQTT broker username/password authentication is supported for the worker only.
- Per-device MQTT authorization is not implemented yet.
- TLS/mTLS is not implemented yet.
- MQTT command publishing is not implemented yet.
- Production broker scaling is not implemented yet.
- The env-var MQTT worker still has no reconnect fallback; if it is enabled and the broker cannot be reached, readiness reports MQTT as not ready.
- HTTP and MQTT ingest into the same canonical-observation model, but the runtime still defaults to in-memory storage unless PostgreSQL is selected explicitly.
