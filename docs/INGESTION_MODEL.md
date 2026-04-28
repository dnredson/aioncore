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
- enabled MQTT/future connectors report `degraded` because dynamic workers per connector are future work

The registry is supported by in-memory storage and PostgreSQL persistence. Durable connector configuration is needed before dynamic MQTT workers can be introduced, because future startup logic will need to reload enabled connectors and start the appropriate source-specific workers.

The existing environment-variable MQTT configuration acts as the default runtime MQTT connector for now. It is not yet created dynamically from the registry.

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

Future MQTT work should support one worker per enabled MQTT connector so multiple controlled or external brokers can run side by side. That future model should support generic AionCore topics, user-defined generic MQTT mappings, and provider-specific profiles.

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
- Dynamic MQTT workers per connector are not implemented yet.
- TTN/The Things Stack live connectivity and full uplink decoding are not implemented yet.
- Secrets storage is not implemented yet.
- Connector authentication is not implemented yet.
- MQTT broker username/password authentication is supported for the worker only.
- Per-device MQTT authorization is not implemented yet.
- TLS/mTLS is not implemented yet.
- MQTT command publishing is not implemented yet.
- Production broker scaling is not implemented yet.
- No MQTT fallback exists; if MQTT is enabled and the broker cannot be reached, readiness reports MQTT as not ready.
- HTTP and MQTT ingest into the same canonical-observation model, but the runtime still defaults to in-memory storage unless PostgreSQL is selected explicitly.
