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

## Limitations

- MQTT broker username/password authentication is supported for the worker only.
- Per-device MQTT authorization is not implemented yet.
- TLS/mTLS is not implemented yet.
- MQTT command publishing is not implemented yet.
- Production broker scaling is not implemented yet.
- No MQTT fallback exists; if MQTT is enabled and the broker cannot be reached, readiness reports MQTT as not ready.
- HTTP and MQTT ingest into the same canonical-observation model, but the runtime still defaults to in-memory storage unless PostgreSQL is selected explicitly.
