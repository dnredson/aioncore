# Ingestion Model

AionCore ingestion is payload-agnostic. Ingestion endpoints receive telemetry, store the raw message, and then pass the payload to a decoder selected by hint, content type, mapping, or route configuration.

## Core Rule

Raw messages must always be stored before normalization.

This preserves auditability and enables later replay when decoders, mappings, or entity registrations change.

## Raw Message

Recommended stored fields:

- `id`: raw message UUID.
- `tenant_id`: owning tenant.
- `source_type`: `http` or `mqtt`.
- `source_ref`: topic, route, or integration reference.
- `device_key`: optional device identifier.
- `decoder_hint`: optional decoder name or mapping key.
- `content_type`: original content type.
- `headers`: request or protocol metadata.
- `payload`: original payload bytes.
- `received_at`: platform receive time.
- `normalization_status`: `pending`, `normalized`, or `failed`.
- `normalization_error`: optional failure message.

## HTTP Ingestion

Initial endpoint:

```text
POST /v1/ingest/http
```

Suggested headers:

```text
X-Aion-Tenant: tenant slug or ID
X-Aion-Device: device key
X-Aion-Decoder: senml_json | ultralight | json_mapping | mapping name
Content-Type: application/json | text/plain
```

HTTP ingestion flow:

```text
Receive request
  -> authenticate tenant/device
  -> store raw message
  -> select decoder
  -> decode payload
  -> resolve entity
  -> write canonical observations
  -> update raw message status
  -> return ingestion result
```

Recommended MVP response behavior:

- If raw storage fails, return an error.
- If raw storage succeeds but normalization fails, return an accepted response with failure details.
- Record normalization failure on the raw message.

## MQTT Ingestion Design

MQTT implementation can be postponed, but the design should be compatible with the raw-message model.

Suggested topic pattern:

```text
aion/{tenant}/{device}/telemetry
```

Optional decoder-specific topic:

```text
aion/{tenant}/{device}/telemetry/{decoder}
```

MQTT flow:

```text
Device publishes telemetry
  -> broker receives message
  -> AionCore MQTT worker subscribes
  -> worker stores raw message with source_type = mqtt
  -> normalizer processes payload
```

MVP 1 should document MQTT and reserve the model. The first implementation can focus on HTTP ingestion.

## Decoder Selection

Decoder selection can use:

- Explicit `X-Aion-Decoder` header.
- Device configuration.
- Decoder mapping name.
- Content type.
- MQTT topic segment.

MVP 1 should prefer explicit decoder hints to avoid ambiguous behavior.

## Payload Decoder Interface

Each decoder converts raw payload bytes into decoded measurements. The normalizer then turns decoded measurements into canonical observations.

Logical decoder output:

```text
entity_key
observed_property
time
value
unit
metadata
```

Initial decoders:

- SenML JSON.
- UltraLight.
- JSON mapping.

## SenML JSON

MVP support should include:

- `bn`: base name.
- `bt`: base time.
- `bu`: base unit.
- `e`: entries.
- `n`: measurement name.
- `t`: relative time.
- `u`: unit.
- `v`: numeric value.
- `vs`: string value.
- `vb`: boolean value.
- `vd`: data value as string.

## UltraLight

MVP support should include payloads like:

```text
t|21.4|h|52
```

Attribute aliases should map short names to observed properties.

Example:

```text
t -> temperature
h -> humidity
```

## JSON Mapping

JSON mapping should be configuration-driven.

Example mapping:

```json
{
  "entity_key": "$.deviceId",
  "timestamp": "$.timestamp",
  "measurements": [
    {
      "property": "temperature",
      "value": "$.temperature",
      "unit": "Cel"
    }
  ]
}
```

MVP 1 can support a small JSON path subset before adopting a full JSONPath implementation.
