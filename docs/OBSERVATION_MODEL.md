# Observation Model

Canonical observations are the normalized telemetry records produced from raw messages. They are the common representation used by query APIs, analytics, MCP tools, dashboard-facing historical queries, and future decision-support systems.

## Goals

- Preserve a consistent model across payload formats.
- Link observations back to raw messages where possible.
- Support time-series queries by entity, observed property, and time range.
- Keep the MVP model simple enough for PostgreSQL and TimescaleDB.

## Canonical Observation

Logical shape:

```json
{
  "id": "4d322466-0e9e-41df-a851-756baf3a5f6f",
  "tenant_id": "4ce4705b-a004-48d1-9076-68aca111de11",
  "feature_of_interest_id": "f6e28445-9d50-4684-83aa-ef4c21ed2c08",
  "producer_entity_id": "59f9d975-c0e5-48df-a42c-53c8dc5efc6b",
  "observed_property": "temperature",
  "observed_at": "2026-04-27T13:00:00Z",
  "received_at": "2026-04-27T13:00:01Z",
  "value": {
    "type": "number",
    "value": 21.4
  },
  "unit": "Cel",
  "raw_message_id": "37d536d1-b3f3-4c56-bd29-fc0d7165e093",
  "quality": {},
  "metadata": {}
}
```

## Required Fields

- `id`: unique observation ID.
- `tenant_id`: owning tenant.
- `feature_of_interest_id`: entity being observed. Historical time-series query APIs use `entity_id` as the request parameter name, and that value maps to `feature_of_interest_id`.
- `producer_entity_id`: entity that reported or produced the observation.
- `observed_property`: measured or reported property.
- `observed_at`: observation timestamp.
- `value`: typed observation value.

## Value Types

MVP 1 should support:

- Number.
- String.
- Boolean.
- JSON object or array for structured values.

Database storage can use nullable typed columns:

- `value_number`.
- `value_text`.
- `value_bool`.
- `value_json`.

Application validation should ensure only one value column is populated.

## Time Semantics

Observation time should come from the payload when available. If the payload does not include a timestamp, the platform should use the raw message `received_at` timestamp.

The system should distinguish:

- `received_at`: when AionCore received the raw message.
- `observed_at`: when the observation occurred.

## Units

Units should be stored as strings. SenML unit names should be preserved when supplied.

MVP 1 should not enforce unit conversion. Unit normalization and conversion can be added later.

## Quality and Metadata

`quality` stores quality indicators such as:

- Decoder confidence.
- Missing timestamp fallback.
- Payload-specific quality flags.

`metadata` stores non-core details such as:

- Source field path.
- Decoder name.
- Original measurement name.

## TimescaleDB

Canonical observations should be stored in a TimescaleDB hypertable partitioned by `time`.

Recommended query dimensions:

- Tenant.
- Entity.
- Observed property.
- Time range.

## Historical Time-Series API Foundation

Milestone 80 adds a dedicated historical query surface without changing the existing `GET /observations` behavior:

- `GET /timeseries/query`
- `GET /timeseries/entities/{entity_id}/properties`

Current semantics:

- `entity_id` in the time-series API means `feature_of_interest_id`.
- Raw-point queries return observations in chronological ascending order.
- Supported whole-range aggregations are `last`, `count`, `avg`, `min`, and `max`.
- `interval` is reserved for future bucketed aggregation and currently returns a clear not-implemented request error.
- Numeric aggregations operate on numeric observation values only. If no numeric values exist for the selected range, the API returns a request error instead of guessing a coercion.

This keeps canonical observations unchanged while adding a dashboard-ready historical read shape.

## Raw Message Link

When an observation is produced from an ingested message, it should reference `raw_message_id`. This supports auditability, replay, and decoder debugging.

Some future observations may be computed or imported without a raw message. For MVP 1, HTTP-ingested observations should always link to a raw message.
