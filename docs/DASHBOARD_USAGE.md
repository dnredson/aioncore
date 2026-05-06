# Dashboard Usage

This guide covers the dashboard API surface and the static frontend under `apps/aion-dashboard/`.

## Scope

Dashboard-oriented reads:

- `GET /dashboard/overview`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`
- `GET /dashboard/timeseries/entities`
- `GET /timeseries/entities/{entity_id}/properties`
- `GET /timeseries/query`
- `GET /dashboard/connectors/overview`
- `GET /flows/{flow_id}/validation`

Connector and worker operational reads used by the dashboard:

- `GET /ingestion/connectors`
- `GET /ingestion/connectors/{connector_id}`
- `GET /ingestion/connectors/{connector_id}/status`
- `GET /ingestion/connectors/{connector_id}/validate`
- `GET /ingestion/connectors/{connector_id}/ttn-live-readiness-plan`
- `GET /ingestion/workers/plan`
- `GET /ingestion/workers/status`

Connector and worker admin actions used by the dashboard:

- `POST /ingestion/connectors`
- `PATCH /ingestion/connectors/{connector_id}`
- `PUT /ingestion/connectors/{connector_id}/enable`
- `PUT /ingestion/connectors/{connector_id}/disable`
- `POST /ingestion/workers/reconcile`

Flow actions used by the dashboard:

- `POST /flows`
- `POST /flows/validate`
- `POST /flows/dry-run`
- `POST /flows/{flow_id}/dry-run`
- `PUT /flows/{flow_id}/enable`
- `PUT /flows/{flow_id}/disable`
- `DELETE /flows/{flow_id}`

These endpoints provide compact summaries and operator workflows that the static dashboard can use directly. The dashboard now also consumes the existing `/timeseries/*` read surface for entity/property exploration.

## Flow Builder

The Flow Builder section in `apps/aion-dashboard/` is deliberately form-based in this milestone.

It provides:

- a guided source -> transform -> sink builder
- generated flow JSON with `nodes`, `edges`, and `metadata`
- a redacted preview pane
- an optional advanced JSON override textarea
- explicit proposed-flow validation with `POST /flows/validate`
- explicit proposed-flow dry-run with `POST /flows/dry-run`
- explicit create with `POST /flows`
- explicit stored-flow validation and dry-run against the selected saved flow
- explicit enable, disable, and confirm-before-delete controls for stored flows

The dashboard does not execute flows. Validation and dry-run remain planning-only and do not perform side effects.

## Time-Series Explorer

Entity inventory:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/dashboard/timeseries/entities"
```

Observed property discovery:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/entities/<entity_id>/properties"
```

Raw series query:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture&limit=1000"
```

Aggregation examples:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture&aggregation=last"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture&aggregation=count"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture&aggregation=avg"
```

The static dashboard explorer provides:

- entity selection and refresh from `GET /dashboard/timeseries/entities`
- observed-property loading from `GET /timeseries/entities/{entity_id}/properties`
- optional `from`, `to`, `aggregation`, and `limit` query controls
- raw point tables showing `time`, `value`, `unit`, `observation_id`, and optional `raw_message_id`
- compact whole-range aggregation summaries using the existing `points` response shape
- a simple inline SVG chart for numeric raw points only, with table fallback for non-numeric values

This remains a no-build static UI with no external chart dependency.

## Overview

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/dashboard/overview"
```

Example response:

```json
{
  "entities_count": 12,
  "observations_count": 482,
  "raw_messages_count": 241,
  "events_count": 38,
  "flows_count": 4,
  "enabled_flows_count": 2,
  "invalid_flows_count": 1,
  "flow_validation_warning_count": 3,
  "dlq_pending_count": 3,
  "dlq_total_count": 7,
  "connectors_count": 3,
  "enabled_connectors_count": 2,
  "workers_running_count": 1,
  "workers_degraded_count": 1,
  "generated_at": "2026-05-06T12:00:00Z"
}
```

## Connector Overview

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/dashboard/connectors/overview"
```

Example response:

```json
{
  "generated_at": "2026-05-06T12:00:00Z",
  "connectors": [
    {
      "connector_id": "7738c91a-40dd-444f-aa40-69442646cba8",
      "connector_key": "field-mqtt-01",
      "connector_type": "mqtt",
      "connector_profile": "generic_mqtt",
      "enabled": true,
      "status": "reconnecting",
      "readiness": "reconnecting",
      "broker_url": "mqtt://broker.example:1883",
      "topic_filter": "sensors/+/up",
      "payload_format": "senml-json",
      "worker_kind": "mqtt_subscriber",
      "worker_status": "reconnecting",
      "running": false,
      "reconnecting": true,
      "degraded": true,
      "last_error": "connection timeout",
      "secret_configured": true
    }
  ]
}
```

Secret values are never returned. `broker_url` is intended for safe display and redacts embedded credentials.

## Connector Detail And Status

Connector inventory:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/connectors"
```

Connector detail:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/connectors/<connector-id>"
```

Connector runtime status:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/connectors/<connector-id>/status"
```

The static dashboard uses the dashboard overview table for selection and the connector detail/status routes for the operator side panel.

## Worker Plan And Runtime

Planner:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/workers/plan"
```

Runtime status:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/workers/status"
```

The dashboard combines these responses to show:

- whether connector workers are enabled
- planned, skipped, invalid, and unsupported worker counts
- connector-specific worker kind and planned status
- running, reconnecting, degraded, stopped, and error runtime state
- `last_error`
- `reconnect_attempts`
- `started_at`
- `stopped_at`
- `last_reconciled_at`

Manual reconcile:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8080/ingestion/workers/reconcile"
```

## Connector Create And Safe Patch

Create connector:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8080/ingestion/connectors" `
  -ContentType "application/json" `
  -Body (@{
    connector_key = "field-mqtt-01"
    connector_type = "mqtt"
    connector_profile = "generic-mqtt"
    enabled = $true
    display_name = "Field MQTT 01"
    broker_url = "mqtt://127.0.0.1:1883"
    client_id = "aioncore-field-mqtt-01"
    topic_filter = "aioncore/+/+/data"
    payload_format = "senml-json"
    secret_ref_id = "00000000-0000-0000-0000-000000000000"
    metadata = @{
      purpose = "local dashboard demo"
    }
  } | ConvertTo-Json -Depth 8)
```

Safe patch example:

```powershell
Invoke-RestMethod `
  -Method Patch `
  -Uri "http://localhost:8080/ingestion/connectors/<connector-id>" `
  -ContentType "application/json" `
  -Body (@{
    display_name = "Field MQTT 01 Updated"
    broker_url = "mqtt://127.0.0.1:1883"
    topic_filter = "aioncore/soil/+/data"
    payload_format = "senml-json"
    secret_ref_id = "00000000-0000-0000-0000-000000000000"
    metadata = @{
      updated_by = "dashboard"
    }
  } | ConvertTo-Json -Depth 8)
```

The dashboard labels display name as "Name / Display Name" for operator usability, but it uses the existing backend field name `display_name`.

## TTN Validation And Dry-Run Readiness

Load deterministic connector validation:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/connectors/<connector-id>/validate"
```

Load TTN live-readiness dry run:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ingestion/connectors/<connector-id>/ttn-live-readiness-plan"
```

The dashboard does not call live validation automatically. It does not call `POST /ingestion/connectors/{connector_id}/ttn-live-validate` in this milestone.

## Token Mode

In `AIONCORE_AUTH_MODE=token`, the dashboard UI requires:

- `dashboard:read` for `GET /dashboard/overview`, `GET /dashboard/timeseries/entities`, `GET /dashboard/connectors/overview`, `GET /dashboard/flows`, and `GET /dashboard/flows/{flow_id}`
- `timeseries:read` for `GET /timeseries/entities/{entity_id}/properties` and `GET /timeseries/query`
- `connectors:read` for `GET /ingestion/connectors`, connector detail/status, TTN validation reads, and worker plan/status
- `connectors:admin` for connector create, patch, enable, disable, and worker reconcile
- `flows:read` for `POST /flows/validate`, `POST /flows/dry-run`, `GET /flows/{flow_id}/validation`, and `POST /flows/{flow_id}/dry-run`
- `flows:write` for `POST /flows`, `PUT /flows/{flow_id}/enable`, `PUT /flows/{flow_id}/disable`, and `DELETE /flows/{flow_id}`

Example:

```powershell
$headers = @{ Authorization = "Bearer <token>" }
Invoke-RestMethod -Method Get -Headers $headers -Uri "http://localhost:8080/dashboard/overview"
Invoke-RestMethod -Method Get -Headers $headers -Uri "http://localhost:8080/ingestion/workers/status"
```

Token-mode behavior surfaced by the UI:

- missing or invalid bearer token: `401`, shown as `Missing or invalid token`
- valid token without the required scope: `403`, shown as `Token lacks required scope`

The UI never logs token values.

## Static Dashboard App

The current dashboard frontend remains a no-build static app:

- `apps/aion-dashboard/index.html`
- `apps/aion-dashboard/styles.css`
- `apps/aion-dashboard/dashboard.js`

Run it locally from the repository root:

```powershell
python -m http.server 5173 --directory apps/aion-dashboard
```

Then open:

```text
http://127.0.0.1:5173
```

The app defaults to:

```text
http://127.0.0.1:8080
```

The UI supports:

- local API base URL override
- optional bearer token input for local development
- `Authorization: Bearer <token>` only when a token is present
- refresh and section switching
- entity/property time-series exploration with optional date filters, aggregation, and limit
- result counts and truncation visibility from `/timeseries/query`
- a dependency-free numeric raw-point SVG chart
- connector overview and detail inspection
- connector create and safe patch actions
- enable, disable, and reconcile controls
- worker plan and runtime inspection
- manual TTN validation and dry-run readiness reads
- guided flow creation, validation, dry-run, and stored-flow lifecycle operations

The app intentionally still does not implement:

- flow execution
- form-free arbitrary flow editing
- drag-and-drop flow building
- visual graph editing
- secret creation
- live TTN validation triggers
- MQTT publish
- HTTP forwarding
- charting libraries
- Grafana integration
