# Dashboard Model

The AionCore dashboard is currently a static operational surface served from `apps/aion-dashboard/`. Milestones 81, 82, 84, 87, 88, 89, 90, and 91 add dashboard-oriented API summaries, flow and DLQ inventory counts, a lightweight static frontend shell, connector and broker management UI, and a simple entity/property time-series explorer so operators can inspect entities, connectors, workers, pipeline inventory, and historical observations without changing ingestion or flow execution behavior.

## Current Intent

- Keep the dashboard surface low-risk and easy to serve.
- Reuse existing canonical observations and raw operational records.
- Support a future exploration flow of `entity -> observed property -> historical chart`.
- Support operational views for connectors, brokers, and workers.
- Provide a no-build UI that can safely consume existing APIs during backend iteration.
- Preserve Grafana as a valid future option for advanced charting and long-range analytics.

## Current Read Surface

- `GET /dashboard/overview`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`
- `GET /dashboard/timeseries/entities`
- `GET /timeseries/entities/{entity_id}/properties`
- `GET /timeseries/query`
- `GET /dashboard/connectors/overview`
- `GET /ingestion/connectors`
- `GET /ingestion/connectors/{connector_id}`
- `GET /ingestion/connectors/{connector_id}/status`
- `GET /ingestion/connectors/{connector_id}/validate`
- `GET /ingestion/connectors/{connector_id}/ttn-live-readiness-plan`
- `GET /ingestion/workers/plan`
- `GET /ingestion/workers/status`

These endpoints are designed for dashboard landing pages, operator inspection, and safe planning views rather than full raw-data export.

## Current Write Surface Used By The Dashboard

Milestone 90 adds explicit operator actions, but only by consuming already existing backend routes:

- `POST /ingestion/connectors`
- `PATCH /ingestion/connectors/{connector_id}`
- `PUT /ingestion/connectors/{connector_id}/enable`
- `PUT /ingestion/connectors/{connector_id}/disable`
- `POST /ingestion/workers/reconcile`

The dashboard does not change backend semantics and does not add any new endpoint.

## Operational Focus

The current and future AionCore dashboard is intended to emphasize:

- entity and property exploration
- historical time-series discovery
- connector and broker inspection
- connector create and safe operational update flows
- worker planning and runtime health
- later pipeline and flow visibility
- flow list, detail, and graph rendering from dashboard-friendly read models
- flow validation and dry-run inspection without execution
- later reliability and provenance visibility for external flow engines such as NiFi and MiNiFi

This is intentionally different from a generic chart-only experience. Grafana can still complement AionCore for richer chart composition later.

## Frontend Skeleton And Management UI

The current frontend remains a first lightweight static dashboard app:

- `apps/aion-dashboard/index.html`
- `apps/aion-dashboard/styles.css`
- `apps/aion-dashboard/dashboard.js`

The frontend intentionally avoids React, Vite, Next, `node_modules`, external CDNs, and charting libraries.

It currently provides:

- overview cards from `GET /dashboard/overview`
- time-series entity discovery table from `GET /dashboard/timeseries/entities`
- entity selection, observed-property loading, and historical query controls using `GET /timeseries/entities/{entity_id}/properties` and `GET /timeseries/query`
- raw point result tables with `time`, `value`, `unit`, `observation_id`, and optional `raw_message_id`
- compact whole-range aggregation summaries for `last`, `count`, `avg`, `min`, and `max`
- a small dependency-free SVG chart for numeric raw points only
- connector overview table from `GET /dashboard/connectors/overview`
- connector detail panel from `GET /ingestion/connectors/{connector_id}` and `GET /ingestion/connectors/{connector_id}/status`
- create connector form using `POST /ingestion/connectors`
- safe update connector form using `PATCH /ingestion/connectors/{connector_id}`
- explicit enable and disable actions using the dedicated endpoints
- worker plan and runtime panels from `GET /ingestion/workers/plan` and `GET /ingestion/workers/status`
- manual worker reconcile action using `POST /ingestion/workers/reconcile`
- manual TTN validation and dry-run readiness inspection using existing TTN read endpoints only when the operator requests them
- flow inventory table from `GET /dashboard/flows`
- flow detail inspection panel from `GET /dashboard/flows/{flow_id}`

## Auth And Safety

The UI stores only operator-provided local development settings:

- API base URL, defaulting to `http://127.0.0.1:8080`
- optional bearer token stored in browser `localStorage`

In token mode the consumed scopes are:

- `dashboard:read` for the dashboard aggregation routes
- `timeseries:read` for `/timeseries/*` explorer reads
- `connectors:read` for connector, TTN validation, and worker operational reads
- `connectors:admin` for connector mutation and worker reconcile

Milestone 90 keeps several safety boundaries:

- no secret creation UI
- no secret value display
- no secret value persistence in browser storage
- redaction of secret-like fields in JSON previews
- no automatic TTN live validation
- no flow execution
- no flow editing
- no external chart library
- no Grafana integration in the static dashboard milestone

## Deferred Work

The following are still explicitly out of scope for the current dashboard phase:

- heavy frontend build tooling
- drag-and-drop graph canvas
- drag-and-drop flow editor
- flow execution
- broker subscription changes outside connector config
- MQTT publish
- HTTP forwarding
- in-browser charting libraries
- rich chart interactions such as zoom or bucket editing
- secret creation workflows
- live TTN validation triggers
- automated pipeline or rule authoring from the dashboard

Node-RED-like flow editing remains future work. The current milestones establish safe backend summaries plus a static UI that later dashboard work can replace or migrate without changing the current API contracts.
