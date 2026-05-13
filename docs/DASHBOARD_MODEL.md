# Dashboard Model

The AionCore dashboard is currently a static operational surface served from `apps/aion-dashboard/`. Milestones 81, 82, 84, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, and 99 add dashboard-oriented API summaries, flow and DLQ inventory counts, a lightweight static frontend shell, connector and broker management UI, a simple entity/property time-series explorer, a form-based flow builder foundation, a native-ES-module maintainability pass, optional static hosting through `aion-api`, the first read-only visual flow graph layer, constrained visual editing for proposed linear drafts only, typed node inspectors for known draft node kinds, a backend-only simulated flow execute foundation, and static simulated execution UI integration.

## Current Intent

- Keep the dashboard surface low-risk and easy to serve.
- Reuse existing canonical observations and raw operational records.
- Support a future exploration flow of `entity -> observed property -> historical chart`.
- Support operational views for connectors, brokers, and workers.
- Provide a no-build UI that can safely consume existing APIs during backend iteration.
- Allow optional backend-hosted static serving for local demos when a separate static server is unavailable.
- Provide a safe flow-definition authoring path before any future visual graph editor exists.
- Keep the frontend split into browser-native modules before introducing runtime-heavy features.
- Add a Node-RED-like inspection and preview layer before any drag-and-drop or execution surface exists.
- Add constrained visual draft editing for known safe linear flow patterns before any arbitrary graph editor exists.
- Add typed node inspectors for known safe flow node kinds before any runtime execution exists.
- Preserve Grafana as a valid future option for advanced charting and long-range analytics.

## Current Read Surface

- `GET /dashboard/overview`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`
- `GET /dashboard/timeseries/entities`
- `GET /timeseries/entities/{entity_id}/properties`
- `GET /timeseries/query`
- `GET /dashboard/connectors/overview`
- `GET /flows/{flow_id}/validation`
- `POST /flows/dry-run`
- `POST /flows/execute`
- `POST /flows/{flow_id}/execute`
- `POST /flows/{flow_id}/dry-run`
- `POST /flows/validate`
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
- `POST /flows`
- `PUT /flows/{flow_id}/enable`
- `PUT /flows/{flow_id}/disable`
- `DELETE /flows/{flow_id}`

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
- form-based flow authoring with redacted preview JSON
- read-only visual graph inspection for stored flows and constrained visual draft editing for proposed flows
- node-level validation issue markers and click-to-inspect node detail
- flow validation, dry-run, and simulated execute inspection without real execution
- later reliability and provenance visibility for external flow engines such as NiFi and MiNiFi

This is intentionally different from a generic chart-only experience. Grafana can still complement AionCore for richer chart composition later.

## Frontend Skeleton And Management UI

The current frontend remains a lightweight static dashboard app:

- `apps/aion-dashboard/index.html`
- `apps/aion-dashboard/styles.css`
- `apps/aion-dashboard/dashboard.js`
- `apps/aion-dashboard/js/constants.js`
- `apps/aion-dashboard/js/state.js`
- `apps/aion-dashboard/js/utils.js`
- `apps/aion-dashboard/js/api.js`
- `apps/aion-dashboard/js/timeseries.js`
- `apps/aion-dashboard/js/connectors.js`
- `apps/aion-dashboard/js/flows.js`

The frontend intentionally avoids React, Vite, Next, `node_modules`, external CDNs, and charting libraries.

`dashboard.js` is the only HTML entrypoint script. The rest of the code is loaded as native browser ES modules with no bundler.

Milestone 94 also allows `aion-api` to serve the same files under `GET /ui/*` when `AIONCORE_DASHBOARD_STATIC_DIR` points at a valid directory. The feature is optional, does not embed assets into the binary, and does not alter existing `/dashboard/*` API behavior.

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
- a guided source -> constrained middle chain -> sink or dlq builder that generates a flow definition with linear edges
- a dependency-free SVG visual graph panel for the current builder draft and stored flow detail
- constrained builder-draft editing for selected nodes and linear chain order only
- typed inspectors for `mqtt_subscribe`, `http_input`, `ttn_uplink`, `internal_observation`, `senml_decode`, `ultralight_decode`, `canonical_json`, `json_map`, `filter_condition`, `threshold_rule`, `internal_observation_store`, `raw_message_store`, `mqtt_publish`, `http_forward`, `event_create`, `command_create`, and `dlq`
- click-to-select node detail panels that show redacted config JSON and node-scoped issues when available
- proposed-flow validation from `POST /flows/validate`
- proposed-flow dry-run planning from `POST /flows/dry-run`
- proposed-flow simulated execute from `POST /flows/execute`
- explicit flow create, enable, disable, and confirm-before-delete actions using the existing `/flows` write surface
- explicit stored-flow validation and dry-run actions using the existing planning endpoints
- explicit stored-flow simulated execute using `POST /flows/{flow_id}/execute`
- simulated execute request preview panels with client-side JSON validation and secret redaction
- graph highlighting for returned node execution states and sink conceptual actions

## Auth And Safety

The UI stores only operator-provided local development settings:

- API base URL, defaulting to `http://127.0.0.1:8080`
- optional bearer token stored in browser `localStorage`

In token mode the consumed scopes are:

- `dashboard:read` for the dashboard aggregation routes
- `timeseries:read` for `/timeseries/*` explorer reads
- `connectors:read` for connector, TTN validation, and worker operational reads
- `connectors:admin` for connector mutation and worker reconcile
- `flows:read` for flow validation, dry-run, and simulated execute reads
- `flows:write` for flow creation and stored-flow mutations

Milestone 90 keeps several safety boundaries:

- no secret creation UI
- no secret value display
- no secret value persistence in browser storage
- redaction of secret-like fields in JSON previews
- redaction of secret-like fields in flow preview JSON and stored flow details
- redaction of secret-like URL and token fragments in typed inspector fields
- no automatic TTN live validation
- no real flow execution
- no drag-and-drop flow editing
- no arbitrary visual graph editing
- no direct graph editing for stored flows
- no graph persistence changes
- no broker subscriptions or sink side effects initiated by the UI
- no MQTT publish, HTTP forward, observation write, event creation, command creation, or DLQ write from simulated execute UI actions
- no external chart library
- no Grafana integration in the static dashboard milestone
- no required backend-hosted static asset serving
- no auth enforcement on static `/ui/*` assets in this milestone

## Deferred Work

The following are still explicitly out of scope for the current dashboard phase:

- heavy frontend build tooling
- drag-and-drop graph canvas
- drag-and-drop flow editor
- arbitrary graph editing
- arbitrary stored-flow graph mutation
- arbitrary node-kind-specific runtime configuration beyond the typed planning inspectors
- graph panning or zooming
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

## Flow Execution Preview Semantics

The dashboard can render execution previews returned by the simulated execution endpoints. Milestone 100 adds edge-level traversal results and richer mapping/rule previews to support future visual branching explanations. These previews remain side-effect-free.


## Flow Execution Status

The dashboard can display simulated execution and sink results. Milestone 103 adds backend support for explicitly authorized MQTT/HTTP sink execution, but the dashboard should continue to present this as an operator-triggered action requiring `flows:execute` and explicit sink-action intent.
