# AionCore Dashboard

This app is the Milestone 96 static frontend for the AionCore dashboard.

## Scope

The dashboard remains plain HTML, CSS, and JavaScript:

- no Node.js toolchain
- no build step
- no framework lock-in
- no external CDN dependencies
- no backend behavior changes

Milestone 96 keeps the static no-build dashboard approach from Milestones 89 through 95, adds constrained visual editing for proposed flow drafts on top of the existing graph layer, and preserves the native ES module structure so it remains easy to maintain and serve locally.

## Files

- `index.html`
- `styles.css`
- `dashboard.js`
- `js/constants.js`
- `js/state.js`
- `js/utils.js`
- `js/api.js`
- `js/timeseries.js`
- `js/connectors.js`
- `js/flows.js`

`dashboard.js` stays the browser entrypoint. `js/flows.js` now contains the dependency-free SVG graph rendering, constrained proposed-draft editing logic, node selection handling, validation markers, dry-run sink highlighting, and stored-flow copy-to-draft behavior. All files remain plain no-build ES modules loaded directly by the browser.

## APIs Consumed

Read-oriented dashboard APIs:

- `GET /dashboard/overview`
- `GET /dashboard/timeseries/entities`
- `GET /timeseries/entities/{entity_id}/properties`
- `GET /timeseries/query`
- `GET /dashboard/connectors/overview`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`
- `GET /flows/{flow_id}/validation`

Connector and worker operational reads:

- `GET /ingestion/connectors`
- `GET /ingestion/connectors/{connector_id}`
- `GET /ingestion/connectors/{connector_id}/status`
- `GET /ingestion/connectors/{connector_id}/validate`
- `GET /ingestion/connectors/{connector_id}/ttn-live-readiness-plan`
- `GET /ingestion/workers/plan`
- `GET /ingestion/workers/status`

Connector and worker admin actions:

- `POST /ingestion/connectors`
- `PATCH /ingestion/connectors/{connector_id}`
- `PUT /ingestion/connectors/{connector_id}/enable`
- `PUT /ingestion/connectors/{connector_id}/disable`
- `POST /ingestion/workers/reconcile`

Flow builder and stored-flow actions:

- `POST /flows`
- `POST /flows/validate`
- `POST /flows/dry-run`
- `POST /flows/{flow_id}/dry-run`
- `PUT /flows/{flow_id}/enable`
- `PUT /flows/{flow_id}/disable`
- `DELETE /flows/{flow_id}`

## UI Sections

- Overview cards from `GET /dashboard/overview`
- Time-series entity discovery from `GET /dashboard/timeseries/entities`
- Entity/property query controls backed by `GET /timeseries/entities/{entity_id}/properties` and `GET /timeseries/query`
- Raw point result table showing time, value, unit, observation ID, and optional raw message ID
- Compact aggregation summary for `last`, `count`, `avg`, `min`, and `max`
- Simple dependency-free SVG chart for numeric raw points only
- Connector overview table from `GET /dashboard/connectors/overview`
- Connector detail and status panel from `GET /ingestion/connectors/{connector_id}` and `GET /ingestion/connectors/{connector_id}/status`
- Create connector form using `POST /ingestion/connectors`
- Safe update form using `PATCH /ingestion/connectors/{connector_id}`
- Enable and disable actions using the dedicated enable/disable endpoints
- Worker plan and runtime tables from `GET /ingestion/workers/plan` and `GET /ingestion/workers/status`
- Manual reconcile action using `POST /ingestion/workers/reconcile`
- Manual TTN validation and dry-run readiness loading only when explicitly triggered
- Flow inventory and detail views from `GET /dashboard/flows` and `GET /dashboard/flows/{flow_id}`
- Guided source -> constrained middle chain -> sink or dlq flow builder with generated linear edges
- Redacted flow JSON preview and optional advanced JSON override
- Constrained visual editing for proposed draft graphs only
- Selected draft node editor for safe name, kind, and connector updates
- Add transform, filter, and rule nodes into a linear draft chain
- Move up, move down, and remove controls for draft chain nodes
- Manual proposed-flow validation using `POST /flows/validate`
- Manual proposed-flow dry-run using `POST /flows/dry-run`
- Manual flow creation using `POST /flows`
- Stored flow validation using `GET /flows/{flow_id}/validation`
- Stored flow dry-run using `POST /flows/{flow_id}/dry-run`
- Stored flow enable, disable, and explicit confirm-before-delete actions
- Read-only visual graph rendering for stored flow detail using `GET /dashboard/flows/{flow_id}` and optional `GET /flows/{flow_id}/validation`
- Read-only proposed graph preview when advanced JSON override is active
- Constrained editable proposed graph preview for guided drafts
- Click-to-select node detail panels with redacted config JSON and node-scoped validation issue display
- Dry-run sink effect highlighting for conceptual observation store, MQTT publish, HTTP forward, event create, command create, and DLQ use
- Copy stored flow to builder draft for safe linear flows only

## Auth Scopes

In `AIONCORE_AUTH_MODE=token`:

- `dashboard:read` for dashboard overview, connector overview, time-series inventory, and flow views
- `timeseries:read` for `/timeseries/entities/{entity_id}/properties` and `/timeseries/query`
- `connectors:read` for connector list, detail, status, TTN validation reads, worker plan, and worker status
- `connectors:admin` for connector create, patch, enable, disable, and worker reconcile
- `flows:read` for `/flows/validate`, `/flows/dry-run`, `/flows/{flow_id}/validation`, and `/flows/{flow_id}/dry-run`
- `flows:write` for `POST /flows`, `PUT /flows/{flow_id}/enable`, `PUT /flows/{flow_id}/disable`, and `DELETE /flows/{flow_id}`

The UI continues to support development mode with no token. If token mode returns `401` or `403`, the UI shows clear user-facing errors and never logs token values.

## Secret Handling

- The dashboard does not implement secret creation in this milestone.
- `secret_ref_id` can be entered manually for existing connector secrets.
- Secret values are never displayed.
- Secret values are never stored in `localStorage`.
- Secret-like keys are redacted in JSON previews.
- Flow previews and stored flow details also redact secret-like keys such as `password`, `secret`, `token`, `api_key`, `access_key`, `private_key`, and `credential`.
- Visual node detail panels reuse the same recursive redaction before rendering config JSON.

## Local Run

From the repository root:

```powershell
python -m http.server 5173 --directory apps/aion-dashboard
```

Then open:

```text
http://127.0.0.1:5173
```

Default API base URL:

```text
http://127.0.0.1:8080
```

The UI supports an optional bearer token for local development. The API base URL and token are stored in browser `localStorage`.

Because the frontend is still no-build, any simple static file server is sufficient for local demos:

- `python -m http.server 5173 --directory apps/aion-dashboard`
- `npx` is intentionally not required
- no bundled assets or `node_modules`

## Optional API Static Serving

Milestone 94 adds optional static serving from `aion-api` for local demos and Windows environments where `python -m http.server` is not available.

Enable it by setting:

```powershell
$env:AIONCORE_DASHBOARD_STATIC_DIR = "apps/aion-dashboard"
cargo run -p aion-api
```

Then open:

```text
http://127.0.0.1:8080/ui/
```

Behavior:

- if `AIONCORE_DASHBOARD_STATIC_DIR` is unset or empty, `aion-api` behavior is unchanged and `/ui` is not served
- `GET /ui` and `GET /ui/` serve `index.html`
- `GET /ui/dashboard.js`, `GET /ui/styles.css`, and `GET /ui/js/*.js` serve the existing no-build assets directly
- the existing `/dashboard/*` routes remain API routes and are not shadowed by static files

This hosting remains intentionally optional because:

- the maintainability split was low risk and self-contained
- backend API behavior must remain unchanged when the feature is off
- local operators may still prefer a separate static server
- embedding assets into the Rust binary would make small frontend edits and asset replacement less operationally convenient

Static assets are not auth-protected in this milestone:

- the goal is low-friction local serving for demos and development
- existing API auth behavior stays where it already exists
- stricter browser-facing auth and transport controls remain future work

If a later milestone adds `AIONCORE_DASHBOARD_STATIC_DIR`, it should remain optional and should not change existing API route behavior.

## Why No Build Step Still Fits

The dashboard is still intentionally plain HTML, CSS, and JavaScript because the current operator surface is:

- small enough to ship directly as browser modules
- easier to inspect during backend iteration
- easier to demo locally without frontend package management
- lower risk while flow execution and richer runtime features are still deferred

## When To Consider A Full Frontend Toolchain

Revisit the no-build approach only if one or more of these become true:

- the static modules become hard to reason about even after modularization
- shared UI state or routing becomes substantially more complex
- asset compilation, typed templates, or component testing become necessary
- dashboard scope expands into a larger product surface beyond the current operator UI

## Time-Series Explorer Notes

- Default aggregation is `none`.
- Default query limit is `1000`.
- `from` and `to` are optional.
- Query results show the backend `count`, `limit`, and `truncated` flags.
- The chart stays deliberately simple and dependency-free:
  - raw queries only
  - numeric points only
  - no zooming, panning, or downsampling

This keeps the dashboard static and low-risk while still supporting InfluxDB/Grafana-style operator exploration.

## Flow Builder Notes

- The builder is guided and linear: source -> zero or more constrained middle nodes -> sink or dlq.
- Generated previews include `nodes`, `edges`, and `metadata`.
- An optional advanced JSON override can replace the guided output for low-risk editing.
- The preview pane now also renders a dependency-free SVG graph for the current draft when it can be parsed safely.
- Proposed visual editing remains constrained to `source -> zero or more decoder/transform/filter/rule nodes -> sink or dlq`.
- Draft edges are always regenerated automatically from the current constrained node order.
- Stored flow graphs remain read-only. Operators must use `Copy To Draft` before any graph-side edits are possible.
- The advanced JSON override remains available, but it disables constrained visual editing until cleared.
- If the advanced JSON override is invalid, the graph panel shows a clear preview error instead of crashing the page.
- Validation and dry-run never save automatically.
- Dry-run remains planning-only and surfaces `side_effects_performed = false` and `execution_supported = false`.
- Validation issues are shown both in result panels and as node-level markers in the visual graph when node IDs are present.
- Dry-run results can highlight conceptual sink effects in the graph and in a dedicated summary panel.
- The UI does not execute flows, subscribe to brokers, publish MQTT, forward HTTP, or create observations, events, commands, or DLQ records.

## Visual Graph Notes

- Stored graph rendering stays inspection-only and preview-only.
- Rendering uses inline SVG and existing browser APIs only.
- Layout is intentionally simple: linear left-to-right for straightforward chain graphs, with a fallback grid for more complex shapes.
- Clicking a stored node opens a read-only node detail panel with `node_id`, `node_type`, `name`, `config.kind`, redacted config JSON, and node-specific validation issues when available.
- Clicking a proposed draft node in guided mode opens a constrained node editor instead of a free-form graph editor.
- Stored-flow graph rendering uses `GET /dashboard/flows/{flow_id}` data and can be enriched by `GET /flows/{flow_id}/validation` and `POST /flows/{flow_id}/dry-run`.
- Proposed-flow graph rendering uses the current form draft or advanced JSON override and can be enriched by `POST /flows/validate` and `POST /flows/dry-run`.

## Deliberate Deferrals

This milestone does not implement:

- secret creation UI
- secret inspection UI
- drag-and-drop flow editing
- arbitrary visual graph editing
- canvas panning or zooming
- flow execution
- MQTT publish
- HTTP forwarding
- live TTN validation triggers
- frontend build tooling
- external chart dependencies
- Grafana provisioning or integration

Live TTN validation remains an explicit backend operator action because it can open a real broker connection. The dashboard only exposes the safe dry-run readiness read and only on manual request.
