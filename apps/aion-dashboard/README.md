# AionCore Dashboard

This app is the Milestone 90 static frontend for the AionCore dashboard.

## Scope

The dashboard remains plain HTML, CSS, and JavaScript:

- no Node.js toolchain
- no build step
- no framework lock-in
- no external CDN dependencies
- no backend behavior changes

Milestone 90 extends the Milestone 89 skeleton with connector and broker management UI backed by existing AionCore APIs.

## Files

- `index.html`
- `styles.css`
- `dashboard.js`

## APIs Consumed

Read-oriented dashboard APIs:

- `GET /dashboard/overview`
- `GET /dashboard/timeseries/entities`
- `GET /dashboard/connectors/overview`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`

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

## UI Sections

- Overview cards from `GET /dashboard/overview`
- Time-series entity discovery from `GET /dashboard/timeseries/entities`
- Connector overview table from `GET /dashboard/connectors/overview`
- Connector detail and status panel from `GET /ingestion/connectors/{connector_id}` and `GET /ingestion/connectors/{connector_id}/status`
- Create connector form using `POST /ingestion/connectors`
- Safe update form using `PATCH /ingestion/connectors/{connector_id}`
- Enable and disable actions using the dedicated enable/disable endpoints
- Worker plan and runtime tables from `GET /ingestion/workers/plan` and `GET /ingestion/workers/status`
- Manual reconcile action using `POST /ingestion/workers/reconcile`
- Manual TTN validation and dry-run readiness loading only when explicitly triggered
- Flow inventory and detail views from `GET /dashboard/flows` and `GET /dashboard/flows/{flow_id}`

## Auth Scopes

In `AIONCORE_AUTH_MODE=token`:

- `dashboard:read` for dashboard overview, connector overview, time-series inventory, and flow views
- `connectors:read` for connector list, detail, status, TTN validation reads, worker plan, and worker status
- `connectors:admin` for connector create, patch, enable, disable, and worker reconcile

The UI continues to support development mode with no token. If token mode returns `401` or `403`, the UI shows clear user-facing errors and never logs token values.

## Secret Handling

- The dashboard does not implement secret creation in this milestone.
- `secret_ref_id` can be entered manually for existing connector secrets.
- Secret values are never displayed.
- Secret values are never stored in `localStorage`.
- Secret-like keys are redacted in JSON previews.

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

## Deliberate Deferrals

This milestone does not implement:

- secret creation UI
- secret inspection UI
- flow editing
- flow execution
- MQTT publish
- HTTP forwarding
- live TTN validation triggers
- frontend build tooling

Live TTN validation remains an explicit backend operator action because it can open a real broker connection. The dashboard only exposes the safe dry-run readiness read and only on manual request.
