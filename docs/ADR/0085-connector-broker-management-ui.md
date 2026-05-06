# ADR 0085: Connector And Broker Management UI

## Status

Accepted

## Context

Milestone 89 added a no-build static dashboard shell under `apps/aion-dashboard/` and limited it to read-only dashboard endpoints:

- `GET /dashboard/overview`
- `GET /dashboard/timeseries/entities`
- `GET /dashboard/connectors/overview`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`

Operators still needed a practical way to inspect connectors and brokers, load worker status, create safe connector records, toggle enabled state, and manually reconcile workers without introducing a frontend build toolchain or changing backend behavior.

This milestone had to stay within existing constraints:

- no React, Vite, Next, or `node_modules`
- no external CDNs
- no backend API behavior changes
- no flow editing or execution
- no secret creation UI
- no secret exposure
- no automatic live TTN validation

## Decision

Extend the static dashboard in `apps/aion-dashboard/` with a dedicated connector and broker management section that consumes only already existing APIs.

Read APIs consumed:

- `GET /dashboard/connectors/overview`
- `GET /ingestion/connectors`
- `GET /ingestion/connectors/{connector_id}`
- `GET /ingestion/connectors/{connector_id}/status`
- `GET /ingestion/connectors/{connector_id}/validate`
- `GET /ingestion/connectors/{connector_id}/ttn-live-readiness-plan`
- `GET /ingestion/workers/plan`
- `GET /ingestion/workers/status`

Write APIs consumed:

- `POST /ingestion/connectors`
- `PATCH /ingestion/connectors/{connector_id}`
- `PUT /ingestion/connectors/{connector_id}/enable`
- `PUT /ingestion/connectors/{connector_id}/disable`
- `POST /ingestion/workers/reconcile`

The UI adds:

- connector overview table
- connector detail panel
- connector create form
- safe connector patch form
- explicit enable and disable buttons
- explicit refresh detail and refresh status buttons
- worker plan and runtime display
- explicit reconcile button
- manual TTN validation and TTN live-readiness dry-run readers

## Auth Scopes

The dashboard uses the existing token-mode scopes:

- `dashboard:read` for the dashboard aggregation routes
- `connectors:read` for connector, TTN validation, and worker operational reads
- `connectors:admin` for connector create, patch, enable, disable, and worker reconcile

The frontend continues to support development mode with no token. When token mode responds with `401` or `403`, the UI shows operator-facing messages and never logs token values.

## Safety Decisions

### Secret Creation Is Deferred

Secret creation remains out of scope because it is a higher-sensitivity browser workflow than connector configuration. The dashboard allows only an existing `secret_ref_id` to be entered manually.

The UI:

- never accepts or stores secret values
- never writes secret values to `localStorage`
- never displays secret values
- redacts secret-like fields in JSON previews

### Live TTN Validation Is Not Automatic

`POST /ingestion/connectors/{connector_id}/ttn-live-validate` can open a real broker connection and should remain an explicit operator action outside this milestone.

The dashboard therefore:

- does not call live validation automatically
- does not surface a live validation trigger in this milestone
- does allow manual loading of:
  - `GET /ingestion/connectors/{connector_id}/validate`
  - `GET /ingestion/connectors/{connector_id}/ttn-live-readiness-plan`

This keeps the browser UI aligned with the backend safety model.

## Consequences

Positive:

- operators get a usable connector and worker management UI without new backend work
- the frontend stays aligned with the current API contracts
- the project still avoids frontend package management and build tooling
- connector and worker operations become easier than raw API invocation for common local and dev tasks

Tradeoffs:

- the static app remains intentionally simple and does not yet have a component model or routing system
- secret creation remains a separate operational workflow
- TTN live validation remains outside the UI to preserve explicit operator intent
- there is still no flow editing or execution surface

## Alternatives Rejected

- adding a frontend toolchain now: rejected because the dashboard surface is still evolving and the no-build approach is materially lower risk
- adding new dashboard-specific backend endpoints: rejected because the existing connector and worker APIs already cover this milestone
- adding secret creation to the browser UI: rejected because it would broaden the sensitivity of the current static dashboard too early
- auto-running TTN live validation after selection or save: rejected because it would create unintended network side effects
