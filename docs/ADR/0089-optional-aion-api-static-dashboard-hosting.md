# ADR 0089: Optional `aion-api` Static Dashboard Hosting

## Status

Accepted

## Context

Milestones 89 through 93 established `apps/aion-dashboard/` as a no-build static dashboard served by a separate static file server such as `python -m http.server`.

That works well on many developer machines, but local Windows demo environments do not always have Python available. The backend already runs as the main local entrypoint, so a small optional static-hosting path can reduce demo friction without changing the dashboard packaging model.

The existing constraints remain important:

- existing API routes such as `/dashboard/*`, `/flows/*`, `/ingest/*`, `/ingestion/*`, and `/timeseries/*` must not change behavior
- the dashboard must remain optional
- frontend files must remain ordinary filesystem assets, not embedded binary resources
- no build tooling, `node_modules`, or external CDN dependencies should be introduced
- this milestone should not add static-asset auth enforcement or flow execution behavior

## Decision

`aion-api` may optionally serve the existing dashboard files from a configured filesystem directory when `AIONCORE_DASHBOARD_STATIC_DIR` is set to a valid directory.

The stable mount path is:

- `GET /ui/*`

Behavior:

- if `AIONCORE_DASHBOARD_STATIC_DIR` is unset or empty, static hosting is disabled and `aion-api` behavior is unchanged
- `GET /ui` and `GET /ui/` serve `index.html`
- `GET /ui/dashboard.js`, `GET /ui/styles.css`, and `GET /ui/js/*.js` serve the existing static assets directly from disk
- `/dashboard/*` remains the API namespace and is never reused for static hosting

Implementation uses filesystem-based static serving through Axum-compatible middleware and serves only from the configured directory.

## Consequences

Positive:

- local demos become easier on machines without Python or another preinstalled static file server
- backend API behavior remains unchanged when the feature is off
- frontend files stay editable and replaceable without rebuilding the Rust binary
- the no-build dashboard architecture remains intact

Tradeoffs:

- the server process now has an optional second concern beyond API routes
- static assets are still unauthenticated in this milestone
- an invalid configured static directory becomes a startup configuration error rather than silently serving partial or wrong content

## Why Not Embed The Assets

Embedding the dashboard files into the binary would work against the current operational goals:

- frontend iteration would become less transparent
- simple asset edits would require rebuilding the Rust binary
- filesystem-hosted assets remain a better fit for a no-build dashboard that is still evolving

## Why Static Assets Are Not Auth-Protected Yet

This milestone is intentionally a packaging convenience milestone, not a browser-facing auth redesign.

Auth remains enforced on the existing API routes where it already applies. Static `/ui/*` asset protection is deferred because:

- local demos and development are the immediate target
- static file auth often needs broader browser/session/origin design decisions
- adding auth now would increase risk beyond the goal of small operational convenience

## Deferred

- embedding dashboard assets into the binary
- auth enforcement for `/ui/*`
- additional frontend build tooling
- dashboard execution behavior
- reuse of `/dashboard/*` for static files
- broader readiness changes tied to optional static hosting
