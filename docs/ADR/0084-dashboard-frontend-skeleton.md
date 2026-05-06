# ADR 0084: Dashboard Frontend Skeleton

## Status

Accepted

## Context

Milestones 80, 81, 82, 87, and 88 established the first dashboard-oriented read APIs:

- `GET /dashboard/overview`
- `GET /dashboard/timeseries/entities`
- `GET /dashboard/connectors/overview`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`

Operators now need a first UI layer for local exploration, but the project should not yet absorb a heavy frontend toolchain while the dashboard contracts and flow model are still settling.

The milestone also must stay low-risk:

- no flow execution
- no flow editing
- no drag-and-drop graph builder
- no connector mutations
- no broker subscription changes
- no MQTT publish or HTTP forward actions
- no external CDNs
- no `node_modules`

## Decision

Add a static no-build dashboard app under `apps/aion-dashboard/`:

- `index.html`
- `styles.css`
- `dashboard.js`
- `README.md`

The app:

- is served by any simple static server
- defaults to `http://127.0.0.1:8080` as the API base URL
- allows a localStorage override for API base URL
- allows an optional local-development bearer token in localStorage
- sends `Authorization: Bearer <token>` only when a token is present
- consumes only the existing read-only dashboard APIs

The UI provides:

- overview cards
- time-series entity table
- connector overview table
- flow inventory table
- flow detail inspection panel with redacted node config, validation summary, planned path, referenced connectors, planned sinks, execution status, nodes, and edges

## Consequences

Positive:

- the project gains an immediately usable operator dashboard without introducing frontend package management or build tooling
- the UI stays aligned with the current dashboard API contracts rather than inventing new backend behavior
- the frontend remains easy to inspect, serve, and replace while the platform is still early-stage

Tradeoffs:

- there is no component system, routing layer, or advanced state management yet
- charting is deferred, so time-series exploration currently stops at entity inventory
- the current UI is intentionally utilitarian and not yet a full operational console

## Deferred Work

This ADR does not introduce:

- React, Vite, Next, or another full frontend stack
- drag-and-drop flow editing
- flow execution
- broker subscription changes
- MQTT publish or HTTP forward actions
- charting libraries
- connector write workflows

If the dashboard grows materially in scope, a future milestone can migrate the static app to React/Vite or another UI stack while preserving the existing `/dashboard/*` API contracts.
