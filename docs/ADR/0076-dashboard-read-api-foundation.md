# ADR 0076: Dashboard Read API Foundation

## Status

Accepted

## Context

AionCore already exposes historical time-series reads through `/timeseries/query` and `/timeseries/entities/{entity_id}/properties`, but operators still need a simpler dashboard-oriented discovery surface for:

- landing-page counts
- entity/property selection
- connector and broker inspection
- worker health summaries

The project also wants a future dashboard experience inspired by Grafana-style exploration and Node-RED-like operational visibility, without implementing UI or flow execution in the same milestone.

## Decision

Add a read-only dashboard API foundation with:

- `GET /dashboard/overview`
- `GET /dashboard/timeseries/entities`
- `GET /dashboard/connectors/overview`

Protect these routes in token mode with `dashboard:read`, while allowing `admin:all` to satisfy the same checks. Reuse the existing tenant-aware read pattern so non-admin principals only see their own tenant resources.

Do not change existing `/timeseries` behavior, connector behavior, or worker lifecycle behavior.

## Consequences

Positive:

- enables a future AionCore dashboard UI without frontend work in this milestone
- keeps the first dashboard surface read-only and low risk
- provides compact summaries for entity/property discovery and connector inspection
- preserves Grafana as a future advanced charting option

Trade-offs:

- overview counts still rely on existing list/query methods rather than specialized count indexes
- admin-all connector overview requires a broader storage list method for connectors
- this does not yet provide flow editing, flow execution, or chart composition

## Deferred

- frontend dashboard UI
- drag-and-drop flow editor
- Node-RED-like flow execution engine
- Grafana provisioning
- dashboard write operations
