# ADR 0086: Time-Series Explorer UI

## Status

Accepted

## Context

Milestone 80 added the historical time-series API foundation:

- `GET /timeseries/query`
- `GET /timeseries/entities/{entity_id}/properties`

Milestone 81 added a dashboard-oriented entity inventory read:

- `GET /dashboard/timeseries/entities`

Milestones 89 and 90 established the no-build static dashboard and extended it with connector and broker management UI. The next step is to make common IoT operational exploration easier by supporting the basic flow:

- select an entity
- load observed properties
- query raw historical points
- inspect whole-range aggregations
- show a simple visual cue when numeric data is available

This work must remain low-risk:

- no React, Vite, Next, or npm toolchain
- no external CDN dependencies
- no backend behavior changes
- no new backend endpoints
- no heavy charting library

## Decision

Extend `apps/aion-dashboard/` with a dedicated static time-series explorer section that consumes only the existing APIs:

- `GET /dashboard/timeseries/entities`
- `GET /timeseries/entities/{entity_id}/properties`
- `GET /timeseries/query`

Key UI decisions:

- keep the dashboard as plain HTML, CSS, and JavaScript
- continue using the optional bearer token from local browser `localStorage`
- support entity selection, observed-property selection, optional `from` and `to`, optional whole-range aggregation, and `limit`
- default to `aggregation=none`
- default to `limit=1000`
- render raw-point queries as a table showing `time`, `value`, `unit`, `observation_id`, and optional `raw_message_id`
- render aggregation responses as compact summary items derived from the existing `/timeseries/query` response shape
- add a small dependency-free SVG chart for numeric raw points only
- keep non-numeric values table-only and show a clear message when charting is not possible

## Consequences

Positive:

- operators can perform common historical inspection without leaving the AionCore dashboard
- the frontend stays easy to serve in dev and simple deployments
- existing backend contracts are exercised directly with no compatibility wrapper
- later richer visualization work can build on a validated UI flow

Trade-offs:

- the chart remains intentionally simple
- there is no zooming, panning, interval bucketing, or downsampling in this milestone
- aggregation summaries remain tied to the existing `points` response shape rather than a specialized dashboard schema
- Grafana integration remains deferred even though the UX direction is intentionally Grafana-like for exploration

## Future Work

- add interval bucket exploration when the backend supports it
- add richer chart interactions only if the static approach remains maintainable
- evaluate optional Grafana interoperability for advanced dashboards
- consider pagination or result navigation if large historical scans become common
