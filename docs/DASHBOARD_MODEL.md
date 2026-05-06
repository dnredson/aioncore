# Dashboard Model

The AionCore dashboard is currently a read-only backend foundation. Milestones 81 and 82 add dashboard-oriented API summaries plus initial flow inventory counts so a future UI can explore entities, observed properties, connector operations, worker health, and pipeline inventory without changing ingestion, connector runtime, or time-series query behavior.

## Current Intent

- Keep the dashboard surface read-only and safe.
- Reuse existing canonical observations and raw operational records.
- Support a future exploration flow of `entity -> observed property -> historical chart`.
- Support future operational views for connectors, brokers, and workers.
- Preserve Grafana as a valid future option for advanced charting and long-range analytics.

## Current Read Surface

- `GET /dashboard/overview`
- `GET /dashboard/timeseries/entities`
- `GET /dashboard/connectors/overview`
- flow counts from `GET /dashboard/overview`

These endpoints are designed for compact dashboard landing pages and navigation panels rather than full raw-data export.

## Operational Focus

The future AionCore dashboard is intended to emphasize:

- entity and property exploration
- historical time-series discovery
- connector and broker inspection
- worker/runtime health
- later pipeline and flow visibility
- later flow list, detail, and graph rendering

This is intentionally different from a generic chart-only experience. Grafana can still complement AionCore for richer chart composition later.

## Deferred Work

The following are explicitly out of scope for this milestone:

- frontend UI assets
- dashboard SPA pages
- drag-and-drop flow editor
- Node-RED-like flow execution
- Grafana provisioning
- automated pipeline/rule authoring from the dashboard

Node-RED-like flow editing remains future work. The current milestones only establish safe backend summaries and the flow storage model that a future dashboard can build on.
