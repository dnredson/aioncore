# Dashboard Model

The AionCore dashboard is currently a read-only backend foundation. Milestones 81, 82, 84, 87, and 88 add dashboard-oriented API summaries plus flow and DLQ inventory counts so a future UI can explore entities, observed properties, connector operations, worker health, pipeline inventory, flow validation readiness, and basic reliability backlog without changing ingestion, connector runtime, or time-series query behavior.

## Current Intent

- Keep the dashboard surface read-only and safe.
- Reuse existing canonical observations and raw operational records.
- Support a future exploration flow of `entity -> observed property -> historical chart`.
- Support future operational views for connectors, brokers, and workers.
- Preserve Grafana as a valid future option for advanced charting and long-range analytics.

## Current Read Surface

- `GET /dashboard/overview`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`
- `GET /dashboard/timeseries/entities`
- `GET /dashboard/connectors/overview`
- flow counts from `GET /dashboard/overview`
- DLQ counts from `GET /dashboard/overview`

These endpoints are designed for compact dashboard landing pages and navigation panels rather than full raw-data export.

## Operational Focus

The future AionCore dashboard is intended to emphasize:

- entity and property exploration
- historical time-series discovery
- connector and broker inspection
- worker/runtime health
- later pipeline and flow visibility
- flow list, detail, and graph rendering from dashboard-friendly read models
- flow validation and dry-run inspection without execution
- later reliability and provenance visibility for external flow engines such as NiFi and MiNiFi

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

## Flow Inventory And Detail

The dashboard flow endpoints are optimized for future UI inventory panels and detail drawers:

- `GET /dashboard/flows` returns compact inventory records with graph counts and validation status.
- `GET /dashboard/flows/{flow_id}` returns flow metadata, redacted nodes, edges, graph summary, validation summary, planned path, referenced connectors, and planned sinks.

These endpoints are intentionally read-only:

- they do not execute flows
- they do not subscribe to brokers
- they do not publish MQTT or forward HTTP
- they do not create observations, events, commands, or DLQ records

Dashboard flow detail uses the same secret-like config redaction behavior introduced for flow validation and dry-run. Future UI work can safely render node configuration summaries without exposing secret material.

Future dashboard flow-builder work should call:

- `GET /flows/{flow_id}/validation`
- `POST /flows/{flow_id}/dry-run`

The dashboard flow detail endpoint complements those APIs. `/dashboard/flows/{flow_id}` is optimized for graph rendering and dashboard inspection, while `/flows/{flow_id}/validation` and `/flows/{flow_id}/dry-run` remain the canonical planning and validation endpoints.

For reliable external runtimes, future dashboard work should be able to show:

- whether a flow is internal-only or references an external engine
- replay and backfill markers on relevant ingestion timelines
- provenance links carried through raw messages, events, and observations
- future DLQ and batch session summaries

See [NiFi Integration Model](NIFI_INTEGRATION_MODEL.md).
