# ADR 0040: SmartSentinel Provenance Query Ergonomics

## Status

Accepted

## Context

Milestone 42 preserved SmartSentinel source, provenance, evidence, incident, alert, workflow, run, trace, cycle, and correlation references in raw-message, event, observation, and lifecycle metadata.

That made the data available for audit and explainability, but operators still had to retrieve broad result sets and inspect metadata manually. AionCore needed ergonomic query filters for common SmartSentinel operational references without making SmartSentinel a required runtime dependency.

## Decision

Add metadata filters to the existing API surfaces:

- `GET /events`
  - `incident_id`
  - `alert_id`
  - `trace_id`
  - `run_id`
  - `workflow_id`
  - `cycle_id`
  - `evidence_id`
  - `external_id`
- `GET /raw-messages`
  - `trace_id`
  - `run_id`
  - `workflow_id`
  - `cycle_id`
  - `correlation_id`
  - `snapshot_id`
  - `node_id`
  - `connector_id`
  - `connector_key`
  - `connector_profile`

Add `GET /provenance/search` as an aggregate read-only endpoint for provenance-oriented lookup across matching events, raw messages, and observations.

The implementation applies these filters in the API layer over existing metadata and raw-message headers. It does not add new core schema fields or require a SmartSentinel storage adapter.

## Consequences

- SmartSentinel remains optional and loosely coupled.
- Existing event and raw-message filters continue to work.
- Operators can query by incident, alert, trace, run, workflow, cycle, correlation, snapshot, node, and evidence references.
- The aggregate provenance endpoint supports audit and explainability without introducing execution behavior.
- PostgreSQL-specific JSONB indexes are left as future optimization work.
- Matching is exact and metadata-based; it does not fetch evidence URLs, call external systems, perform full-text search, or execute recovery actions.

## Non-Goals

- No SmartSentinel runtime.
- No live polling.
- No recovery execution.
- No authentication changes.
- No dashboard.
- No Cassandra adapter.
- No production MCP transport.
- No external AI calls.
- No evidence URI fetching.
