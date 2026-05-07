# ADR 0094: Flow Execution UI Integration

## Status

Accepted

## Context

Milestone 98 added simulated flow execution endpoints:

- `POST /flows/execute`
- `POST /flows/{flow_id}/execute`

Those endpoints already interpret supported flow nodes against explicit input and return preview artifacts such as:

- `node_results`
- `sink_results`
- `observations_preview`
- `events_preview`
- `commands_preview`
- `dlq_preview`

They intentionally keep execution side-effect-free:

- `simulated = true`
- `side_effects_performed = false`
- no MQTT publish
- no HTTP forward
- no observation writes
- no event creation
- no command creation
- no DLQ writes

Before this milestone, the static dashboard could validate and dry-run flows but could not expose the simulated execute surface.

## Decision

Integrate simulated execute into the static dashboard only, without changing backend behavior.

The dashboard now:

- adds proposed-flow simulated execute in the Flow Builder using `POST /flows/execute`
- adds stored-flow simulated execute in the stored-flow detail panel using `POST /flows/{flow_id}/execute`
- validates JSON input client-side for `sample_payload` and optional metadata before sending
- keeps optional bearer-token handling and shows clearer scope guidance for `flows:read`, `dashboard:read`, and optional `connectors:read`
- renders redacted request previews and redacted execution responses
- projects returned `node_results` and sink conceptual actions onto the existing immutable SVG graph
- keeps stored-flow graphs read-only in place and does not mutate graph structure during execution preview

## Validation, Dry-Run, And Execute Boundary

The dashboard must preserve the distinction between the three operator surfaces:

- validation checks structural correctness and references
- dry-run reports conceptual path and conceptual sink effects without interpreting node logic
- simulated execute interprets supported node logic against explicit input but still performs no side effects

## Consequences

Positive:

- operators can inspect realistic preview behavior from the existing dashboard without leaving the static UI
- the graph now communicates simulated node status and sink intent more clearly
- the platform keeps the safe preview-only execution boundary

Tradeoffs:

- the UI must carry more state for validation, dry-run, execute request previews, and execute response rendering
- execution metadata remains frontend-only request enrichment until the backend defines a dedicated typed contract for it

## Deferred

This ADR does not add:

- real flow execution
- worker integration
- broker subscriptions
- MQTT publish
- HTTP forward
- observation persistence
- event persistence
- command persistence
- DLQ persistence
- frontend build tooling
