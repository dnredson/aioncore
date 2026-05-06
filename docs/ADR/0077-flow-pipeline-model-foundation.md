# ADR 0077: Flow Pipeline Model Foundation

## Status

Accepted

## Context

AionCore already has ingestion connectors, rules, commands, events, observations, MQTT workers, and dashboard read APIs. It does not yet have an explicit model for operator-configured source -> processing -> sink graphs.

Project members use Node-RED and want a clearer operational configuration model than today's disconnected connector and rule records provide. At the same time, Milestone 82 must not introduce:

- visual UI
- drag-and-drop editing
- runtime flow execution
- MQTT or HTTP sink dispatch
- changes to existing ingestion behavior
- changes to existing rule behavior
- changes to connector worker behavior

## Decision

Add a first-class tenant-scoped `Flow` model with:

- generic nodes and edges
- a compact `FlowNodeType` enum
- JSON config for node-specific conventions
- enable and disable state
- CRUD plus enable and disable APIs
- in-memory and PostgreSQL persistence
- token-mode scopes and tenant-aware filtering
- lifecycle audit events

## Consequences

### Positive

- AionCore now has an explicit backend contract for future Node-RED-like operator configuration.
- Dashboard and API consumers can list, inspect, and manage flow definitions without touching runtime execution.
- Existing ingestion, rules, commands, events, and observations remain intact and can later be composed through the flow model.
- PostgreSQL and in-memory storage stay aligned.

### Negative

- The platform still cannot execute flows.
- Generic JSON config keeps the model flexible but does not yet strongly type each node kind.
- Operators can create `mqtt_publish`, `http_forward`, and `dlq` sink definitions before those runtimes exist, so documentation must keep the non-execution boundary explicit.

## Follow-Up

Likely next flow-related milestones:

- dashboard flow list and detail views
- stronger node-kind validation helpers
- dry-run validation and simulation
- flow execution engine
- runtime sink dispatch and DLQ handling
- visual graph editor
