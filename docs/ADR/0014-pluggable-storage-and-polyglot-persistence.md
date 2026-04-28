# ADR 0014: Pluggable Storage and Polyglot Persistence

## Status

Accepted

## Context

AionCore must support lightweight local use, research and lab deployments, production deployments, and high-throughput horizontally scalable deployments. These profiles do not all need the same storage topology.

PostgreSQL with TimescaleDB is a strong default because it can store control-plane data, JSONB documents, and canonical observation time-series in one durable backend. However, some deployments may eventually need a write-optimized telemetry/event backend such as Apache Cassandra, object storage for large raw payloads, or search infrastructure for log and text investigation.

The core runtime should not hardcode one database into API handlers or domain logic.

## Decision

Keep storage access behind repository traits and logical store boundaries. `InMemoryStorage` remains the test and local-development reference behavior. PostgreSQL/TimescaleDB remains the first durable reference backend and the default target for general-purpose deployments.

Define logical storage boundaries:

- `ControlPlaneStore` for tenants, entities, relationships, payload profiles, capabilities, policies, commands, actions, action results, rules, executors, and command leases.
- `TelemetryStore` for observations, raw messages, and events.
- Model-specific stores such as `RawMessageStore`, `EventStore`, `CommandStore`, `RuleStore`, `ExecutorStore`, and `PolicyStore`.
- `AiContextStore` for semantic context assembly across entities, relationships, observations, events, commands, actions, and action results.

Cassandra or another wide-column NoSQL backend may be added later as an optional adapter for high-throughput telemetry/event workloads. Object storage and search backends may also be added later for payload archives and investigation workflows.

Do not implement Cassandra, switch runtime storage, or replace the in-memory runtime in this milestone.

## Consequences

- Core APIs remain insulated from specific database drivers.
- The current in-memory implementation continues to define expected behavior in tests.
- PostgreSQL/TimescaleDB can be implemented first without blocking future polyglot storage.
- High-volume telemetry/event paths can evolve independently from control-plane state.
- Future adapters must preserve tenant scoping, command lifecycle behavior, lease semantics, rule behavior, and AI context query contracts.
