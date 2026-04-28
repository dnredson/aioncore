# Persistence Model

AionCore currently runs on the in-memory storage implementation. The PostgreSQL and TimescaleDB schema exists as a migration foundation for the future durable adapter, but the API is not wired to PostgreSQL yet.

## Current Runtime Boundary

- `InMemoryStorage` remains the reference behavior for the local API.
- `PostgresStorage` is introduced as an opt-in adapter skeleton, but it is not the default runtime backend.
- Migrations define the durable schema contract only.
- No runtime backend selection, connection pool wiring into the API, or production persistence switching is implemented in this milestone.
- Docker or a running PostgreSQL instance is not required for the Rust test suite.

## Pluggable Storage Strategy

AionCore storage must remain backend-pluggable. Core API handlers and domain crates should depend on repository traits and logical storage boundaries, not directly on a database driver, SQL dialect, or NoSQL client.

The intended deployment profiles are:

- Lightweight/local deployments: in-memory or a single general-purpose durable backend.
- Research and lab deployments: simple all-in-one runtime with deterministic behavior and inspectable data.
- Production deployments: PostgreSQL/TimescaleDB as the default durable reference backend.
- High-throughput horizontally scalable deployments: split telemetry/event-heavy storage to an optional backend such as Cassandra while keeping control-plane state in a strongly consistent store.

Backend selection is future work. The current code keeps `InMemoryStorage` as the runtime implementation and test reference.

## Logical Store Boundaries

The Rust storage crate exposes model-specific traits and lightweight aggregate boundaries:

- `ControlPlaneStore`
- `TelemetryStore`
- `RawMessageStore`
- `EventStore`
- `CommandStore`
- `RuleStore`
- `ExecutorStore`
- `PolicyStore`
- `AiContextStore`

`ControlPlaneStore` groups state that benefits from strong consistency, relational constraints, and clear tenant scoping. `TelemetryStore` groups append-heavy time-series, raw ingestion, and event streams. `AiContextStore` represents the read model needed to assemble semantic context from entities, relationships, observations, events, commands, actions, and action results.

These boundaries are intentionally logical. A single backend can implement all of them, or different backends can implement different boundaries later.

## Data Orientation

Control-plane oriented data:

- Tenants
- Entities
- Relationships
- Payload profiles
- Capabilities
- Policies
- Commands
- Actions
- Action results
- Rules
- Executor agents
- Executor capabilities
- Executor scopes
- Command leases

Telemetry/event oriented data:

- Observations
- Raw messages
- Events
- High-frequency operational metrics
- Future SmartSentinel snapshots or derived metrics

Some data can appear in both views. For example, events are telemetry/event oriented because they are append-heavy, but they also feed AI context and audit workflows. Commands are control-plane oriented because they need lifecycle constraints, approval gates, executor leases, and policy behavior.

## Backend Roles

Recommended backend roles:

- `InMemoryStorage`: development, unit tests, integration tests, examples, and reference behavior.
- PostgreSQL/TimescaleDB: first durable reference backend and default for general-purpose deployments.
- Cassandra or another wide-column NoSQL backend: optional future backend for high-throughput observations, raw messages, events, and telemetry-heavy workloads.
- Object storage such as S3 or MinIO: optional future backend for large raw payloads, binary attachments, or long-retention archives.
- Search/index backend such as OpenSearch: optional future backend for text search, log search, and operational investigation.

PostgreSQL/TimescaleDB remains the first durable target because it can cover both control-plane and time-series needs in one operationally simple backend. Cassandra should not be introduced as a hard dependency; it should be an optional adapter for deployments whose write volume or retention model justifies it.

## Database Platform

The durable target is PostgreSQL with TimescaleDB enabled. Migration `0001_create_tenants.sql` enables:

- `pgcrypto` for UUID generation through `gen_random_uuid()`.
- `timescaledb` for time-series observation storage.

Canonical observations remain TimescaleDB-compatible through the `observations` hypertable partitioned by `observed_at`.

## Adapter Coverage

The PostgreSQL adapter currently covers the control-plane subset needed for parity testing and future expansion:

- `tenants`
- `entities`
- `entity_relationships`
- `payload_profiles`
- `capabilities`
- `policies`
- `executor_agents`
- `executor_capabilities`
- `executor_scopes`

The migration set still covers the current in-memory domain models:

- `tenants`
- `entities`
- `entity_relationships`
- `raw_messages`
- `observations`
- `payload_profiles`
- `capabilities`
- `policies`
- `commands`
- `actions`
- `action_results`
- `events`
- `executor_agents`
- `executor_capabilities`
- `executor_scopes`
- `command_leases`
- `rules`

The remaining persistence areas are planned for later milestones:

- `observations`
- `raw_messages`
- `events`
- `commands`
- `actions`
- `action_results`
- `command_leases`
- `rules`

## JSONB Fields

JSONB is used for structured or extensible fields:

- Entity JSON-LD documents: `entities.jsonld`
- Relationship JSON-LD fragments: `entity_relationships.jsonld`
- Raw message headers: `raw_messages.headers`
- Observation values, quality, and metadata: `observations.value_json`, `observations.quality`, `observations.metadata`
- Payload profile mappings and metadata: `payload_profiles.attribute_mapping`, `payload_profiles.metadata`
- Capability, policy, command, action, result, event, executor, lease, and rule metadata
- Command payloads and policy decisions
- Action result payloads
- Rule conditions and actions

## TimescaleDB Observation Notes

`observations` uses a composite primary key of `(observed_at, id)` because TimescaleDB unique constraints on hypertables must include the partitioning column. Tables that need to refer to an observation store `observation_id` as a UUID reference value, but the current migration foundation does not add a foreign key to `observations(id)` because `id` is not globally unique at the database constraint level without `observed_at`.

## Query Indexes

The schema includes indexes for common local API and future repository queries:

- Entities by tenant, key, and type.
- Relationships by source, target, and relationship type.
- Observations by feature of interest, producer, observed property, and time.
- Raw messages by producer, feature of interest, payload format, received time, device, and normalization status.
- Commands by target entity, status, approval status, and command type.
- Events by target, source, event type, severity, command, raw message, and correlation ID.
- Executor agents by agent key and status.
- Executor scopes and capabilities by agent and matching fields.
- Command leases by command, executor, status, and expiry.
- Rules by enabled state, trigger type, observed property, and event type.

## Future Adapter Expectations

The future PostgreSQL repository adapter should preserve the same behavior as the in-memory stores before becoming the default runtime. It should:

- Use tenant scoping on all tenant-owned rows.
- Preserve JSON-LD and payload-agnostic ingestion behavior.
- Store raw messages before normalization.
- Keep command approval, executor scope, lease, and retry semantics consistent with the in-memory implementation.
- Keep SmartSentinel or any other domain pack optional.

Runtime persistence, migrations execution strategy, connection configuration, and operational hardening are future work.

## Optional PostgreSQL Tests

PostgreSQL parity tests are opt-in and skip cleanly when `AIONCORE_TEST_DATABASE_URL` is not set.

Run them with:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_
```

The tests apply the embedded migrations before exercising the adapter. The database must have access to the required PostgreSQL and TimescaleDB extensions.
