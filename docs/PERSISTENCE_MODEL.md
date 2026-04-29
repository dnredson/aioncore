# Persistence Model

AionCore currently runs on the in-memory storage implementation. The PostgreSQL and TimescaleDB schema exists as a migration foundation for the future durable adapter, but the API is not wired to PostgreSQL yet.

## Current Runtime Boundary

- `InMemoryStorage` remains the reference behavior for the local API.
- Runtime storage backend selection is available.
- `InMemoryStorage` remains the default runtime backend and the reference behavior for the local API.
- `PostgresStorage` is introduced as an opt-in backend, but it is not the default runtime backend.
- Migrations define the durable schema contract only.
- Docker or a running PostgreSQL instance is not required for the Rust test suite.
- `/health` is a lightweight liveness check.
- `/ready` is the storage readiness check.
- PostgreSQL mode does not fall back to memory if readiness fails.

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
- `commands`
- `actions`
- `action_results`
- `command_leases`
- `rules`
- `ingestion_connectors`
- `connector_secrets`
- `ttn_device_mappings`

The PostgreSQL adapter now also covers the telemetry-oriented core tables:

- `raw_messages`
- `observations`
- `events`

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
- `ingestion_connectors`

Remaining adapter areas are planned for later milestones:

- none for the current command and rule lifecycle scope
- connector status persistence; connector status remains runtime-derived for now

## JSONB Fields

JSONB is used for structured or extensible fields:

- Entity JSON-LD documents: `entities.jsonld`
- Relationship JSON-LD fragments: `entity_relationships.jsonld`
- Raw message headers: `raw_messages.headers`
- Observation values, quality, and metadata: `observations.value_json`, `observations.quality`, `observations.metadata`
- Payload profile mappings and metadata: `payload_profiles.attribute_mapping`, `payload_profiles.metadata`
- Ingestion connector metadata: `ingestion_connectors.metadata`
- Connector secret metadata: `connector_secrets.metadata`
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
- Ingestion connectors by tenant, key, type, profile, and enabled state.
- Connector secrets by tenant, key, type, and connector reference.
- TTN device mappings by tenant, connector, application ID, device ID, and enabled state.

## Ingestion Connector Persistence

PostgreSQL now persists ingestion connector configuration through `ingestion_connectors`.

This includes generic HTTP connectors, generic AionCore MQTT connectors, generic MQTT connectors, and provider-specific profiles such as `ttn-v3`. Persisting these records is a prerequisite for future dynamic worker startup, where enabled MQTT connectors can become runtime workers after restart.

Connector status remains runtime-derived in the current model. The table persists connector configuration and enablement, but it does not yet persist last error, last message time, last successful ingest time, or last failed ingest time.

Secrets are not stored in ingestion connector records. Connectors can hold `secret_ref_id`, which points to `connector_secrets`.

## Connector Secret Persistence

PostgreSQL now persists connector secrets through `connector_secrets`.

Connector secret rows include:

- `id`
- `tenant_id`
- `secret_key`
- `secret_type`: `mqtt_basic_auth`, `token`, `api_key`, or `custom`
- optional `username`
- `secret_value`
- optional JSONB metadata
- timestamps

The API treats `secret_value` as write-only and never returns it in create/get/list responses. Event metadata, worker status, readiness, connector responses, raw-message headers, and debug output must not include secret values.

This milestone deliberately does not implement encryption, KMS, Vault, rotation, access policy, TLS/mTLS, or per-device MQTT authorization. The stored value is suitable for local-development and adapter-plumbing validation only. Production deployments should replace or harden this with an external secret manager and encrypted-at-rest behavior.

The `ttn-v3` connector profile configuration is persisted, including broker URL, topic filter, payload format, and metadata. Full TTN adapter behavior remains future work.

## TTN Device Mapping Persistence

PostgreSQL now persists explicit TTN device-to-entity mapping rules through `ttn_device_mappings`.

Rows include tenant and connector IDs, optional `ttn_application_id`, required `ttn_device_id`, required `producer_entity_id`, optional `feature_of_interest_id`, `enabled`, optional metadata, and timestamps. The table references existing connectors and entities; it does not auto-provision entities from TTN device IDs.

Mapping lookup is scoped to tenant and connector, requires an enabled row, and prefers an exact application ID match over a connector/device fallback row with no application ID.

Mappings can be updated or deleted through the storage interface and API. Updates can change TTN application/device IDs, target entity IDs, enabled state, and metadata; they cannot change mapping ID, tenant ID, or connector ID. Deletes remove the mapping row only.

Conflict handling is enforced in storage logic and by PostgreSQL enabled-row uniqueness indexes. Enabled exact mappings are unique by tenant, connector, application ID, and device ID. Enabled fallback mappings are unique by tenant, connector, and device ID where `ttn_application_id IS NULL`. This allows one application-specific mapping and one fallback mapping to coexist while preventing ambiguous enabled fallback resolution.

## Future Adapter Expectations

The future PostgreSQL repository adapter should preserve the same behavior as the in-memory stores before becoming the default runtime. It should:

- Use tenant scoping on all tenant-owned rows.
- Preserve JSON-LD and payload-agnostic ingestion behavior.
- Store raw messages before normalization.
- Keep command approval, executor scope, lease, and retry semantics consistent with the in-memory implementation.
- Keep SmartSentinel or any other domain pack optional.

Runtime persistence, migrations execution strategy, connection configuration, and operational hardening are future work.
Runtime readiness now validates storage connectivity through `/ready`, and startup diagnostics report the selected backend, whether a database URL was provided in postgres mode, and whether embedded migrations were applied.

## Optional PostgreSQL Tests

PostgreSQL parity tests are opt-in and skip cleanly when `AIONCORE_TEST_DATABASE_URL` is not set.

Run them with:

```powershell
$env:AIONCORE_TEST_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo test -p aion-storage postgres_
```

Lifecycle parity coverage includes command create/query/update behavior, action and action result storage, command lease persistence, rule create/list/enable/disable behavior, and ingestion connector create/list/get/enable/disable behavior.

The tests apply the embedded migrations before exercising the adapter. The database must have access to the required PostgreSQL and TimescaleDB extensions.

Telemetry parity coverage includes raw message filtering, canonical observation storage and lookup, and event storage/query behavior. `InMemoryStorage` remains the runtime default and the reference for local tests outside the PostgreSQL-specific suite.

The optional runtime validation path uses the same backend-selection variables as the server:

```powershell
$env:AIONCORE_STORAGE_BACKEND = "postgres"
$env:AIONCORE_DATABASE_URL = "postgres://user:password@localhost:5432/aioncore"
cargo run -p aion-api
```
