# Persistence Model

AionCore currently runs on the in-memory storage implementation. The PostgreSQL and TimescaleDB schema exists as a migration foundation for the future durable adapter, but the API is not wired to PostgreSQL yet.

## Current Runtime Boundary

- `InMemoryStorage` remains the reference behavior for the local API.
- Migrations define the durable schema contract only.
- No SQLx adapter, connection pool, runtime database selection, or production persistence wiring is implemented in this milestone.
- Docker or a running PostgreSQL instance is not required for the Rust test suite.

## Database Platform

The durable target is PostgreSQL with TimescaleDB enabled. Migration `0001_create_tenants.sql` enables:

- `pgcrypto` for UUID generation through `gen_random_uuid()`.
- `timescaledb` for time-series observation storage.

Canonical observations remain TimescaleDB-compatible through the `observations` hypertable partitioned by `observed_at`.

## Tables

The migration set covers the current in-memory domain models:

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
