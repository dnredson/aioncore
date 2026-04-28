# ADR 0013: PostgreSQL and TimescaleDB Persistence Model

## Status

Accepted

## Context

AionCore has grown beyond the original entity, raw message, and observation models. The in-memory runtime now includes payload profiles, capabilities, policies, commands, actions, action results, events, executor agents, command leases, and rules.

The project needs a durable schema foundation before adding a PostgreSQL repository adapter. The in-memory runtime should remain the reference behavior until the durable adapter can match its contracts and tests.

## Decision

Extend the migration set to cover all current in-memory models while keeping the runtime wired to `InMemoryStorage`.

Use PostgreSQL for relational state and JSONB for extensible semantic, payload, metadata, rule, policy, and result data. Use TimescaleDB for canonical observations, with `observations` remaining a hypertable partitioned by `observed_at`.

Add indexes for the common query patterns used by the current API and expected repository contracts, including tenant-scoped lookup of entities, relationships, observations, raw messages, commands, events, executor agents, command leases, and rules.

Do not add a database adapter, SQLx runtime wiring, authentication, dashboard, MQTT, or production MCP transport in this milestone.

## Consequences

- Durable persistence has a concrete schema target for future adapter work.
- The local API remains simple and in-memory for development.
- Migration validation can run through the existing Rust test suite without Docker or a live PostgreSQL instance.
- Observation references from other tables remain UUID values for now because the TimescaleDB hypertable primary key includes `observed_at`.
- Future work must add the PostgreSQL adapter, migration execution, connection configuration, and parity tests against the in-memory behavior.
