# ADR 0017: PostgreSQL Command and Rule Lifecycle Storage

## Status

Accepted

## Context

AionCore now has an opt-in PostgreSQL adapter skeleton. The telemetry-oriented
tables were added first, but the command lifecycle and rule engine still lived
only in memory. That left a gap for deployments that want durable approval,
claim, lease, action, and rule state while the runtime remains on
`InMemoryStorage`.

## Decision

Add PostgreSQL adapter support for:

- `commands`
- `actions`
- `action_results`
- `command_leases`
- `rules`

Keep the API runtime on in-memory storage for now. Keep the PostgreSQL adapter
opt-in and testable only when `AIONCORE_TEST_DATABASE_URL` is set. Preserve the
existing schema foundation and TimescaleDB-compatible observation storage.

## Consequences

- Command approval, claim, execute, fail, cancel, lease, and retry state can be
  persisted and queried in PostgreSQL.
- Rule definitions and enable/disable state can be persisted and queried in
  PostgreSQL.
- Local development and normal `cargo test` remain database-free.
- Backend selection is still future work.
- Cassandra remains a future optional telemetry backend, not a command lifecycle
  backend by default.
