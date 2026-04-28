# ADR 0018: Runtime Storage Backend Selection

## Status

Accepted

## Context

AionCore has an in-memory reference implementation and an opt-in PostgreSQL
adapter. The API runtime still needs a way to choose between them without
changing handler code or forcing PostgreSQL on local development and tests.

## Decision

Introduce runtime storage backend selection through environment variables:

- `AIONCORE_STORAGE_BACKEND=memory|postgres`
- `AIONCORE_DATABASE_URL` when PostgreSQL is selected

Keep `InMemoryStorage` as the default backend. When PostgreSQL is selected, the
API initializes `PostgresStorage`, applies embedded migrations, and starts with
the durable backend instead of falling back to memory.

## Consequences

- Local development stays simple and database-free by default.
- PostgreSQL becomes a selectable runtime backend for durable deployments.
- Startup fails fast on unknown backend values or missing database URLs.
- The API handlers remain backend-agnostic and continue to use repository
  traits rather than database-specific APIs.
- Cassandra remains a future optional telemetry backend, not a runtime selection
  target in this milestone.
