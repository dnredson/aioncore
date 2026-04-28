# ADR 0019: PostgreSQL Runtime Readiness

## Status

Accepted

## Context

Milestone 21 introduced runtime storage backend selection. The API could start
with PostgreSQL, but the startup path needed clearer diagnostics and a formal
readiness check that distinguishes simple process liveness from actual storage
availability.

## Decision

Add a lightweight `/health` endpoint for liveness and a `/ready` endpoint for
storage readiness.

- `/health` reports the active backend and avoids expensive database checks.
- `/ready` verifies storage readiness.
- In memory mode, `/ready` reports ready immediately.
- In PostgreSQL mode, `/ready` performs a connectivity probe and returns a
  not-ready response if the database cannot be reached.
- PostgreSQL mode does not fall back to memory.

Startup diagnostics report the selected backend, whether a database URL was
provided in PostgreSQL mode, and whether embedded migrations were applied during
startup.

## Consequences

- Operators can distinguish process health from storage readiness.
- PostgreSQL startup failures are clearer and fail fast instead of degrading to
  memory.
- The API remains safe for local development because memory stays the default.
- Runtime validation can be exercised without making PostgreSQL mandatory for
  normal tests.
