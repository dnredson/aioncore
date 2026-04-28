# ADR 0015: PostgreSQL Adapter Skeleton for Control-Plane Storage

## Status

Accepted

## Context

AionCore needs a durable storage path without losing the in-memory runtime that defines current local behavior. The storage architecture must stay pluggable so later deployments can support different backends without changing API handlers or domain logic.

The first durable adapter should cover the control-plane tables that are most naturally relational and easiest to validate against the in-memory implementation. Telemetry-heavy areas, command lifecycle persistence, and other high-volume paths can follow later.

## Decision

Add an opt-in `PostgresStorage` skeleton in the storage crate. It connects through a plain PostgreSQL client, can apply the embedded migrations, and implements the initial control-plane subset:

- tenants
- entities
- entity relationships
- payload profiles
- capabilities
- policies
- executor agents
- executor capabilities
- executor scopes

The API runtime remains on `InMemoryStorage`. PostgreSQL is not selected by default, and no database adapter is wired into the service layer yet.

PostgreSQL tests are opt-in and skip cleanly when `AIONCORE_TEST_DATABASE_URL` is not provided.

## Consequences

- The codebase now has a concrete PostgreSQL adapter target for future runtime wiring.
- In-memory behavior remains the source of truth for current local development.
- Integration tests can exercise the adapter without forcing PostgreSQL on every contributor.
- Telemetry, events, commands, actions, leases, and rules remain available for later backend milestones.
