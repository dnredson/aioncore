# ADR 0016: PostgreSQL telemetry and event storage

## Status

Accepted

## Context

AionCore introduced a PostgreSQL adapter skeleton for control-plane data in Milestone 18. The platform also needs durable storage for telemetry-heavy records that drive queries, audits, and AI context:

- raw messages
- canonical observations
- events

These records are append-heavy and time-oriented, but they still need to remain compatible with the in-memory runtime and the existing storage abstractions.

## Decision

Extend the PostgreSQL adapter to support raw messages, observations, and events while keeping `InMemoryStorage` as the runtime default.

The adapter uses the existing PostgreSQL/TimescaleDB schema foundation:

- `raw_messages` stores the original payload before normalization.
- `observations` remains a TimescaleDB hypertable.
- `events` stores append-only operational and audit events.

The adapter is opt-in and validated through environment-gated parity tests using `AIONCORE_TEST_DATABASE_URL`.

## Consequences

- The PostgreSQL adapter now covers both control-plane and telemetry-oriented tables.
- The runtime behavior in the API remains unchanged until backend selection is introduced later.
- The codebase stays backend-pluggable, with Cassandra or other telemetry backends remaining future options.
- Optional PostgreSQL tests can verify parity without requiring Docker or a live database for the normal test suite.

