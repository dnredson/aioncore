# ADR 0101: MVP Demo Scenario And Documentation Freeze

## Status

Accepted.

## Context

AionCore has reached a broad MVP-capable state across semantic IoT modeling, ingestion, reliable/backfill handling, dashboard exploration, flow configuration, guarded execution, DLQ planning, and sync-session tracking.

The project needs a stable demo scenario and a clear freeze boundary before more ambitious post-MVP work such as full replay workers, arbitrary visual flow editing, production auth hardening, Grafana provisioning, Cassandra, and deeper NiFi/MiNiFi examples.

## Decision

Milestone 106 freezes the current MVP boundary around a local memory-backed demonstration.

The MVP demo uses:

- `cargo run -p aion-api`
- optional static dashboard hosting through `AIONCORE_DASHBOARD_STATIC_DIR=apps/aion-dashboard`
- `scripts/demo-mvp-memory.ps1`
- documentation in `docs/MVP_DEMO_SCENARIO.md` and `docs/MVP_SCOPE_FREEZE.md`

The demo intentionally stays local-first and avoids required external dependencies.

## Consequences

The MVP can be demonstrated without PostgreSQL, NiFi, MiNiFi, TTN credentials, live MQTT brokers, Grafana, or external HTTP targets.

Post-MVP work should use this freeze as a baseline and should not destabilize the demo path without a deliberate decision.

## Non-Goals

This ADR does not declare AionCore production-ready.

It does not add new runtime behavior, new API endpoints, replay workers, or new side-effect execution paths.
