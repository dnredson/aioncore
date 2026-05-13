# AionCore MVP Scope Freeze

Milestone 106 freezes the current MVP boundary for demonstration and review.

The MVP is not a production release. It is a coherent local demonstration of the platform direction: semantic IoT entities, raw-message-first ingestion, reliable/backfill ingestion, dashboard exploration, flow configuration, guarded execution previews, DLQ planning, and sync-session tracking.

## In MVP

The following capabilities are in the MVP boundary:

- JSON-LD entities and relationships.
- RawMessage preservation before normalization.
- Canonical Observation creation from HTTP/reliable/batch ingestion.
- Tenant-aware in-memory and PostgreSQL persistence foundations.
- API token model and staged token-mode protection.
- Dashboard APIs and static no-build dashboard served optionally under `/ui/`.
- Time-series query and entity/property explorer UI.
- IngestionConnector registry, connector status, and dynamic worker foundations.
- TTN mapping/validation/preflight foundations.
- SmartSentinel snapshot/provenance/executor bridge foundations.
- Aion Edge Adapter registration/status contract.
- Flow model, validation, dry-run, visual graph, typed inspectors, and execution preview.
- Guarded flow side-effect authorization model.
- Safe internal flow side effects and guarded MQTT/HTTP sink execution foundations.
- DLQ model/API and replay-planning/request foundations.
- Reliable ingestion envelope, tenant-scoped idempotency, batch/backfill ingestion, and sync-session tracking.
- NiFi/MiNiFi integration boundary and provenance conventions.

## Out Of MVP

The following are intentionally deferred:

- production security certification or public exposure readiness.
- full tenant/resource authorization coverage for every route.
- OIDC/OAuth/JWT provider integration.
- full production MCP transport hardening.
- live TTN downlinks and production TTN runtime hardening.
- automatic DLQ routing from all failure paths.
- replay worker execution.
- automatic enabled-flow runtime workers.
- full Node-RED-like arbitrary drag-and-drop editor.
- Grafana provisioning as a required component.
- Cassandra/high-throughput telemetry backend.
- production secret manager/Vault/KMS.
- SmartSentinel runtime agent deployment.
- Aion Edge Adapter runtime implementation.

## Freeze Rules

For MVP review, avoid expanding scope unless it fixes a blocking defect.

Allowed after freeze:

- documentation corrections
- demo script fixes
- validation fixes
- bug fixes needed for the MVP scenario
- small redaction/security corrections

Deferred after freeze:

- new runtime subsystems
- new external dependencies
- new production integrations
- broad refactors
- major UI rewrites

## Recommended Review Checklist

Before presenting the MVP:

1. Run `cargo fmt --all`.
2. Run `cargo build -p aion-api`.
3. Run `cargo test -p aion-storage`.
4. Run `cargo test -p aion-api`.
5. Start `aion-api` with `AIONCORE_DASHBOARD_STATIC_DIR=apps/aion-dashboard`.
6. Run `scripts/demo-mvp-memory.ps1`.
7. Open `/ui/` and inspect overview, time-series, connectors, and flows.
8. Confirm no `target*`, `node_modules`, or smoke logs are tracked by Git.
