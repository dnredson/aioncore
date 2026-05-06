# AionCore Roadmap

This roadmap summarizes completed work and the next planned milestones for AionCore.

## Completed

- Core domain model: JSON-LD entities, relationships, and semantic context.
- Ingestion HTTP/MQTT: payload-agnostic HTTP ingestion and MQTT foundations.
- Payload profiles and raw messages: raw-message-first capture, profiles, and decoder-driven normalization.
- Command/action/policy/executor lifecycle: commands, actions, leases, approvals, and executor agents.
- Event/audit timeline: event logging and provenance-oriented audit records.
- AI context and MCP-style tools: local AI context assembly and read-oriented MCP-style tooling.
- PostgreSQL/TimescaleDB persistence foundation: durable schema and parity work for core runtime models.
- Connector registry and dynamic MQTT workers: connector records, enable/disable flows, validation, and worker planning.
- TTN v3 decoding/mapping/validation/preflight: TTN uplink decoding, device mappings, validation, and live-readiness planning.
- SmartSentinel integration: snapshot ingestion, provenance, evidence, and executor bridge support.
- Edge Adapter architecture: optional edge/fog model, registration, and status contract.
- Security model: documented principals, credential types, scopes, trust boundaries, and auth roadmap.
- Milestone 55 auth hardening: `/events*` and `/raw-messages*` now require dedicated read scopes in `token` mode.
- Milestone 56 auth hardening: broader read-surface coverage now protects selected entity, observation, command, action, rule, policy, capability, and executor-inspection reads in `token` mode.
- Milestone 57 auth hardening: selected token mode protected read surfaces now enforce the first tenant/resource ownership skeleton, including tenant-filtered lists and `403` on known cross-tenant detail reads.
- Milestone 58 documentation simplification: the root `README.md` now stays concise and operational examples live in focused usage guides under `docs/`.
- Milestone 59 documentation consistency pass: usage guides, model docs, roadmap references, and documentation navigation now use normalized cross-links and terminology.
- Milestone 60 auth hardening: selected token mode write paths now enforce explicit write scopes plus first-pass tenant-aware create/update checks without cross-tenant sharing.
- Milestone 61 modularization foundation: `apps/aion-api/src/lib.rs` now begins a staged split by extracting cohesive auth code into `src/auth.rs` while preserving runtime behavior and deferring route-level refactors.
- Milestone 62 modularization foundation: `apps/aion-api/src/lib.rs` now extracts shared API error and response primitives into `src/error.rs` so later route modules can reuse stable HTTP failure behavior without changing endpoint semantics.
- Milestone 63 route modularization foundation: `apps/aion-api/src/lib.rs` now extracts the Edge Adapter route group into `src/routes/adapters.rs`, preserving endpoint paths, auth semantics, entity projection, and event behavior while establishing the first dedicated route module.
- Milestone 64 route modularization: `apps/aion-api/src/lib.rs` now extracts the auth/token HTTP surface into `src/routes/auth.rs`, preserving auth mode behavior, token issuance/validation semantics, audit events, endpoint paths, and JSON response shapes while continuing the staged route split.
- Milestone 65 route modularization: `apps/aion-api/src/lib.rs` now extracts the executor HTTP surface into `src/routes/executors.rs`, preserving endpoint paths, executor auth scopes, tenant/resource ownership behavior, polling/claim/complete/fail semantics, command lease behavior, and executor event metadata while continuing the staged route split.
- Milestone 66 helper modularization: `apps/aion-api/src/lib.rs` now extracts shared command/action/lease support logic into `src/command_support.rs`, preserving command lifecycle behavior, lease semantics, executor compatibility checks, SmartSentinel bridge behavior, event metadata, and JSON shapes while reducing risk before command route extraction.
- Milestone 67 route modularization: `apps/aion-api/src/lib.rs` now extracts the generic command, command-lease, action, and action-result HTTP surface into `src/routes/commands.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership checks, command lifecycle behavior, lease/retry semantics, executor compatibility, SmartSentinel bridge behavior, event metadata, and JSON shapes while continuing the staged route split.
- Milestone 68 route modularization: `apps/aion-api/src/lib.rs` now extracts the SmartSentinel snapshot ingestion and executor bridge HTTP surface into `src/routes/smartsentinel.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership checks, raw-message-first mapping behavior, provenance/evidence metadata, executor bridge lifecycle semantics, and JSON shapes while continuing the staged route split.

## Next

1. Milestone 69: review whether remaining open write surfaces should split into narrower operator and machine scopes.
2. Milestone 70: add production MCP transport hardening, including Origin validation and stronger browser-facing transport controls.
3. Continue incremental `aion-api` route extraction only where a route group or shared read/query surface still has enough remaining cohesion to justify a dedicated module.

## Future

- Cassandra telemetry adapter.
- Dashboard.
- Production MCP transport.
- SmartSentinel runtime and agent integration.
- Aion Edge Adapter runtime.

## Notes

- The roadmap is intentionally concise. Canonical details live in the individual model docs and ADRs.
- Security hardening remains staged after the current selected write-surface rollout to avoid overreaching beyond verified behavior in one milestone.
- `aion-api` modularization is intentionally incremental; Milestones 61 through 68 establish safe extraction patterns for later route-level splits.
