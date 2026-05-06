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
- Milestone 69 route modularization: `apps/aion-api/src/lib.rs` now extracts the MCP-style local tool routes and minimal JSON-RPC compatibility handlers into `src/routes/mcp.rs`, preserving endpoint paths, auth semantics, tool behavior, AI context behavior, JSON shapes, and the intentionally minimal non-production MCP transport.
- Milestone 70 route modularization: `apps/aion-api/src/lib.rs` now extracts the shared AI context builder into `src/ai_context.rs` and the `GET /ai/context/entity/{entity_id}` HTTP surface into `src/routes/ai.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership behavior, AI context content, MCP compatibility, and the current no-external-LLM behavior.
- Milestone 71 route modularization: `apps/aion-api/src/lib.rs` now extracts `GET /provenance/search` into `src/routes/provenance.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership behavior, provenance/event/raw-message/observation filtering behavior, SmartSentinel provenance compatibility, response JSON shapes, count behavior, and the current no-evidence-fetching local-only query flow.
- Milestone 72 route modularization: `apps/aion-api/src/lib.rs` now extracts the `/events*` and `/raw-messages*` HTTP surface into `src/routes/events.rs` and `src/routes/raw_messages.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership behavior, event/raw-message filtering behavior, response JSON shapes, raw-message response shaping, and provenance-search compatibility through shared query-filter primitives in `src/query_filters.rs`.
- Milestone 73 route modularization: `apps/aion-api/src/lib.rs` now extracts the `/observations` HTTP surface into `src/routes/observations.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership behavior, direct observation creation behavior, rule-evaluation triggering, observation query filtering behavior, and JSON shapes ahead of later historical time-series API work.
- Milestone 74 route modularization: `apps/aion-api/src/lib.rs` now extracts the entity-centered `/entities*` and `/relationships` HTTP surface into `src/routes/entities.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership behavior, JSON-LD parsing, `entity_key` derivation, relationship behavior, capability behavior, payload-profile behavior, and JSON shapes before later historical query and dashboard-facing work.
- Milestone 75 route modularization: `apps/aion-api/src/lib.rs` now extracts the HTTP ingestion surface into `src/routes/ingestion.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership behavior, raw-message-first preservation, payload decoding, connector-aware defaults, TTN-over-HTTP mapping resolution, rule evaluation, and JSON shapes while intentionally leaving connector admin, TTN admin/operations, and worker management in `lib.rs` for later milestones.
- Milestone 76 route modularization: `apps/aion-api/src/lib.rs` now extracts the ingestion connector administration surface into `src/routes/connectors.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership behavior, connector create/list/get/update/enable/disable/status behavior, connector lifecycle events, and post-mutation worker reconciliation while intentionally leaving TTN mapping/validation/live operations and worker management in `lib.rs` for later milestones.
- Milestone 77 route modularization: `apps/aion-api/src/lib.rs` now extracts the TTN mapping, TTN validation, TTN live-readiness, and TTN live-validation HTTP surface into `src/routes/ttn.rs`, preserving endpoint paths, auth semantics, tenant/resource ownership behavior, TTN device mapping behavior, TTN validation diagnostics, dry-run/live-preflight safety behavior, and JSON shapes while intentionally leaving ingestion worker plan/status/reconcile routes in `lib.rs` for the next extraction step.
- Milestone 78 route modularization: `apps/aion-api/src/lib.rs` now extracts the ingestion worker plan/status/reconcile HTTP surface into `src/routes/workers.rs`, preserving endpoint paths, auth semantics, `/ready` worker summaries, worker planner output, dynamic reconciliation behavior, MQTT worker behavior, TTN worker skip behavior, and JSON shapes while intentionally leaving shared worker planner/runtime orchestration in `lib.rs` for a later cleanup pass.

## Next

1. Continue incremental `aion-api` modularization only where a remaining shared support surface has enough cohesion to justify extraction, with worker planner/runtime support cleanup planned next rather than broader route movement.
2. Review whether remaining open write surfaces should split into narrower operator and machine scopes.
3. Add historical observation/time-series query APIs without changing the existing `/observations` behavior.
4. Add production MCP transport hardening, including Origin validation and stronger browser-facing transport controls.

## Future

- Cassandra telemetry adapter.
- Dashboard.
- Production MCP transport.
- SmartSentinel runtime and agent integration.
- Aion Edge Adapter runtime.

## Notes

- The roadmap is intentionally concise. Canonical details live in the individual model docs and ADRs.
- Security hardening remains staged after the current selected write-surface rollout to avoid overreaching beyond verified behavior in one milestone.
- `aion-api` modularization is intentionally incremental; Milestones 61 through 77 establish safe extraction patterns for later route-level and shared-surface splits.
