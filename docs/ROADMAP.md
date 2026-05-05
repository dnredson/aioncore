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

## Next

1. Milestone 57: add production MCP transport hardening, including Origin validation and stronger browser-facing transport controls.
2. Milestone 58: start tenant/resource ownership enforcement for token-authenticated reads and writes.
3. Milestone 59: review whether remaining open write surfaces should split into narrower operator and machine scopes.

## Future

- Cassandra telemetry adapter.
- Dashboard.
- Production MCP transport.
- SmartSentinel runtime and agent integration.
- Aion Edge Adapter runtime.

## Notes

- The roadmap is intentionally concise. Canonical details live in the individual model docs and ADRs.
- Security hardening is staged after the documentation-first model to avoid changing runtime behavior prematurely.
