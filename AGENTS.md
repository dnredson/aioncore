# AGENTS.md

## Project name
AionCore

## Vision
AionCore is an open-source, AI-native IoT platform that combines JSON-LD domain entities, payload-agnostic ingestion, canonical observations, and MCP-ready semantic context for interoperable sensing and closed-loop decision support.

## Core architectural decisions
- Domain/context entities must be represented using JSON-LD.
- Sensor payloads are payload-agnostic at ingestion.
- All valid telemetry must be normalized into Canonical Observations.
- Raw messages must always be stored before normalization.
- The platform must support multiple payload decoders, including SenML JSON, UltraLight, JSON mapping, and future formats.
- The architecture should support both all-in-one deployment and distributed microservices.
- The platform must be ready for MCP/LLM integration, but LLMs must not directly execute critical actions by default.

## Initial modules
- context: JSON-LD entities and relationships.
- ingest: HTTP/MQTT ingestion.
- normalizer: payload decoding and canonical observation generation.
- observation: time-series storage and query API.
- mcp: AI/LLM-facing tools.
- gateway: public API.
- identity: tenants, users, device credentials.

## Engineering rules
- Start simple and incremental.
- Prefer modular monolith or all-in-one mode for the first MVP, but keep service boundaries clear.
- Do not implement dashboard in the first task.
- Do not implement complex rule engine in the first task.
- Do not introduce paid cloud dependencies.
- Do not store secrets in the repository.
- Do not delete files without explicit approval.
- Every major decision should be documented in docs/ADR.
- Every feature must include basic tests where feasible.
- At the end of each task, show the diff, explain what changed, and list validation commands.

## Suggested stack
- Backend: Rust with Axum.
- Database: PostgreSQL with TimescaleDB.
- Messaging/event bus: NATS.
- MQTT broker: Mosquitto or EMQX.
- Deployment: Docker Compose first.