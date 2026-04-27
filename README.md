# AionCore

AionCore is an open-source, AI-native IoT platform for interoperable sensing and closed-loop decision support.

The platform combines JSON-LD domain entities, payload-agnostic ingestion, canonical observations, and MCP-ready semantic context. Its first MVP focuses on a small but durable foundation: register domain entities, ingest telemetry from multiple payload formats, store every raw message before normalization, and expose normalized observations for applications and AI-facing tools.

## MVP Scope

The initial MVP includes:

- JSON-LD domain entity registry.
- Entity relationship registry.
- Raw message storage before normalization.
- Canonical observation model.
- Payload decoder interface.
- Initial decoder designs for SenML JSON, UltraLight, and JSON mapping.
- HTTP ingestion.
- MQTT ingestion design, with implementation allowed after HTTP ingestion.
- PostgreSQL and TimescaleDB persistence design.
- Docker Compose local development design.
- Minimal read-only MCP design for querying entities and observations.

The first MVP does not include:

- A dashboard.
- A complex rule engine.
- Paid cloud dependencies.
- LLM-controlled critical actions by default.

## Architecture Direction

AionCore starts as a modular monolith implemented in Rust, with service boundaries kept explicit from the beginning. The first deployable should be an all-in-one API process that contains context, ingest, normalizer, observation, MCP, gateway, and identity modules behind clear internal interfaces.

The architecture should later support distributed deployment, where HTTP ingestion, MQTT ingestion, normalization, observation storage, public API, and MCP serving can become separate services.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Domain Model](docs/DOMAIN_MODEL.md)
- [Observation Model](docs/OBSERVATION_MODEL.md)
- [Ingestion Model](docs/INGESTION_MODEL.md)
- [AI and MCP Model](docs/AI_MCP_MODEL.md)
- [Architecture Decision Records](docs/ADR)

## Key Principles

- Domain/context entities are represented using JSON-LD.
- Sensor payloads are payload-agnostic at ingestion.
- All valid telemetry is normalized into canonical observations.
- Raw messages are always stored before normalization.
- Multiple payload decoders are supported.
- MCP/LLM integration is read-oriented by default.
- Critical actions require explicit non-default control paths.

## Suggested Technology Stack

- Backend: Rust with Axum.
- Database: PostgreSQL with TimescaleDB.
- Messaging/event bus: NATS.
- MQTT broker: Mosquitto or EMQX.
- Deployment: Docker Compose first.

## Project Status

AionCore is currently in documentation and MVP planning. Backend and frontend code have not been implemented yet.
