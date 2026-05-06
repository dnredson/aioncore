# AionCore

AionCore is an open-source, AI-native IoT platform for interoperable sensing and closed-loop decision support.

It combines JSON-LD domain entities, payload-agnostic ingestion, canonical observations, raw-message preservation, and MCP-ready semantic context. The current runtime is aimed at local development and architecture validation rather than production deployment.

## MVP Scope

The current MVP direction focuses on:

- JSON-LD domain entities and relationships.
- Payload-agnostic ingestion with raw-message preservation first.
- Canonical observation normalization.
- HTTP ingestion plus optional MQTT ingestion foundations.
- Read-oriented MCP and AI context surfaces.
- Pluggable storage, with in-memory behavior as the reference path and PostgreSQL/TimescaleDB as the first durable backend direction.
- Read-only dashboard API foundations for future operational and time-series exploration views.

Out of scope for the current MVP:

- dashboard UI
- complex rule engine
- paid cloud dependencies
- direct LLM control of critical actions by default

## Architecture Direction

AionCore starts as a modular monolith in Rust with clear internal boundaries across context, ingest, normalizer, observation, gateway, identity, and MCP modules. The first deployable remains an all-in-one API process, while the design keeps room for later service separation around ingestion, normalization, storage, API, and MCP layers.

## Quick Local Start

The default local runtime uses in-memory storage and does not require Docker, PostgreSQL, TimescaleDB, NATS, or Mosquitto.

```powershell
cargo run -p aion-api
```

Useful local checks:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/health"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/ready"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/auth/whoami"
```

PostgreSQL mode is available when you explicitly set:

- `AIONCORE_STORAGE_BACKEND=postgres`
- `AIONCORE_DATABASE_URL=postgres://user:password@localhost:5432/aioncore`

Detailed runtime validation and PostgreSQL testing notes live in [Runtime Validation](docs/RUNTIME_VALIDATION.md).

## Current Status

- The local API runtime is functional with in-memory storage.
- SQL migrations and PostgreSQL/TimescaleDB persistence foundations exist, but in-memory remains the reference development behavior.
- Authentication and authorization are still incremental.
- Connector, TTN, SmartSentinel, MCP, and command/executor flows have usage examples in focused guides under `docs/`.
- A read-only dashboard API foundation exists for future UI work, but no frontend dashboard is implemented yet.

## Authentication Warning

`AIONCORE_AUTH_MODE=dev` is still the default when unset. That is acceptable for trusted local development only.

Current token mode enforcement is intentionally partial:

- `token` mode protects selected machine-facing, MCP/AI, secret-management, connector, and read-oriented routes.
- Tenant/resource ownership checks exist only for selected protected read surfaces.
- Writes and full ownership enforcement remain future work.
- The platform is not production-ready and should not be exposed publicly in its current state.

For the detailed auth model, scopes, bootstrap flow, and token examples, use [Authentication Usage](docs/AUTH_USAGE.md) and [Security Model](docs/SECURITY_MODEL.md).

## Documentation

Core architecture and models:

- [Architecture](docs/ARCHITECTURE.md)
- [Domain Model](docs/DOMAIN_MODEL.md)
- [Observation Model](docs/OBSERVATION_MODEL.md)
- [Ingestion Model](docs/INGESTION_MODEL.md)
- [Persistence Model](docs/PERSISTENCE_MODEL.md)
- [AI and MCP Model](docs/AI_MCP_MODEL.md)
- [Action Model](docs/ACTION_MODEL.md)
- [Security Model](docs/SECURITY_MODEL.md)
- [Dashboard Model](docs/DASHBOARD_MODEL.md)
- [Aion Edge Adapter Model](docs/EDGE_ADAPTER_MODEL.md)
- [SmartSentinel Integration Model](docs/SMARTSENTINEL_INTEGRATION.md)

Operational usage guides:

- [Authentication Usage](docs/AUTH_USAGE.md)
- [Ingestion Usage](docs/INGESTION_USAGE.md)
- [Time-Series Usage](docs/TIMESERIES_USAGE.md)
- [Dashboard Usage](docs/DASHBOARD_USAGE.md)
- [TTN Usage](docs/TTN_USAGE.md)
- [SmartSentinel Usage](docs/SMARTSENTINEL_USAGE.md)
- [MCP Usage](docs/MCP_USAGE.md)
- [Commands, Rules, and Executors Usage](docs/COMMANDS_RULES_EXECUTORS_USAGE.md)
- [Runtime Validation](docs/RUNTIME_VALIDATION.md)

Project planning:

- [Roadmap](docs/ROADMAP.md)
- [Documentation Index](docs/INDEX.md)
- [Architecture Decision Records](docs/ADR)

## Important Notes

- Raw messages are always stored before normalization.
- In-memory storage remains the reference behavior for tests and local development.
- MQTT and connector workers are opt-in and disabled unless explicitly enabled.
- Edge Adapter remains an optional future deployment path and is not required for current ingestion flows.
