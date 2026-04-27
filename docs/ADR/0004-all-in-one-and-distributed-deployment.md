# ADR 0004: All-In-One and Distributed Deployment

## Status

Accepted

## Context

AionCore should be easy to run for local development and early adopters. At the same time, ingestion, normalization, observation storage, public APIs, and MCP serving may need to scale independently later.

## Decision

AionCore will start as an all-in-one modular monolith.

MVP 1 should run as one Rust API process backed by PostgreSQL and TimescaleDB. Internal module boundaries should be explicit so the platform can later split into distributed services.

Future distributed deployment may separate:

- HTTP ingestion.
- MQTT ingestion.
- Normalization workers.
- Observation API.
- MCP server.
- Gateway.

## Consequences

Positive:

- MVP 1 remains simple to build, run, and test.
- Docker Compose local development is straightforward.
- Service boundaries are preserved for later scaling.

Negative:

- The first process may contain multiple responsibilities.
- Care is required to avoid tight coupling between modules.

## MVP Simplification

Use direct in-process calls between modules. Introduce NATS events only when asynchronous processing or service separation becomes necessary.
