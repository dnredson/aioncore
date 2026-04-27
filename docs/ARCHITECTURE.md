# Architecture

AionCore is an AI-native IoT platform built around semantic domain context, payload-agnostic ingestion, canonical observations, and safe AI-facing query surfaces.

## Goals

- Represent domain entities and relationships using JSON-LD.
- Accept telemetry without coupling ingestion to one sensor payload format.
- Store every raw message before normalization.
- Normalize valid telemetry into canonical observations.
- Support HTTP ingestion first and MQTT ingestion next.
- Use PostgreSQL and TimescaleDB for durable local-first persistence.
- Expose read-oriented MCP tools for AI and LLM clients.
- Start as an all-in-one modular monolith while preserving service boundaries.

## Initial Modules

### Context

Owns JSON-LD domain entities and relationships.

Responsibilities:

- Register entities.
- Validate minimal JSON-LD shape.
- Store tenant-scoped entity keys.
- Register relationships between entities.
- Provide entity and graph lookup APIs.

### Ingest

Owns inbound telemetry entry points and raw message capture.

Responsibilities:

- Accept HTTP telemetry.
- Design MQTT topic and message handling.
- Persist raw messages before any decoding.
- Attach source metadata such as tenant, device, decoder hint, content type, and headers.

### Normalizer

Owns payload decoding and canonical observation generation.

Responsibilities:

- Define the payload decoder interface.
- Implement SenML JSON, UltraLight, and JSON mapping decoders.
- Convert decoded measurements into canonical observations.
- Report normalization failures without losing raw payloads.

### Observation

Owns canonical observation persistence and query behavior.

Responsibilities:

- Store normalized time-series observations.
- Query observations by entity, observed property, and time range.
- Preserve links from observations to source raw messages where available.

### MCP

Owns AI-facing tools and resources.

Responsibilities:

- Expose read-only tools for querying entities, relationships, and observations.
- Keep critical action execution outside default AI paths.
- Reuse domain and observation query services rather than duplicating access logic.

### Gateway

Owns public API routing and request-level concerns.

Responsibilities:

- Route REST APIs.
- Apply tenant and device authentication.
- Provide consistent error responses.

### Identity

Owns tenants, users, and device credentials.

MVP 1 should keep identity minimal. Tenants and device credentials are required; full user management can be postponed.

## Deployment Model

MVP 1 should use one Rust API process and one PostgreSQL/TimescaleDB database in Docker Compose.

Optional local services can be introduced as designs first:

- NATS for future event-driven service separation.
- Mosquitto or EMQX for MQTT ingestion.

The all-in-one process should keep internal interfaces clean enough that these modules can later become independent services.

## Data Flow

HTTP ingestion flow:

```text
Client or device
  -> HTTP ingestion endpoint
  -> authenticate tenant/device
  -> store raw message
  -> select decoder
  -> decode payload
  -> resolve entity
  -> write canonical observations
  -> update raw message normalization status
```

MQTT future flow:

```text
Device
  -> MQTT broker
  -> MQTT ingestion worker
  -> store raw message
  -> normalize directly or publish raw-message event
```

## Persistence

PostgreSQL stores:

- Tenants and device credentials.
- JSON-LD entities.
- Entity relationships.
- Raw messages.
- Decoder mappings.
- Canonical observations.

TimescaleDB should be used for canonical observation time-series storage.

## Architectural Constraints

- Do not skip raw message storage.
- Do not couple ingestion endpoints to a single decoder.
- Do not allow LLMs to execute critical actions by default.
- Do not require paid cloud infrastructure for MVP 1.
- Document major decisions in ADRs.
