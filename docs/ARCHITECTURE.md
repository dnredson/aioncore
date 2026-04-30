# Architecture

AionCore is an AI-native, domain-agnostic platform built around semantic context, payload-agnostic ingestion, canonical observations, and safe AI-facing query surfaces.

AionCore Core does not depend on agriculture, buildings, cities, infrastructure monitoring, or any other specific operational domain. Those domains are represented through JSON-LD entities, relationships, payload profiles, decoders, policies, and optional domain packs.

## Goals

- Represent domain entities and relationships using JSON-LD.
- Accept telemetry without coupling ingestion to one sensor payload format.
- Store every raw message before normalization.
- Normalize valid telemetry into canonical observations.
- Support HTTP ingestion first and MQTT ingestion next.
- Use PostgreSQL and TimescaleDB for durable local-first persistence.
- Expose read-oriented MCP tools for AI and LLM clients.
- Start as an all-in-one modular monolith while preserving service boundaries.
- Keep the core model domain-agnostic and move domain-specific vocabulary into reference examples or optional packs.

## Domain-Agnostic Core

AionCore Core provides primitives for semantic state and closed-loop decision support:

- Entities describe things, places, systems, software components, people, assets, or abstract domain objects.
- Relationships connect entities into a graph.
- Observations describe measured, detected, inferred, or reported state.
- Events describe meaningful occurrences.
- Commands describe requested intent.
- Actions describe execution attempts.
- ActionResults describe the outcome of actions.
- Capabilities describe what an entity or integration can do.
- Policies constrain when commands and actions are allowed.
- PayloadProfiles and Decoders connect external payloads to canonical platform records.

Agriculture is the first reference use case for AionCore, not part of the core model. A smart agriculture deployment may define farms, plots, valves, pumps, crops, agronomic observations, and irrigation commands as JSON-LD domain entities and relationships. A smart building deployment may define rooms, air handlers, occupancy zones, energy meters, and comfort policies using the same core primitives.

Domain packs may provide:

- JSON-LD contexts and vocabularies.
- Entity templates.
- Relationship type suggestions.
- Payload profiles and decoder mappings.
- Policy templates.
- Example commands, capabilities, and action result schemas.

Domain packs must remain optional. Core migrations, runtime services, and APIs should not require a specific pack.

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

### Action

Owns command, action, action result, capability, and policy concepts. MVP runtime support can be postponed, but the architecture reserves these concepts so closed-loop workflows do not become ad hoc API calls.

Responsibilities:

- Represent requested intent as commands.
- Represent execution attempts as actions.
- Record action outcomes as action results.
- Bind executable behavior to explicit capabilities.
- Apply policies before critical commands or actions.

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

Closed-loop semantic flow:

```text
Observe
  -> Contextualize
  -> Decide
  -> Command
  -> Act
  -> Verify
```

See [Action Model](ACTION_MODEL.md) for the detailed action vocabulary and safety boundaries.

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

Future persistence areas:

- Events.
- Capabilities.
- Policies.
- Commands.
- Actions.
- Action results.

## Reference Use Cases

### Smart Agriculture

Reference examples may include farms, plots, greenhouses, crops, soil sensors, weather stations, pumps, tanks, valves, and irrigation zones. Observations can represent soil moisture, temperature, pump state, water level, rainfall, evapotranspiration estimates, and agronomic alerts.

Agriculture-specific terms should live in JSON-LD contexts, domain packs, examples, and deployments rather than in core platform assumptions.

### Smart Building

Examples may include buildings, floors, rooms, HVAC equipment, meters, occupancy zones, access points, and indoor environmental sensors. Observations can represent temperature, humidity, occupancy, CO2, power demand, equipment state, and comfort indicators.

### Smart City

Examples may include streets, intersections, lighting circuits, public assets, waste containers, air-quality stations, parking zones, and flood-prone areas. Observations can represent traffic flow, air quality, noise, fill levels, lighting status, water level, and public infrastructure events.

### Operational and Infrastructure Monitoring

Examples may include services, hosts, containers, jobs, APIs, databases, queues, network devices, backups, and SLOs. Observations can represent health status, deployment state, incident signals, latency summaries, error rates, saturation summaries, and operational events.

High-frequency metrics can stay in specialized metric backends. AionCore stores semantic state, summarized observations, events, decisions, commands, action results, and references to external evidence.

## Optional Integrations

Aion Edge Adapter is a future optional edge/fog component for multiprotocol collection, local parsing, offline buffering, DLQ handling, and publishing into AionCore. It is not required by the AionCore runtime, and server-side ingestion connectors remain valid without it.

See [Aion Edge Adapter Model](EDGE_ADAPTER_MODEL.md).

SmartSentinel can integrate with AionCore as an optional observer and executor. It is not a core dependency. AionCore can ingest SmartSentinel snapshots as raw messages and materialize selected elements as semantic entities, relationships, observations, events, commands, actions, and action results.

See [SmartSentinel Integration](SMARTSENTINEL_INTEGRATION.md).

## Architectural Constraints

- Do not skip raw message storage.
- Do not couple ingestion endpoints to a single decoder.
- Do not allow LLMs to execute critical actions by default.
- Do not require paid cloud infrastructure for MVP 1.
- Do not make domain-specific reference examples mandatory dependencies.
- Document major decisions in ADRs.
