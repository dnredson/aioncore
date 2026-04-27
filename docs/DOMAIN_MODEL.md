# Domain Model

AionCore Core is domain-agnostic. It provides semantic primitives that can model smart agriculture, smart buildings, smart cities, operational monitoring, industrial systems, energy systems, logistics, or future domains without changing the core schema.

Agriculture is the first reference use case. It is not part of the core model. Agriculture-specific concepts such as farms, plots, crops, irrigation zones, pumps, and valves should be represented through JSON-LD domain packs, examples, payload profiles, and deployment data.

## Core Concepts

### Entity

An Entity is any thing, place, system, actor, software component, asset, or abstract object that can participate in context, observations, commands, or actions.

Examples:

- Smart agriculture: farm, greenhouse, crop plot, soil sensor, pump, valve.
- Smart building: building, floor, room, HVAC unit, occupancy zone, meter.
- Smart city: street segment, intersection, light pole, waste container, air-quality station.
- Operational monitoring: service, host, container, database, queue, API endpoint, backup job.

An entity is stored as a tenant-scoped JSON-LD document with a stable application key.

Minimum JSON-LD shape:

```json
{
  "@context": {
    "aion": "https://aioncore.org/ns#",
    "schema": "https://schema.org/"
  },
  "@id": "urn:aion:device:weather-station-01",
  "@type": "aion:Device",
  "name": "Weather Station 01"
}
```

Recommended stored fields:

- `id`: internal UUID.
- `tenant_id`: tenant UUID.
- `entity_key`: stable tenant-scoped key used by ingestion and APIs.
- `entity_type`: extracted or declared type for indexing.
- `jsonld`: complete JSON-LD document.
- `created_at`.
- `updated_at`.

`entity_key` is the practical lookup key used by ingestion and APIs. It should be unique per tenant. The JSON-LD `@id` remains the semantic identifier.

### Relationship

A Relationship is a typed edge between two entities. Relationships create the semantic graph used for context lookup, observation interpretation, command targeting, and verification.

Example:

```json
{
  "@context": {
    "aion": "https://aioncore.org/ns#"
  },
  "@type": "aion:Relationship",
  "aion:relationshipType": "aion:locatedIn",
  "aion:source": "urn:aion:device:weather-station-01",
  "aion:target": "urn:aion:room:room-101"
}
```

Common relationship types:

- `aion:locatedIn`
- `aion:observes`
- `aion:controls`
- `aion:partOf`
- `aion:connectedTo`
- `aion:installedOn`
- `aion:dependsOn`
- `aion:protects`
- `aion:reportsTo`

MVP 1 should not enforce a closed relationship vocabulary.

### Observation

An Observation is a canonical statement about state at a point in time. It may come from a sensor, service snapshot, external monitoring system, human input, imported dataset, inference process, or decoder.

Canonical observations should identify:

- Producer entity: the entity that produced or reported the observation.
- Feature of interest: the entity the observation is about.
- Observed property.
- Typed value.
- Unit, when applicable.
- Observation timestamp.
- Received timestamp.
- Protocol and payload format.
- Raw message reference, when available.

Examples:

- Smart agriculture: soil moisture for plot A is 18%.
- Smart building: room 101 CO2 is 850 ppm.
- Smart city: waste container 42 fill level is 91%.
- Operational monitoring: API service health is degraded.

### Event

An Event is a meaningful occurrence detected, reported, inferred, or imported by AionCore.

Events differ from observations because they describe something that happened rather than a sampled property value.

Examples:

- Pump entered fault state.
- Occupancy threshold exceeded.
- Flood warning issued.
- Deployment failed.
- Backup completed.
- Alert opened by an external monitoring system.

Events should include event type, subject entity, occurred timestamp, severity or priority, source, raw message or external reference, and structured metadata.

### Command

A Command is requested intent. It says what should happen, not whether it happened.

Commands are the boundary between decision support and execution. They must be explicit, auditable, and policy-governed before they can lead to actions.

Examples:

- Open valve V1 for 10 minutes.
- Reduce HVAC setpoint in zone A.
- Dim street lights on circuit C.
- Restart service S.
- Create incident ticket for degraded service.

Commands should include target entity, requested capability, parameters, requester, decision or policy reference, status, and expiration or schedule when applicable.

### Action

An Action is an execution attempt derived from a command or operational workflow. It records that AionCore or an external executor attempted to do something.

Examples:

- Sent MQTT command to valve controller.
- Called building automation API.
- Sent command to lighting management system.
- Triggered SmartSentinel remediation workflow.
- Opened ticket in an incident system.

Actions should include command reference, executor entity or integration, capability used, started timestamp, status, request payload or reference, and external correlation ID when available.

### ActionResult

An ActionResult records the outcome of an action.

Examples:

- Valve reported open.
- HVAC API accepted setpoint update.
- Lighting command timed out.
- SmartSentinel remediation succeeded.
- Restart command failed.

Action results should include action reference, success or failure status, completed timestamp, result payload or summary, error details when applicable, and verification observations or events.

### Capability

A Capability describes something an entity, service, integration, or executor can do.

Capabilities are required to avoid treating arbitrary entities as executable targets.

Examples:

- `aion:OpenCloseValve`
- `aion:SetTemperatureSetpoint`
- `aion:DimLightingCircuit`
- `aion:RestartService`
- `aion:CreateIncident`
- `aion:RunDiagnosticSnapshot`

Capabilities should define owning entity or integration, input parameters, constraints, expected result type, and safety classification.

### Policy

A Policy constrains observations, decisions, commands, and actions.

Policies are especially important for closed-loop operation because AionCore must not let AI clients directly execute critical actions by default.

Examples:

- Require human approval before restarting production services.
- Allow irrigation only within configured water budget.
- Disallow HVAC setpoints outside comfort and safety bounds.
- Permit street light dimming only during configured hours.
- Allow SmartSentinel to execute read-only diagnostics automatically but require approval for remediation.

Policies should be explicit records, not hidden code paths.

### PayloadProfile

A PayloadProfile describes how a payload source is expected to look and which decoder or mapping should process it.

Payload profiles keep ingestion payload-agnostic while still making normalization predictable.

Examples:

- SenML JSON profile for soil sensor telemetry.
- UltraLight profile for constrained devices.
- JSON mapping profile for building automation exports.
- SmartSentinel snapshot profile for operational state summaries.

Payload profiles should include expected protocol, payload format, decoder name, mapping configuration, entity resolution strategy, and version.

### Decoder

A Decoder converts raw payloads into decoded measurements, observations, events, or other canonical records.

Initial decoder types:

- SenML JSON.
- UltraLight.
- JSON mapping.

Future decoders can support SmartSentinel snapshots, cloud provider events, building automation exports, city platform feeds, or domain-specific files.

Decoders must not bypass raw message storage.

## Minimal JSON-LD Validation

MVP 1 should validate only the minimum shape:

- JSON document must be an object.
- `@context` must exist.
- `@type` must exist.
- Either `@id` or `entity_key` must exist.

Full JSON-LD expansion, remote context fetching, ontology validation, and reasoning should be postponed.

## Entity Resolution During Ingestion

Decoders should produce an `entity_key` or equivalent reference. The normalizer resolves that key to a registered entity within the tenant.

If an entity cannot be resolved:

- The raw message remains stored.
- Normalization should fail for that message.
- The failure reason should be recorded.

Automatic entity creation should be postponed unless explicitly enabled later.

## Reference Use Cases

### Smart Agriculture

Reference entities:

- Farm.
- Field.
- Plot.
- Crop.
- Irrigation zone.
- Soil sensor.
- Weather station.
- Pump.
- Valve.

Reference observations:

- Soil moisture.
- Soil temperature.
- Air temperature.
- Rainfall.
- Pump state.
- Water tank level.

Reference commands and actions:

- Open valve.
- Start pump.
- Adjust irrigation schedule.

These are reference examples only. They should not be hard-coded into core migrations or runtime modules.

### Smart Building

Reference entities:

- Building.
- Floor.
- Room.
- HVAC unit.
- Air handling unit.
- Meter.
- Occupancy sensor.

Reference observations:

- Temperature.
- Humidity.
- Occupancy.
- CO2.
- Power demand.
- Equipment status.

Reference commands and actions:

- Set temperature setpoint.
- Change ventilation mode.
- Shed noncritical load.

### Smart City

Reference entities:

- Street segment.
- Intersection.
- Light pole.
- Lighting circuit.
- Waste container.
- Parking zone.
- Flood sensor.

Reference observations:

- Traffic flow.
- Light status.
- Waste fill level.
- Air quality.
- Noise level.
- Water level.

Reference commands and actions:

- Dim lights.
- Dispatch collection.
- Raise flood warning.

### Operational and Infrastructure Monitoring

Reference entities:

- Service.
- Host.
- Container.
- Database.
- Queue.
- API endpoint.
- Backup job.
- Incident.

Reference observations:

- Health state.
- Deployment state.
- Error-rate summary.
- Latency summary.
- Saturation summary.
- Backup freshness.

Reference events:

- Alert opened.
- Deployment failed.
- Node unreachable.
- Backup completed.

Reference commands and actions:

- Run diagnostic.
- Restart service.
- Create incident.
- Trigger rollback.

High-frequency metrics should remain in specialized metric backends when needed. AionCore should store semantic state, summaries, events, decisions, commands, action results, and references.
