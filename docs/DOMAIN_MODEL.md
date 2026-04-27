# Domain Model

AionCore domain context is represented with JSON-LD entities and explicit relationships. The domain model gives raw telemetry semantic meaning by connecting measurements to known entities such as devices, rooms, assets, sensors, equipment, sites, and logical systems.

## Entity Registry

An entity is a tenant-scoped JSON-LD document with a stable application key.

Minimum fields:

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

## Entity Key

`entity_key` is the practical lookup key used by ingestion and APIs. It should be unique per tenant.

Examples:

- `weather-station-01`
- `room-101`
- `pump-a7`
- `sensor-temp-001`

The JSON-LD `@id` remains the semantic identifier. `entity_key` exists to make operational lookup predictable.

## Minimal JSON-LD Validation

MVP 1 should validate only the minimum shape:

- JSON document must be an object.
- `@context` must exist.
- `@type` must exist.
- Either `@id` or `entity_key` must exist.

Full JSON-LD expansion, remote context fetching, ontology validation, and reasoning should be postponed.

## Relationship Registry

Relationships connect two registered entities with a typed edge.

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

Recommended stored fields:

- `id`: internal UUID.
- `tenant_id`: tenant UUID.
- `source_entity_id`.
- `relationship_type`.
- `target_entity_id`.
- `jsonld`: complete JSON-LD relationship document.
- `created_at`.

## Common Relationship Types

Initial relationship types can be plain strings with JSON-LD-compatible names:

- `aion:locatedIn`
- `aion:observes`
- `aion:controls`
- `aion:partOf`
- `aion:connectedTo`
- `aion:installedOn`

MVP 1 should not enforce a closed relationship vocabulary.

## Entity Resolution During Ingestion

Decoders should produce an `entity_key` or equivalent reference. The normalizer resolves that key to a registered entity within the tenant.

If an entity cannot be resolved:

- The raw message remains stored.
- Normalization should fail for that message.
- The failure reason should be recorded.

Automatic entity creation should be postponed unless explicitly enabled later.
