# ADR 0001: JSON-LD Domain Entities

## Status

Accepted

## Context

AionCore needs a domain model that can represent IoT devices, sensors, physical spaces, assets, and logical systems in a way that remains interoperable with external semantic systems.

Plain relational records are easy to query but do not carry enough semantic context. AionCore also needs AI-facing tools that can expose meaningful context without relying on undocumented conventions.

## Decision

AionCore will represent domain entities as JSON-LD documents.

Each entity will also have an internal UUID and a tenant-scoped `entity_key` for operational lookup.

MVP 1 will validate only a minimal JSON-LD shape:

- The entity document is a JSON object.
- `@context` is present.
- `@type` is present.
- Either `@id` or `entity_key` is present.

Full JSON-LD expansion, remote context fetching, ontology validation, and reasoning are postponed.

## Consequences

Positive:

- Domain entities can carry semantic meaning from the beginning.
- MCP tools can return context-rich JSON-LD records.
- The model remains open to interoperability with external vocabularies.

Negative:

- JSON-LD validation can become complex if expanded too early.
- Querying arbitrary JSON-LD requires careful indexing and API design.

## MVP Simplification

Store complete JSON-LD in PostgreSQL `jsonb`, extract practical fields such as `entity_key` and `entity_type`, and keep semantic validation intentionally minimal.
