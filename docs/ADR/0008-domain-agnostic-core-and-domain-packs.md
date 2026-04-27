# ADR 0008: Domain-Agnostic Core and Domain Packs

## Status

Accepted

## Context

AionCore started with IoT and smart agriculture as motivating examples. The platform, however, is intended to support many domains that need semantic context, payload-agnostic ingestion, canonical observations, and safe closed-loop decision support.

If agriculture-specific or any other domain-specific assumptions enter the core schema and runtime, AionCore becomes harder to reuse for smart buildings, smart cities, infrastructure monitoring, industrial operations, energy, logistics, and future domains.

## Decision

AionCore Core will remain domain-agnostic.

The core model will define reusable primitives:

- Entity.
- Relationship.
- Observation.
- Event.
- Command.
- Action.
- ActionResult.
- Capability.
- Policy.
- PayloadProfile.
- Decoder.

Domain-specific vocabulary, examples, mappings, payload profiles, and policies should live in optional domain packs or deployment-specific configuration.

Agriculture is a reference use case, not a core dependency.

## Consequences

Positive:

- The same core can serve multiple operational domains.
- Domain examples can evolve independently from platform migrations.
- Integrations can map their own concepts into common semantic primitives.
- AI/MCP tools can query consistent concepts across domains.

Negative:

- Domain packs need clear conventions and documentation.
- Core APIs must avoid hard-coded assumptions that may feel convenient for a single domain.
- Some domain-specific validation must happen outside the core MVP.

## MVP Simplification

Use generic core tables and models first. Provide practical reference examples for agriculture, buildings, cities, and operational monitoring in documentation. Add formal domain-pack loading later.
