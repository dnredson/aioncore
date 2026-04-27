# AI and MCP Model

AionCore should be ready for AI and LLM integration through MCP-compatible tools and resources. The MVP focuses on read-only semantic context and observation access.

## Goals

- Let AI clients inspect domain entities and relationships.
- Let AI clients query observations by entity, property, and time range.
- Provide semantic context grounded in JSON-LD.
- Avoid direct execution of critical actions by default.

## Non-Goals for MVP 1

- No autonomous control actions.
- No complex rule engine.
- No dashboard.
- No LLM-generated database writes.
- No direct device actuation through MCP.

## MCP Tools

Initial read-only tools:

```text
query_entities
query_relationships
query_observations
```

### query_entities

Inputs:

- Tenant.
- Optional entity type.
- Optional entity key.
- Optional text filter.

Returns:

- Matching entity metadata.
- JSON-LD document.

### query_relationships

Inputs:

- Tenant.
- Optional source entity.
- Optional target entity.
- Optional relationship type.

Returns:

- Relationship records.
- Source and target entity references.
- JSON-LD relationship document.

### query_observations

Inputs:

- Tenant.
- Entity key or entity ID.
- Optional observed property.
- Time range.
- Limit.

Returns:

- Canonical observations.
- Units.
- Observation timestamps.
- Optional source raw message reference.

## MCP Resources

Potential resources:

```text
aion://tenants/{tenant}/entities/{entity_key}
aion://tenants/{tenant}/entities/{entity_key}/relationships
aion://tenants/{tenant}/entities/{entity_key}/observations
```

MVP 1 can expose tools first and add resource URIs later.

## Safety Model

MCP integration is read-only by default.

Critical actions include:

- Device actuation.
- Rule changes.
- Credential changes.
- Entity deletion.
- Bulk data deletion.

Those actions require explicit future design with authorization, auditing, and human approval where appropriate.

## Context Grounding

AI responses should be grounded in stored AionCore data:

- JSON-LD entities.
- Entity relationships.
- Canonical observations.
- Raw message references where needed for audit.

The MCP layer should not invent domain facts. It should query the platform and return structured results.

## Future Extensions

Future MCP capabilities may include:

- Summarizing recent observations.
- Detecting missing telemetry.
- Suggesting entity relationship improvements.
- Recommending decoder mappings.
- Drafting control plans without executing them.

Execution of control plans should remain outside the default MCP read-only surface.
