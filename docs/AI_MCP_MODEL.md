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

The local in-memory runtime also exposes a minimal MCP-ready HTTP tool layer for development and integration testing:

```text
GET  /mcp/tools
POST /mcp/tools/{tool_name}
POST /mcp
```

Initial local tool names:

```text
list_entities
get_entity
get_entity_context
get_recent_observations
get_events
get_pending_commands
build_ai_context
```

`/mcp/tools` and `/mcp/tools/{tool_name}` are the development HTTP API for listing and invoking the local tool abstraction directly.

`/mcp` is a minimal JSON-RPC compatibility endpoint for local testing of the core MCP `tools/list` and `tools/call` flow. It maps the same local tool definitions into MCP-like `name`, `description`, and `inputSchema` fields, and maps successful tool calls into MCP-like `content`, `structuredContent`, and `isError` fields.

This is not a standalone production MCP server yet. It is an internal MCP-style tool abstraction using structured tool definitions, requests, responses, results, and errors, plus a thin local JSON-RPC wrapper. A future MCP server can wrap the same tool layer without changing AionCore's domain model.

No real LLM is called by either `/mcp/tools` or `/mcp`. The AI context builder only assembles stored AionCore data from the in-memory runtime.

Auth behavior for the local MCP-style surfaces:

- `AIONCORE_AUTH_MODE=dev` keeps the local development bypass unchanged
- `AIONCORE_AUTH_MODE=disabled` keeps auth explicitly off
- `AIONCORE_AUTH_MODE=token` requires `mcp:tools` for:
  - `GET /mcp/tools`
  - `POST /mcp/tools/{tool_name}`
  - `POST /mcp`
- `admin:all` also satisfies those checks

The minimal `/mcp` endpoint is intended for localhost development only. Do not expose it publicly without authentication and Origin validation. In `token` mode it now requires `mcp:tools`, but production MCP transport, Origin validation, browser-facing CSRF-style controls, and SSE or streaming support are still future work.

The AI context HTTP surface is separate from the MCP transport but part of the same AI-facing model:

- `GET /ai/context/entity/{entity_id}` requires `ai:context:read` in `token` mode
- AI context can expose operational topology, relationships, recent observations, events, command context, and raw-message references
- no external AI call is made by the context builder

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
