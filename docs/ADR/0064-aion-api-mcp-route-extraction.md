# ADR 0064: aion-api MCP route extraction

## Status

Accepted

## Context

Milestones 61 through 68 established the incremental `aion-api` modularization pattern by first extracting auth and shared error code, then moving bounded route groups such as edge adapter, auth/token, executor, generic command, and SmartSentinel surfaces out of `apps/aion-api/src/lib.rs`.

After those extractions, the remaining MCP-style HTTP surface in `lib.rs` was still a cohesive, low-risk group:

- `GET /mcp/tools`
- `POST /mcp/tools/{tool_name}`
- `POST /mcp`

These handlers already shared dedicated DTOs, JSON-RPC compatibility shaping, and local MCP tool dispatch. They were also distinct from the larger remaining entity, ingestion, and provenance route groups.

## Decision

Extract the MCP route registration and MCP-specific handlers from `apps/aion-api/src/lib.rs` into `apps/aion-api/src/routes/mcp.rs`.

Move with them:

- MCP route registration
- local tool request/response wrappers
- minimal JSON-RPC request/response shaping for `tools/list` and `tools/call`
- MCP-specific error shaping
- MCP-local tool dispatch
- MCP-only argument DTOs and parsing helpers

Intentionally keep shared AI context assembly in `lib.rs`:

- `AiContextQuery`
- `build_ai_entity_context`
- existing `/ai/context/entity/:entity_id` route

That logic is still shared by both the dedicated AI context endpoint and the MCP `build_ai_context` tool. Keeping it outside the MCP module avoids coupling the general AI context path to an MCP-specific module and reduces extraction risk.

## Consequences

Positive:

- `lib.rs` continues shrinking through bounded route-level extraction instead of broad refactoring.
- MCP HTTP behavior remains unchanged while registration and handlers become easier to find.
- Shared AI context logic stays centralized for both `/ai/context/*` and MCP tool usage.

Neutral / intentional:

- This milestone does not introduce production MCP transport.
- This milestone does not add SSE/streaming support.
- This milestone does not add browser Origin validation.
- This milestone does not change auth semantics, endpoint paths, JSON shapes, or tool behavior.

Future work:

- production MCP transport hardening
- stronger browser-facing controls such as Origin validation
- later extraction of other cohesive remaining route groups from `lib.rs`
