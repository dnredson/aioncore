# ADR 0065: aion-api AI context extraction

## Status

Accepted

## Context

Milestones 61 through 69 established the staged `aion-api` modularization pattern by extracting shared auth and error foundations first, then moving bounded route groups such as edge adapter, auth/token, executor, generic command, SmartSentinel, and MCP surfaces out of `apps/aion-api/src/lib.rs`.

After Milestone 69, `lib.rs` still contained one cohesive AI-facing surface that remained shared across both a dedicated HTTP endpoint and MCP:

- `GET /ai/context/entity/{entity_id}`
- `AiContextQuery`
- `build_ai_entity_context`
- AI-context-local event aggregation and metadata shaping helpers

Milestone 69 intentionally left that logic in `lib.rs` because the MCP extraction was lower risk if the shared builder stayed centralized first. Once the MCP route module was established, the shared AI context surface became the next bounded extraction target.

## Decision

Extract shared AI context assembly from `apps/aion-api/src/lib.rs` into `apps/aion-api/src/ai_context.rs` and move the dedicated AI context HTTP route into `apps/aion-api/src/routes/ai.rs`.

Move into `ai_context.rs`:

- `AiContextQuery`
- `AiEntityContextResponse`
- `build_ai_entity_context`
- the entity-local event aggregation helper used only by AI context assembly

Move into `routes/ai.rs`:

- route registration for `GET /ai/context/entity/{entity_id}`
- the HTTP handler that applies the existing `ai:context:read` scope check and delegates to the shared builder

Update `routes/mcp.rs` to call the extracted builder and query DTO from `ai_context.rs` instead of depending on `lib.rs`.

## Consequences

Positive:

- `lib.rs` continues shrinking through narrow, behavior-preserving extraction.
- The shared AI context builder now has a dedicated module that can be reused by both `/ai/context/*` and MCP without coupling it to the MCP route module.
- MCP compatibility remains unchanged because the tool still invokes the same builder with the same query defaults, data sources, and response shape.

Neutral / intentional:

- Tests remain in `lib.rs` for now to minimize churn during staged modularization.
- No endpoint paths, auth semantics, tenant/resource ownership behavior, JSON shapes, or AI context content changed.
- No external AI or LLM calls were introduced; `llm_invoked` remains `false` and AI context remains a local aggregation surface only.

What intentionally remained in `lib.rs`:

- unrelated entity context, provenance, ingestion, and storage-facing handlers
- centralized tests, including AI context and MCP coverage

Future work:

- continue incremental extraction of other cohesive remaining route or shared-query surfaces
- production MCP transport hardening and browser-facing controls
