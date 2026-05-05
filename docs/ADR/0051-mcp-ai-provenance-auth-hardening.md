# ADR 0051: MCP, AI Context, and Provenance Auth Hardening

## Status

Accepted

## Context

Milestones 48 through 53 introduced auth middleware, API tokens, selective token-mode route protection, and clearer readiness diagnostics.

That rollout still left several sensitive read-oriented surfaces open in `token` mode:

- `GET /mcp/tools`
- `POST /mcp/tools/{tool_name}`
- `POST /mcp`
- `GET /ai/context/entity/{entity_id}`
- `GET /provenance/search`

Those endpoints do not mutate credentials or execute external AI calls, but they can expose semantic topology, operational state, pending commands, events, raw-message references, incidents, alerts, traces, and other provenance-linked context. Leaving them open in `token` mode would undercut the staged production hardening model.

At the same time, broad protection of `/events` and `/raw-messages` would create larger test churn and scope expansion than this milestone needs.

## Decision

In `token` mode:

- require `mcp:tools` for:
  - `GET /mcp/tools`
  - `POST /mcp/tools/{tool_name}`
  - `POST /mcp`
- require `ai:context:read` for:
  - `GET /ai/context/entity/{entity_id}`
- require `provenance:read` for:
  - `GET /provenance/search`
- keep `admin:all` as a universal scope satisfier
- keep `dev` and `disabled` behavior unchanged

For readiness reporting:

- continue reporting `enforcement_level = partial` in `token` mode
- extend `auth.protected_endpoint_groups` with:
  - `mcp_tools`
  - `ai_context`
  - `provenance_search`

For production guidance:

- treat `/mcp` and `/mcp/tools` as local-development-friendly surfaces by default
- require authentication in `token` mode
- document that production MCP transport, Origin validation, and stronger browser-facing transport hardening remain future work

For event and raw-message surfaces:

- do not broadly protect `/events*` or `/raw-messages*` in this milestone
- explicitly document them as remaining open surfaces for later hardening

## Consequences

Positive:

- token mode now covers the main MCP-style and AI/provenance aggregation surfaces
- production guidance is more honest about what these endpoints expose
- readiness diagnostics stay aligned with actual route protection
- development and disabled behavior remain unchanged

Tradeoffs:

- token-mode protection remains partial rather than broad
- `/events*` and `/raw-messages*` still need a later milestone
- current `/mcp` remains a compatibility wrapper, not a production MCP transport

## Alternatives Considered

Protect the full read API now:

- rejected because it would widen milestone scope, increase test churn, and mix route-hardening with broader resource-ownership design

Leave MCP, AI context, and provenance open until full-API auth:

- rejected because these surfaces aggregate high-value operational context and should not remain open once `token` mode exists

Protect `/events*` and `/raw-messages*` in the same change:

- rejected for now because it risks larger compatibility impact and deserves a dedicated scope and rollout decision
