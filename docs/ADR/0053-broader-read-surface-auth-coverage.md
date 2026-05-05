# ADR 0053: Broader Read-Surface Auth Coverage

## Status

Accepted

## Context

Milestones 48 through 55 introduced token-mode auth plumbing plus selective protection for machine-facing endpoints, connector administration, MCP and AI context, provenance search, events, and raw messages.

That still left several read-oriented API surfaces open in `AIONCORE_AUTH_MODE=token`, including:

- `GET /entities`
- `GET /entities/{entity_id}`
- `GET /entities/{entity_id}/context`
- `GET /observations`
- `GET /commands`
- `GET /commands/{command_id}`
- `GET /actions`
- `GET /actions/{action_id}`
- `GET /action-results`
- `GET /rules`
- `GET /rules/{rule_id}`
- `GET /policies`
- `GET /entities/{entity_id}/capabilities`
- `GET /executors`
- `GET /executors/{executor_id}`
- `GET /executors/{executor_id}/capabilities`
- `GET /executors/{executor_id}/scopes`

These routes expose semantic topology, canonical telemetry, control history, automation rules, policy state, and executor inventory. Leaving them open under token mode weakens the staged hardening model.

At the same time, this milestone should not:

- blindly protect the entire API
- add tenant or resource ownership enforcement
- change default `dev` behavior
- change existing machine scopes introduced earlier

Standalone relationship read routes and observation detail routes do not currently exist, so this milestone should not invent them.

## Decision

In `token` mode:

- require `entities:read` for:
  - `GET /entities`
  - `GET /entities/{entity_id}`
  - `GET /entities/{entity_id}/context`
- require `observations:read` for:
  - `GET /observations`
- require `commands:read` for:
  - `GET /commands`
  - `GET /commands/{command_id}`
- require `actions:read` for:
  - `GET /actions`
  - `GET /actions/{action_id}`
  - `GET /action-results`
- require `rules:read` for:
  - `GET /rules`
  - `GET /rules/{rule_id}`
- require `policies:read` for:
  - `GET /policies`
- require `capabilities:read` for:
  - `GET /entities/{entity_id}/capabilities`
- require `executors:read` for:
  - `GET /executors`
  - `GET /executors/{executor_id}`
  - `GET /executors/{executor_id}/capabilities`
  - `GET /executors/{executor_id}/scopes`
- keep `admin:all` as a universal scope satisfier
- keep `dev` and `disabled` behavior unchanged
- keep broader tenant/resource ownership enforcement deferred

For readiness reporting:

- continue reporting `enforcement_level = partial` in `token` mode
- extend `auth.protected_endpoint_groups` with:
  - `entities`
  - `observations`
  - `commands`
  - `actions`
  - `rules`
  - `policies`
  - `capabilities`
  - `executors_read`

## Consequences

Positive:

- token mode now protects the main remaining generic read surfaces without widening to blanket API enforcement
- scope boundaries are explicit and readable in handlers and diagnostics
- development and disabled behavior stay unchanged

Tradeoffs:

- write surfaces for these domains are still not broadly scope-gated
- token-mode protection is still partial rather than ownership-aware
- standalone relationship reads remain future work because the routes do not exist yet
- observation detail reads remain future work because a dedicated detail route does not exist yet

## Alternatives Considered

Protect the entire API in token mode:

- rejected because it would break the staged rollout and force premature ownership decisions

Reuse existing scopes such as `commands:read` for actions or `entities:read` for executor inspection:

- rejected because dedicated read scopes better match the surface areas being exposed now

Add tenant or resource ownership checks in the same milestone:

- rejected because ownership enforcement is a larger behavioral change and should land separately
