# ADR 0052: Events and Raw-Messages Auth Hardening

## Status

Accepted

## Context

Milestones 48 through 54 introduced auth middleware, API tokens, selected token-mode route protection, readiness diagnostics, and hardening for MCP, AI context, and provenance search.

That still left two sensitive operational read surface groups open in `AIONCORE_AUTH_MODE=token`:

- `GET /events`
- `GET /events/{event_id}`
- `GET /raw-messages`
- `GET /raw-messages/{raw_message_id}`

Those routes can expose operational history, provenance references, ingestion metadata, raw payloads, connector metadata, SmartSentinel evidence linkage, incident context, and trace information. Leaving them open would undercut the staged hardening model now that narrower read scopes are already in use elsewhere.

At the same time, this milestone should not broaden into full tenant or resource ownership enforcement, and it should not broadly protect the rest of the API.

## Decision

In `token` mode:

- require `events:read` for:
  - `GET /events`
  - `GET /events/{event_id}`
- require `raw-messages:read` for:
  - `GET /raw-messages`
  - `GET /raw-messages/{raw_message_id}`
- keep `admin:all` as a universal scope satisfier
- keep `dev` and `disabled` behavior unchanged
- keep `/provenance/search` on `provenance:read`

For readiness reporting:

- continue reporting `enforcement_level = partial` in `token` mode
- extend `auth.protected_endpoint_groups` with:
  - `events`
  - `raw_messages`

For scope design:

- introduce dedicated read scopes for these surfaces rather than reusing `observations:read`, `commands:read`, or `provenance:read`
- defer tenant/resource ownership checks to a later milestone

## Consequences

Positive:

- token mode now covers the remaining exposed event and raw-message operational reads
- route protection stays narrow and explicit
- readiness diagnostics stay aligned with actual protected surfaces
- development-mode behavior remains unchanged

Tradeoffs:

- token-mode enforcement is still partial rather than broad
- event and raw-message access is scope-gated but not yet ownership-gated
- other read surfaces such as entities, observations, commands, rules, and executor inspection remain for later milestones

## Alternatives Considered

Reuse `provenance:read` for `/events*` and `/raw-messages*`:

- rejected because provenance search is an aggregated search surface and should remain independently scoped

Reuse `observations:read` or `commands:read`:

- rejected because events and raw messages span multiple domains and can expose operational metadata beyond either read family

Protect the broader read API in the same change:

- rejected because it would widen scope, increase churn, and mix endpoint hardening with deferred ownership design
