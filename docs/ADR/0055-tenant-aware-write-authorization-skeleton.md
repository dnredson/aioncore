# ADR 0055: Tenant-Aware Write Authorization Skeleton

## Status

Accepted

## Context

ADR 0054 introduced the first tenant/resource ownership skeleton for selected protected read surfaces in `AIONCORE_AUTH_MODE=token`.

That left an intentional gap:

- several generic write paths were still open or only partially protected in token mode
- write handlers still relied mostly on the default runtime tenant rather than authenticated token tenant context
- selected read routes could return `403` for known cross-tenant access, but equivalent generic writes did not yet enforce the same staged tenant boundary

This milestone must extend the staged hardening model without changing the default `dev` behavior, without introducing cross-tenant sharing, and without implementing a full authorization engine.

## Decision

Add selected tenant-aware write authorization in `token` mode.

Implemented shape:

- keep `AIONCORE_AUTH_MODE=dev` as the default
- keep `dev` and `disabled` bypass behavior unchanged
- keep `admin:all` as the break-glass bypass for both scope checks and tenant checks
- require explicit write scopes on selected generic write routes:
  - `entities:write`
  - `relationships:write`
  - `observations:write`
  - `commands:create`
  - `commands:approve`
  - `commands:write`
  - `commands:claim`
  - `commands:lease`
  - `actions:write`
  - `rules:write`
  - `policies:write`
  - `capabilities:write`
  - `executors:write` or `executors:admin`
- on selected generic create routes, store new resources under the authenticated tenant context instead of trusting caller-supplied tenant information
- on selected generic mutation routes, require the target resource `tenant_id` to match the authenticated principal tenant unless `admin:all` is present
- on selected entity-linked write routes, require referenced entities to belong to the authenticated tenant unless `admin:all` is present
- return structured `401` for missing/invalid bearer tokens and structured `403` for missing scopes or known cross-tenant writes
- extend `/ready` `auth.protected_endpoint_groups` with selected write groups while keeping `enforcement_level = partial`

Covered generic write surfaces in this milestone:

- `POST /entities`
- `POST /relationships`
- `POST /observations`
- `POST /commands`
- selected generic command lifecycle and lease endpoints
- `POST /actions`
- `POST /action-results`
- `POST /rules`
- `PUT /rules/{rule_id}/enable`
- `PUT /rules/{rule_id}/disable`
- `POST /rules/evaluate`
- `PUT /policies`
- `PUT /entities/{entity_id}/capabilities`
- `PUT /executors/{executor_id}/capabilities`
- `PUT /executors/{executor_id}/scopes`

## Consequences

Positive:

- token-mode write behavior now matches the staged tenant-isolation direction already established for selected reads
- selected generic create/update flows now align resource tenancy with authenticated token tenancy
- cross-tenant generic writes now fail explicitly with `403` instead of silently relying on default tenant behavior
- admin recovery remains possible through `admin:all`

Negative:

- write authorization is still intentionally partial
- some generic writes still depend on direct `tenant_id` checks rather than richer graph-aware ownership logic
- cross-tenant sharing remains unsupported
- executor-specific and connector-specific flows still rely on their existing specialized scope models rather than a unified policy engine

## Rejected Alternatives

Implement full authorization engine now:

- rejected because this milestone is intentionally incremental and must not refactor the entire API security model in one step

Introduce cross-tenant sharing now:

- rejected because sharing semantics require a clearer policy model, audit rules, and user-facing configuration surface

Switch default auth mode away from `dev`:

- rejected because it would break current development workflows and violates the staged rollout plan

Use JWT/OIDC/OAuth instead of opaque API tokens:

- rejected because the current milestone is about extending the existing token-mode model, not replacing it
