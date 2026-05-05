# ADR 0047: Selected Machine Endpoint Auth Enforcement

## Status

Accepted

## Context

ADR 0045 introduced auth-mode plumbing and ADR 0046 introduced API tokens, hashing, storage, and bearer-token principal resolution. At that point, `AIONCORE_AUTH_MODE=token` could resolve a caller, but most business endpoints were still reachable without any scope checks.

AionCore needs the first real enforcement step, but it should stay narrow:

- keep `dev` as the default mode
- preserve `dev` and `disabled` behavior for local development and tests
- avoid protecting the entire API at once
- start with machine-facing routes that already map cleanly to machine principals
- protect connector secret administration and token administration because both change runtime security posture
- solve the first-token bootstrap problem without introducing login, JWT, or OAuth

## Decision

Introduce selected endpoint enforcement in `AIONCORE_AUTH_MODE=token`.

### Authorization Helpers

Add lightweight route-level helpers:

- `require_authenticated`
- `require_scope`
- `require_any_scope`

Behavior:

- `dev`
  - allow the request through
  - preserve `dev_bypass`
- `disabled`
  - allow the request through
- `token`
  - require a valid authenticated principal
  - return `401` for missing or invalid bearer tokens
  - require exact scope matches unless the principal has `admin:all`
  - return `403` for authenticated principals missing the required scope

### Protected Endpoint Groups

Protect only these routes in `token` mode:

- adapter endpoints
  - `POST /adapters` requires `adapters:register`
  - `PUT /adapters/{adapter_id}/heartbeat` requires `adapters:heartbeat`
- executor endpoints
  - `POST /executors` requires `executors:register`
  - `PUT /executors/{executor_id}/heartbeat` requires `executors:heartbeat`
  - `GET /executors/{executor_id}/commands/pending` requires `executors:poll`
  - `POST /executors/{executor_id}/commands/{command_id}/claim` requires `executors:claim`
  - `POST /executors/{executor_id}/commands/{command_id}/complete` requires `executors:report`
  - `POST /executors/{executor_id}/commands/{command_id}/fail` requires `executors:report`
- SmartSentinel executor bridge endpoints
  - `POST /integrations/smartsentinel/executors/register` requires `smartsentinel:executor_register`
  - `GET /integrations/smartsentinel/executors/{executor_id}/commands` requires `smartsentinel:executor_poll`
  - `POST /integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/claim` requires `smartsentinel:executor_claim`
  - `POST /integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/report` requires `smartsentinel:executor_report`
- connector secret endpoints
  - `POST /secrets/connectors`
  - `GET /secrets/connectors`
  - `GET /secrets/connectors/{secret_id}`
  - `DELETE /secrets/connectors/{secret_id}`
  - all require `secrets:admin`
- token management endpoints
  - `POST /auth/tokens`
  - `GET /auth/tokens`
  - `GET /auth/tokens/{token_id}`
  - `POST /auth/tokens/{token_id}/revoke`
  - all require `auth:tokens:admin`

### Bootstrap Administration

Use `AIONCORE_BOOTSTRAP_ADMIN_TOKEN` as the initial token-mode bootstrap mechanism.

Behavior:

- the configured value is never stored as a raw token record
- the runtime compares the presented bearer token against its hash
- a match resolves an in-memory admin principal with:
  - `auth:tokens:admin`
  - `admin:all`
- the bootstrap token value is not logged or emitted in events

### Audit Events

Keep the Milestone 49 token events and add selected authorization events:

- `aion:AuthTokenAccepted`
- `aion:AuthAccessDenied`
- `aion:AuthScopeDenied`

Plaintext bearer tokens are never recorded in those events.

## Consequences

### Positive

- `token` mode now protects the highest-value machine-facing routes without breaking the rest of the API.
- Connector secret administration and token administration now require explicit scope-based authorization.
- `admin:all` provides a narrow break-glass path for bootstrap and operator recovery.
- Existing executor capability and target-scope logic remains in place and is now combined with route-level token scopes.

### Negative

- Enforcement remains intentionally partial; many routes are still unprotected in `token` mode.
- The bootstrap admin token is environment-managed rather than storage-managed, so operational rotation is manual.
- Route-level checks are repetitive and should likely be refactored further when broader enforcement is introduced.

## Alternatives Considered

### Protect Every Endpoint In Token Mode Immediately

Rejected because it would create a larger compatibility break, make the milestone riskier, and force scope design for routes that do not yet have stable principal boundaries.

### Allow First Token Creation When No Tokens Exist

Rejected because it depends on mutable storage state, is easier to misread operationally, and creates a broader anonymous write exception than necessary.

### Introduce JWT Or OIDC For Bootstrap

Rejected because the current goal is incremental enforcement using the existing opaque token foundation without adding issuer, signing, or external identity-provider complexity.

## Follow-Up

Next milestones should:

- extend token enforcement to connector administration and ingestion write paths
- protect MCP and AI-context routes
- add broader tenant-aware authorization beyond the current route scope checks
- review whether `ready` should remain public in hardened deployments
