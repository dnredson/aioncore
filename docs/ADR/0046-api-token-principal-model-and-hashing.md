# ADR 0046: API Token Principal Model And Hashing

## Status

Accepted

## Context

ADR 0044 defined the security roadmap and ADR 0045 added auth-mode parsing plus auth-context plumbing, but `AIONCORE_AUTH_MODE=token` still failed fast because there was no token model, no persistent storage, and no bearer-token validation path.

AionCore needs a first credential type that:

- works for machine and operator bootstrap scenarios
- binds to the existing principal model
- is compatible with in-memory and PostgreSQL storage backends
- never stores raw bearer tokens
- keeps current local development behavior intact until endpoint protection is introduced in Milestone 50

## Decision

Implement an API token foundation with these properties:

- Add an `ApiToken` record with:
  - `id`
  - `tenant_id`
  - `token_name`
  - `token_prefix`
  - `token_hash`
  - `principal_type`
  - `principal_id`
  - `scopes`
  - `expires_at`
  - `revoked_at`
  - `last_used_at`
  - `metadata`
  - `created_at`
  - `updated_at`
- Add `ApiTokenPrincipalType` aligned with the runtime principal model:
  - `User`
  - `Device`
  - `Adapter`
  - `Executor`
  - `Connector`
  - `Service`
  - `Admin`
- Issue opaque tokens in the format `aion_<prefix>_<secret>`.
- Return the raw token only once in the creation response.
- Persist only:
  - `token_prefix`
  - `token_hash`
  - token metadata and principal binding
- Hash tokens with SHA-256 over the full raw token value.
- Compare stored and presented token hashes with constant-time equality.
- Add token storage support to:
  - `InMemoryStorage`
  - `PostgresStorage`
- Add PostgreSQL migration `0012_create_api_tokens.sql`.
- Add token-management endpoints:
  - `POST /auth/tokens`
  - `GET /auth/tokens`
  - `GET /auth/tokens/{token_id}`
  - `POST /auth/tokens/{token_id}/revoke`
- Add `GET /auth/whoami` as a safe diagnostic endpoint.
- Make `AIONCORE_AUTH_MODE=token` start successfully and attempt bearer-token principal resolution in middleware.
- Do not broadly enforce endpoint scopes or authentication yet.

## Consequences

### Positive

- AionCore now has a real tenant-bound machine credential foundation.
- Raw token values are no longer needed after issuance.
- Storage parity exists across memory and PostgreSQL.
- Token mode can resolve principals and update `last_used_at`.
- Audit/event hooks now exist for:
  - `aion:ApiTokenCreated`
  - `aion:ApiTokenRevoked`
  - `aion:ApiTokenUsed`
  - `aion:ApiTokenRejected`

### Negative

- SHA-256 token hashing is intentionally simple and meant as a foundation, not a final hardened credential subsystem.
- Endpoint protection remains partial in this milestone.
- In-memory bootstrap tokens do not survive process restart, so practical token-mode bootstrap for restarted processes still benefits from durable storage.

## Alternatives Considered

### Fail Fast In Token Mode Until Full Endpoint Protection

Rejected because it blocks incremental progress and keeps the runtime unable to exercise token principal resolution.

### Store Raw Tokens Server-Side

Rejected because bearer tokens should be treated as secrets and should not be recoverable from normal storage.

### Implement JWT/OIDC First

Rejected because the immediate need is a minimal AionCore-managed credential that works without introducing issuer, signing-key, browser-flow, or external identity-provider complexity.

## Follow-Up

Milestone 50 should:

- start protecting selected machine-facing endpoints
- enforce authenticated principals on those routes
- introduce scope checks using the stored token scopes
- tighten token-management authorization rules beyond the current bootstrap-oriented foundation
