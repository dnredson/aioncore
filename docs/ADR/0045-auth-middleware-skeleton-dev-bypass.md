# ADR 0045: Auth Middleware Skeleton With Dev Bypass

## Status

Accepted

## Context

AionCore now has a documented security model and auth roadmap in [docs/SECURITY_MODEL.md](../SECURITY_MODEL.md) and [ADR 0044](0044-security-model-and-auth-roadmap.md), but the runtime still allows all API calls without any auth plumbing.

Before introducing API tokens or protecting machine-facing endpoints, the runtime needs a small foundation that:

- parses auth mode explicitly
- installs middleware consistently
- exposes current auth mode in diagnostics
- preserves current development behavior
- avoids implying that production auth already exists

## Decision

Add an auth middleware skeleton to `aion-api` with `AIONCORE_AUTH_MODE=dev|disabled|token`.

Mode behavior:

- `dev`
  - default when unset
  - middleware is installed
  - requests are allowed through
  - request extensions receive an anonymous development-bypass auth context
  - `/ready` reports `auth.mode = dev`, `auth.enforced = false`, and `auth.dev_bypass = true`
- `disabled`
  - middleware is installed
  - requests are allowed through
  - request extensions receive an anonymous auth-disabled context
  - `/ready` reports `auth.mode = disabled` and `auth.enforced = false`
- `token`
  - config parsing recognizes the mode
  - runtime startup fails fast because token validation is not implemented yet

Add lightweight auth skeleton models:

- `AuthMode`
- `PrincipalType`
- `Principal`
- `AuthContext`

`PrincipalType` includes:

- `Anonymous`
- `User`
- `Device`
- `Adapter`
- `Executor`
- `Connector`
- `Service`
- `Admin`

## Consequences

- The runtime now has a stable place to attach future authenticated principals without changing current handler behavior.
- Readiness diagnostics make the current security posture explicit during development and operations.
- `token` mode cannot be mistaken for real protection because startup fails clearly.
- Existing APIs, tests, and examples continue to work in default development mode.

## Non-Goals

- No token issuance
- No token storage
- No token hashing
- No JWT, OAuth, or OIDC
- No login or user session flow
- No endpoint protection in this milestone
- No tenant-behavior changes
