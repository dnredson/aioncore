# ADR 0044: Security Model And Auth Roadmap

## Status

Accepted

## Context

AionCore now includes ingestion connectors, dynamic MQTT workers, connector secret references, TTN validation and mappings, SmartSentinel snapshot ingestion and executor bridge endpoints, edge adapter registration and status APIs, command and executor lifecycle APIs, and a local MCP-style tool layer.

Most current APIs are still unauthenticated. Implementing security-sensitive runtime enforcement without a documented model would risk:

- inconsistent principal handling across user, device, adapter, executor, connector, and service callers
- weak tenant isolation
- accidental overexposure of MCP and AI-context endpoints
- secret leakage through connector administration and diagnostics
- unclear scope boundaries for commands, rules, provenance, and operational integrations

The platform needs an explicit security architecture before adding auth middleware, tokens, or protected endpoint behavior.

## Decision

Document the AionCore security model in [docs/SECURITY_MODEL.md](../SECURITY_MODEL.md).

The documented model establishes:

- security goals and threat model
- trust boundaries and tenant isolation rules
- principal types:
  - `UserPrincipal`
  - `DevicePrincipal`
  - `AdapterPrincipal`
  - `ExecutorPrincipal`
  - `ConnectorPrincipal`
  - `ServicePrincipal`
  - `AdminPrincipal`
- future credential types:
  - API token
  - JWT access token
  - device key
  - adapter token
  - executor token
  - connector secret reference for upstream broker auth
  - mTLS certificate as future work
  - OAuth/OIDC as future work
- proposed authorization scopes for entities, observations, ingestion, connectors, secrets, commands, rules, MCP, SmartSentinel, adapters, executors, and admin access
- an endpoint protection plan for current API groups
- separate development and production security modes
- a staged auth roadmap

The first staged roadmap is:

1. Milestone 48: auth middleware skeleton with development-mode bypass
2. Milestone 49: API token principal model and token hashing
3. Milestone 50: protect adapter and executor endpoints
4. Milestone 51: protect connector secrets and connector admin APIs
5. Milestone 52: protect MCP and production Origin validation

## Consequences

- Authentication design becomes explicit before runtime enforcement.
- Future implementation can add auth incrementally without changing the platform’s principal taxonomy midstream.
- Tenant isolation, connector secret handling, and MCP exposure rules are documented for both local and production deployment modes.
- Machine identities for devices, adapters, executors, connectors, and services are separated from operator identities.
- Production hardening work can proceed in staged milestones without blocking current local development or existing tests.

## Non-Goals

- No auth middleware.
- No login or session flow.
- No JWT implementation.
- No token issuance or storage implementation.
- No secret generation.
- No endpoint behavior change.
- No test changes driven by enforcement.
