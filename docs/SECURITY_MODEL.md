# AionCore Security Model

This document defines the authentication and authorization architecture for AionCore APIs while runtime enforcement is being introduced incrementally.

Milestone 50 adds selected endpoint enforcement for machine-facing routes, connector secret administration, and token administration. It still does not broadly enforce authentication across the full API, add login, or validate JWTs.

## Status

- Current local runtime behavior: `dev` remains the default and still bypasses auth; `token` mode now enforces selected machine-facing and secret-management routes only.
- Current production suitability: not suitable for exposed production deployment without additional protection in front of the API.
- Current runtime auth foundation: middleware installed with development-mode bypass, explicit disabled mode, or token principal resolution.
- Current selected enforcement in `token` mode:
  - `/auth/tokens*` requires `auth:tokens:admin`
  - `/adapters` registration and heartbeat require adapter scopes
  - `/executors` registration, heartbeat, polling, claim, complete, and fail require executor scopes
  - `/integrations/smartsentinel/executors/*` register, poll, claim, and report require SmartSentinel executor scopes
  - `/secrets/connectors*` requires `secrets:admin`
- Unprotected in this milestone: the rest of the API surface, including entities, observations, generic commands, rules, connectors, MCP, and SmartSentinel snapshot ingestion.

## Security Goals

- Preserve tenant isolation across all API surfaces.
- Require explicit authentication for every non-public production API surface.
- Separate human, device, adapter, executor, connector, and internal service identities.
- Keep raw messages, canonical observations, commands, secrets, and provenance auditable.
- Prevent secret disclosure through API responses, logs, events, and MCP tool output.
- Keep MCP and AI-facing access read-oriented by default.
- Allow all-in-one deployment first while keeping identities valid for future distributed services.
- Support pluggable storage backends without coupling the security model to one database vendor.

## Non-Goals For This Milestone

- No broad authentication enforcement across the full API.
- No login UI, browser session flow, or password-based user auth.
- No JWT minting.
- No secret manager, KMS, or Vault integration.
- No mTLS implementation.
- No OAuth/OIDC provider integration.
- No broad scope enforcement outside the selected endpoint groups listed below.

## Threat Model

### Assets

- Tenant-scoped entities, relationships, observations, raw messages, events, commands, actions, rules, and provenance data.
- Connector secrets and future device or executor credentials.
- Executor command lifecycle state, approval state, and leases.
- Adapter registration and operational status.
- SmartSentinel snapshot and command-reporting channels.
- MCP tool surface and AI context output.

### Threats

- Unauthenticated callers reading or mutating tenant data.
- Cross-tenant access caused by missing principal-to-tenant checks.
- Privilege escalation from read APIs into command, rule, secret, or MCP write paths.
- Secret leakage through logs, events, errors, traces, or debug endpoints.
- Replay of adapter, executor, or device submissions.
- Forged heartbeat, command report, or SmartSentinel bridge traffic.
- Public exposure of localhost-oriented MCP endpoints.
- Abuse of raw-message or provenance queries to exfiltrate sensitive operational metadata.
- Credential reuse across connector, adapter, executor, and service roles.
- Future distributed deployments trusting internal traffic without authenticating it.

### Assumptions

- Transport security such as TLS will be required in production even though local development may use plain HTTP.
- Reverse proxies or ingress controllers may exist in front of AionCore, but AionCore must still authenticate callers itself in production mode.
- Storage backends can persist credential hashes or references, but secret plaintext must remain minimized and redacted from normal API output.

## Trust Boundaries

### Public Or Semi-Public API Boundary

Internet, LAN, or partner-facing clients calling the AionCore HTTP API must be treated as untrusted until authenticated and authorized.

### Tenant Boundary

Every principal except platform-level administration must be bound to one tenant. Tenant identity must be part of principal resolution, audit records, and authorization checks.

### Device And Integration Boundary

Devices, connectors, edge adapters, SmartSentinel bridges, and executor agents are external actors. They must not inherit user privileges and must receive narrowly scoped credentials.

### Internal Service Boundary

Future internal services, worker processes, or sidecars must authenticate as services rather than relying on network location alone.

### Local MCP Boundary

`/mcp`, `/mcp/tools`, and `/ai/context/*` are local-development-friendly today but must be treated as sensitive API surfaces. Localhost access is not the same as trusted production access.

### Secret Boundary

Connector secret values, future token hashes, private keys, and broker credentials cross the highest sensitivity boundary. They must never appear in normal API reads, logs, events, or MCP output.

## Tenant Isolation

- Every principal resolves to exactly one tenant unless it is an explicit platform admin principal.
- Every storage query and write path must eventually enforce tenant scoping from the authenticated principal, not from caller-supplied request data alone.
- Resource identifiers from another tenant must resolve as not found or forbidden without disclosing existence details.
- Connector secrets, adapters, executor agents, commands, rules, and provenance queries must all remain tenant-scoped.
- Platform admin behavior should be explicit and rare. It must not be the default for service-to-service traffic.

## Principal Types

These principal types define the future authenticated caller model.

### UserPrincipal

Human operator or application user acting within one tenant.

Typical uses:

- read and manage entities, relationships, observations, events, rules, connectors, and commands
- inspect provenance and AI context
- approve or reject commands when policy requires human approval

### DevicePrincipal

A physical or logical telemetry-producing device bound to one tenant and usually one producer identity.

Typical uses:

- submit ingestion payloads
- optionally read limited registration or acknowledgement data in future designs

### AdapterPrincipal

An edge adapter instance running outside AionCore and representing a site, host, broker edge, or protocol gateway.

Typical uses:

- register adapter presence
- send heartbeat and status
- publish connector-aware ingestion on behalf of configured local sources when allowed

### ExecutorPrincipal

An external command executor agent that polls compatible commands, claims work, and reports results.

Typical uses:

- list pending commands assigned by capability and scope
- claim commands
- complete or fail commands
- report heartbeat

### ConnectorPrincipal

A machine identity for ingestion connectors or future source-specific runtime workers when direct connector authentication is needed.

Typical uses:

- write ingestion data
- perform source-specific validation calls
- access only connector-bound operational metadata

### ServicePrincipal

An internal AionCore service or trusted automation component.

Typical uses:

- internal service-to-service calls in distributed deployments
- maintenance tasks
- controlled access to storage-backed operational APIs

### AdminPrincipal

A platform-level administrator identity with intentionally elevated privileges across tenants or deployment-wide operations.

Typical uses:

- controlled support and operations
- deployment-wide diagnostics
- break-glass maintenance

`AdminPrincipal` should be rare, fully audited, and disabled by default in normal tenant-facing workflows.

## Credential Types

These are the planned credential mechanisms. Not all should be implemented at once.

### API Token

Opaque token for simple machine or operator access. Recommended as the first implementation for AionCore-managed access.

Implemented handling in Milestone 49:

- issued by `POST /auth/tokens`
- formatted as `aion_<prefix>_<secret>`
- stored server-side as a SHA-256 hash, never in plaintext after creation
- looked up by `token_prefix` and validated against the stored hash
- bound to one principal and one tenant
- scoped and revocable
- returned in plaintext only once in the creation response

Current limitations:

- token issuance is foundational and not yet a full operator-hardening story
- endpoint enforcement is still intentionally partial in Milestone 50
- local bootstrap for token administration uses `AIONCORE_BOOTSTRAP_ADMIN_TOKEN`

### JWT Access Token

Bearer token for future stateless access once a stable issuer and claim model exist.

Expected uses:

- user-facing APIs
- service federation
- future browser or CLI integrations

### Device Key

A device-specific credential for ingestion or registration flows.

Expected uses:

- device to `/ingest/http`
- device to connector-aware ingestion when explicitly allowed

### Adapter Token

Credential bound to one edge adapter deployment or adapter registration record.

Expected uses:

- `/adapters`
- `/adapters/{adapter_id}/heartbeat`
- optional connector-aware ingestion from registered adapters

### Executor Token

Credential bound to one executor agent identity.

Expected uses:

- `/executors/*`
- `/integrations/smartsentinel/executors/*`

### Connector Secret Reference

A stored secret reference used by AionCore runtime workers to authenticate to upstream systems such as MQTT brokers.

Important boundary:

- this is not a caller authentication mechanism for the AionCore API
- it authenticates AionCore to external systems
- it still needs authorization controls because managing it changes security posture

### mTLS Certificate (Future)

Certificate-based identity for high-trust machine or service paths.

Good fit for:

- internal services
- adapters in controlled environments
- industrial or regulated deployments

### OAuth/OIDC (Future)

External identity provider integration for user and possibly service authentication.

Good fit for:

- human users
- enterprise SSO
- future delegated admin workflows

## Authorization Scopes

Scopes are additive. Principals should receive the minimum set required for their role.

- `entities:read`
- `entities:write`
- `observations:write`
- `observations:read`
- `ingestion:write`
- `connectors:admin`
- `secrets:admin`
- `commands:read`
- `commands:create`
- `commands:claim`
- `commands:report`
- `rules:admin`
- `mcp:tools`
- `smartsentinel:ingest`
- `adapters:register`
- `adapters:heartbeat`
- `executors:register`
- `executors:heartbeat`
- `executors:poll`
- `executors:claim`
- `executors:report`
- `smartsentinel:executor_register`
- `smartsentinel:executor_poll`
- `smartsentinel:executor_claim`
- `smartsentinel:executor_report`
- `auth:tokens:admin`
- `admin:all`

### Scope Notes

- `entities:write` covers entity, relationship, capability, and payload-profile mutation unless a later split is needed.
- `connectors:admin` covers connector lifecycle, worker planning, TTN mappings, validation, enable, disable, and configuration updates.
- `commands:read` covers command visibility through generic, executor, and AI-context read paths.
- `commands:create` covers command creation and approval-oriented write flows may later split into narrower scopes such as `commands:approve`.
- `commands:claim` and `executors:poll` separate execution workflow from broad command administration.
- `adapters:register` and `adapters:heartbeat` keep adapter self-registration separate from broader operator APIs.
- `executors:register`, `executors:heartbeat`, `executors:poll`, `executors:claim`, and `executors:report` separate executor lifecycle operations.
- SmartSentinel executor bridge scopes are intentionally separate from generic executor scopes so bridge tokens can stay narrow.
- `mcp:tools` should authorize local or production MCP tool invocation, not generic write access.
- `auth:tokens:admin` covers token issuance, listing, inspection, and revocation.
- `admin:all` is a reserved break-glass scope and satisfies any route scope check.

## API Authentication Model

### General Rules

- Production mode should require authentication for every endpoint except explicitly public health-style endpoints.
- Authentication must resolve principal type, principal ID, tenant ID, and scopes before request handling reaches business logic.
- Authorization must validate both scope and tenant/resource ownership.
- Caller-supplied `tenant_id` fields must not override authenticated tenant context.

### User-Facing APIs

User and admin APIs should eventually support API tokens first, with JWT or OAuth/OIDC as future extensions.

### Machine-Facing APIs

Devices, adapters, executors, connectors, and bridges should use dedicated machine credentials rather than user tokens reused across automation.

### Secret Storage For API Credentials

For bearer-style API credentials, only hashed or otherwise non-recoverable server-side representations should be persisted. Plaintext display must remain one-time only at issuance.

## Device Credentials

- Devices should not share tenant-wide operator tokens.
- Each device or logical producer group should have its own credential.
- Device credentials should be scoped primarily to `ingestion:write`.
- Device credentials may optionally bind to allowed producer entity IDs, connector IDs, or payload formats in future hardening.
- Device credential rotation and revocation must be supported in the future identity model.

## Edge Adapter Credentials

- Each adapter deployment should have an adapter-specific credential.
- The credential should identify both tenant and adapter identity.
- Adapter credentials should not expose connector secret values.
- Adapter operations should be limited to registration, heartbeat, status, and explicitly allowed ingestion paths.
- Future adapter credentials may be API tokens first and mTLS later.

## Executor Agent Credentials

- Each executor agent should have a dedicated credential, not a shared operator token.
- Executor credentials should be limited to command polling, claim, completion, failure reporting, and heartbeat.
- Executor scopes should be combined with existing executor capability and target-scope matching.
- Lease and approval workflows remain domain logic and are not replaced by auth scopes alone.

## SmartSentinel Bridge Credentials

- SmartSentinel bridge calls should authenticate as either `ExecutorPrincipal` or a narrowly scoped `ServicePrincipal`.
- Snapshot ingestion and executor reporting should not share a broad tenant admin token.
- Bridge credentials should be able to separate `smartsentinel:ingest` from `executors:poll` and `executors:report`.
- Provenance and evidence metadata remain auditable, but bridge credentials must not be allowed to exfiltrate unrelated tenant data by default.

## Connector Secret Handling

- Connector secrets remain tenant-scoped references.
- Secret values stay write-only in API responses.
- Secret plaintext must not be logged, copied into events, or returned by validation endpoints.
- Future secret administration must require `secrets:admin`.
- Connector create and update flows that attach `secret_ref_id` should require `connectors:admin`.
- Runtime workers may resolve secrets internally, but the resolved value must not leave the worker boundary through status, readiness, errors, or audit events.

## Broker Credential Handling

- Broker username and password or token authenticate AionCore to the upstream broker, not the broker or device to AionCore.
- Broker credentials must not appear in logs, readiness responses, connector status, raw-message headers, or MCP output.
- Future production deployments should prefer TLS and should consider mTLS where broker capabilities allow it.
- Per-device MQTT authorization remains separate future work and is not solved by connector secret references.

## MCP Local Vs Production Exposure

### Local Development

- `/mcp`, `/mcp/tools`, and `/ai/context/*` may remain easy to use in local development mode.
- Local mode must carry explicit warnings that these endpoints expose semantic and operational data.

### Production

- MCP endpoints must require authentication and `mcp:tools` or equivalent read scopes.
- Public exposure should require Origin validation for browser-like clients.
- Production MCP must remain read-oriented by default.
- Any future write-capable MCP tool must require separate scope design, approval, and audit controls.

## Audit And Event Requirements

Future auth implementation should record enough information to explain who did what without leaking secrets.

Audit requirements:

- authenticated principal type and principal ID
- tenant ID
- requested endpoint and method
- target resource identifiers when available
- authorization decision outcome
- reason for denial when applicable
- correlation or trace identifiers when available
- timestamp and source IP or transport metadata when available

Never include:

- plaintext API tokens
- connector `secret_value`
- passwords
- private keys
- full Authorization headers

High-value audit events include:

- API token create, revoke, use, and rejection
- command create, approve, reject, claim, complete, and fail
- rule create, enable, disable, and evaluate
- connector create, update, enable, disable, validate, and secret attachment changes
- connector secret create and delete
- adapter registration and heartbeat anomalies
- executor registration and report calls
- SmartSentinel snapshot ingestion and command reporting
- MCP tool invocation in production mode

## Endpoint Protection Plan

The table below describes the intended future protection model. It does not change current behavior.

| Endpoint group | Future principal types | Required scope(s) | Notes |
| --- | --- | --- | --- |
| `/entities` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `entities:read` for GET, `entities:write` for POST | Includes standard entity CRUD-style growth over time. |
| `/relationships` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `entities:write` | Relationship mutation stays under semantic graph administration. |
| `/observations` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `observations:read` for GET, `observations:write` for POST | Direct write path should remain limited; most machine writers should prefer ingestion endpoints. |
| `/raw-messages` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `observations:read` | Sensitive because raw payloads and headers may include operational metadata. |
| `/ingest/http` | `DevicePrincipal`, `ConnectorPrincipal`, `ServicePrincipal`, optionally `UserPrincipal` in development or tooling cases | `ingestion:write` | Primary generic ingestion write surface. |
| `/ingestion/connectors/*` | `UserPrincipal`, `AdminPrincipal`, selected `ConnectorPrincipal` or `ServicePrincipal` for narrow validation flows | `connectors:admin` | Includes connector CRUD, enable/disable, status, validate, TTN mappings, worker plan, and connector-aware ingest unless separated later. |
| `/secrets/connectors/*` | `UserPrincipal`, `AdminPrincipal` | `secrets:admin` | Implemented in Milestone 50. Highest sensitivity operator surface. |
| `/commands/*` | `UserPrincipal`, `ExecutorPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `commands:read` for GET, `commands:create` for creation and approval-like writes, `commands:claim` for claim/release flows, `commands:report` for execution result writes where applicable | Approval may later split into its own scope. |
| `/actions/*` | `UserPrincipal`, `ExecutorPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `commands:read` for GET, `commands:report` for POST | Action reporting is part of execution audit. |
| `/executors` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:register` for POST | Implemented in Milestone 50 for `POST /executors` only. |
| `/executors/{executor_id}/heartbeat` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:heartbeat` | Implemented in Milestone 50. |
| `/executors/{executor_id}/commands/pending` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:poll` | Implemented in Milestone 50. Existing executor capability and target-scope matching still applies. |
| `/executors/{executor_id}/commands/{command_id}/claim` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:claim` | Implemented in Milestone 50. Existing executor capability and target-scope matching still applies. |
| `/executors/{executor_id}/commands/{command_id}/complete` and `/fail` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:report` | Implemented in Milestone 50. Existing executor capability and target-scope matching still applies. |
| `/integrations/smartsentinel/snapshots` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:ingest` | Still unprotected in Milestone 50. |
| `/integrations/smartsentinel/executors/register` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:executor_register` | Implemented in Milestone 50. |
| `/integrations/smartsentinel/executors/{executor_id}/commands` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:executor_poll` | Implemented in Milestone 50. |
| `/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/claim` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:executor_claim` | Implemented in Milestone 50. |
| `/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/report` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:executor_report` | Implemented in Milestone 50. |
| `/adapters` | `AdapterPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `adapters:register` for POST | Implemented in Milestone 50 for `POST /adapters` only. |
| `/adapters/{adapter_id}/heartbeat` | `AdapterPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `adapters:heartbeat` | Implemented in Milestone 50. |
| `/mcp` | `UserPrincipal`, `ServicePrincipal`, `AdminPrincipal` | `mcp:tools` | Production requires auth and Origin validation. |
| `/mcp/tools` | `UserPrincipal`, `ServicePrincipal`, `AdminPrincipal` | `mcp:tools` | Includes tool listing and invocation. |
| `/ai/context/*` | `UserPrincipal`, `ServicePrincipal`, `AdminPrincipal` | `entities:read`, `observations:read`, and `commands:read` when command context is included | AI context is read-only but aggregates sensitive data. |
| `/rules/*` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `rules:admin` | Rule evaluation can create commands and should remain restricted. |
| `/events/*` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `observations:read` or `commands:read` depending on event domain; first implementation can require both read families or a broader operator scope | Event data spans telemetry and control audit. |
| `/provenance/search` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `observations:read` and `commands:read` | Aggregates events, raw messages, and observations. |
| `/health` | public or any principal | none in first auth implementation | Intended for liveness probes. Must not expose secrets. |
| `/ready` | environment-dependent: private probe, `ServicePrincipal`, or public in dev | none in dev, authenticated in hardened production deployments when exposure is broader | Readiness reveals operational state and should usually stay private in production. |

## Development Mode Vs Production Mode

### Development Mode

- `AIONCORE_AUTH_MODE=dev` is the default for local work.
- Auth middleware is installed.
- Requests are allowed through with an anonymous development-bypass auth context attached internally.
- Startup logs and README must clearly warn that the API is unauthenticated.
- Intended for localhost, tests, and integration prototyping.
- MCP local tool layer may remain available for local development.
- Secret values still must remain redacted even when auth is disabled.

### Explicit Disabled Mode

- `AIONCORE_AUTH_MODE=disabled` keeps auth middleware plumbing available but explicitly disables auth for the runtime.
- Requests are allowed through with an anonymous auth-disabled context attached internally.
- This mode is useful for tests and local demos where operators want the bypass to be intentional rather than implicit.

### Token Mode Foundation

- `AIONCORE_AUTH_MODE=token` now starts successfully.
- Middleware attempts to resolve `Authorization: Bearer <token>` against stored API tokens.
- Valid stored tokens attach tenant, principal type, principal ID, scopes, and token ID into the internal `AuthContext`.
- If `AIONCORE_BOOTSTRAP_ADMIN_TOKEN` is set and the presented bearer token matches it exactly, AionCore resolves an in-memory admin principal with `auth:tokens:admin` and `admin:all`.
- Successful validation updates `last_used_at` and emits `aion:ApiTokenUsed`.
- Invalid, expired, revoked, or malformed presented tokens resolve to an anonymous context and emit `aion:ApiTokenRejected`.
- Selected protected routes in this milestone return:
  - `401` for missing or invalid bearer tokens
  - `403` for valid authenticated principals missing the required scope
- Selected protected routes also emit:
  - `aion:AuthTokenAccepted`
  - `aion:AuthAccessDenied`
  - `aion:AuthScopeDenied`
- Existing development behavior remains intentionally unchanged in `dev` and `disabled` modes.

### Production Mode

- Authentication required for all non-public endpoints.
- Tenant-bound principals and scopes required before business logic execution.
- Secrets and credential-like fields remain redacted in responses, logs, events, and MCP output.
- MCP must not be exposed publicly without authentication and Origin validation.
- Connector credentials must not be logged.
- TLS termination is expected.
- Readiness exposure should be reviewed and typically kept private to infrastructure.

## First Implementation Plan

### Milestone 48

Add auth middleware skeleton with request principal extraction, explicit development-mode bypass, and no broad behavior change beyond optional enforcement hooks.

Implemented shape:

- parse `AIONCORE_AUTH_MODE=dev|disabled|token`
- default to `dev` when unset
- attach an internal `AuthContext` with `AuthMode`, `Principal`, and `PrincipalType`
- report auth diagnostics in `/ready`
- keep `auth.enforced = false` for all current modes
- keep existing endpoints working in development mode without credentials

### Milestone 49

Add API token principal model, token hashing, storage model, operator-managed token issuance, `/auth/whoami`, and token-mode principal resolution without broad endpoint enforcement.

### Milestone 50

Protect selected machine-facing routes first because they already represent clear principal boundaries and higher-risk operational actions. This milestone starts real endpoint authentication and scope checks using the token model introduced in Milestone 49.

### Milestone 51

Protect connector secret endpoints and connector administration APIs, including secret attachment and validation flows.

### Milestone 52

Protect MCP endpoints and add production Origin validation for browser-like clients and local-tool HTTP exposure.

## Open Questions For Later Milestones

- Whether command approval should get its own dedicated scope.
- Whether connector-aware ingestion should distinguish connector administration from connector write traffic with a separate scope.
- Whether `ready` should be fully authenticated in all production deployments or only kept network-private.
- Whether device credentials should bind directly to entity IDs, connector IDs, or both.
- Whether service-to-service auth should start with API tokens or skip directly to mTLS in distributed mode.
