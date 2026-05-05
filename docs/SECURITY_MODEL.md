# AionCore Security Model

This document defines the authentication and authorization architecture for AionCore APIs while runtime enforcement is being introduced incrementally.

Milestone 60 adds selected tenant-aware write authorization on top of the Milestone 50 through 57 foundation. It still does not broadly enforce authentication across the full API, add login, validate JWTs, or implement full authorization.

## Status

- Current local runtime behavior: `dev` remains the default and still bypasses auth; `token` mode now enforces selected machine-facing, broader read, selected write, MCP/AI, and secret-management routes, with first-pass tenant/resource ownership checks on selected protected reads and writes.
- Current production suitability: not suitable for exposed production deployment without additional protection in front of the API.
- Current runtime auth foundation: middleware installed with development-mode bypass, explicit disabled mode, or token principal resolution.
- Current selected enforcement in `token` mode:
  - `/auth/tokens*` requires `auth:tokens:admin`
  - `/adapters` registration and heartbeat require adapter scopes
  - `/adapters` list, detail, and `/adapters/{adapter_id}/status` require `adapters:read`
  - `/executors` registration, heartbeat, polling, claim, complete, and fail require executor scopes
  - `/ingestion/connectors` create/update/enable/disable plus `/ingestion/workers/reconcile` and `/ingestion/connectors/{connector_id}/ttn-live-validate` require `connectors:admin`
  - `/ingestion/connectors` selected reads, TTN device-mapping reads, plus `/ingestion/workers/plan` and `/ingestion/workers/status` require `connectors:read`
  - `/ingestion/connectors/{connector_id}/ttn-device-mappings` create/update/enable/disable/delete requires `connectors:admin`
  - `/ingestion/connectors/{connector_id}/ingest` and `/ingest/http` require `ingestion:write`
  - `/integrations/smartsentinel/executors/*` register, poll, claim, and report require SmartSentinel executor scopes
  - `/integrations/smartsentinel/snapshots` requires `smartsentinel:ingest`
  - `/mcp`, `/mcp/tools`, and `/mcp/tools/{tool_name}` require `mcp:tools`
  - `/ai/context/entity/{entity_id}` requires `ai:context:read`
  - `/provenance/search` requires `provenance:read`
  - `/events` and `/events/{event_id}` require `events:read`
  - `/raw-messages` and `/raw-messages/{raw_message_id}` require `raw-messages:read`
  - `/entities`, `/entities/{entity_id}`, and `/entities/{entity_id}/context` require `entities:read`
  - `POST /entities` requires `entities:write`
  - `POST /relationships` requires `relationships:write`
  - `/observations` requires `observations:read`
  - `POST /observations` requires `observations:write`
  - `/commands` and `/commands/{command_id}` require `commands:read`
  - selected generic command writes require `commands:create`, `commands:approve`, `commands:write`, `commands:claim`, or `commands:lease`
  - `/actions`, `/actions/{action_id}`, and `/action-results` require `actions:read`
  - `POST /actions` and `POST /action-results` require `actions:write`
  - `/rules` and `/rules/{rule_id}` require `rules:read`
  - selected generic rule writes require `rules:write`
  - `/policies` requires `policies:read`; `PUT /policies` requires `policies:write`
  - `/entities/{entity_id}/capabilities` requires `capabilities:read`; `PUT` requires `capabilities:write`
  - `/executors`, `/executors/{executor_id}`, `/executors/{executor_id}/capabilities`, and `/executors/{executor_id}/scopes` require `executors:read`
  - `/executors/{executor_id}/capabilities` and `/executors/{executor_id}/scopes` also require `executors:admin` or `executors:write` for mutation
  - `/secrets/connectors*` requires `secrets:admin`
- Unprotected in this milestone: the rest of the API surface, plus remaining tenant-aware writes and ownership enforcement outside the selected protected read/write surfaces.

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

Milestone 57 current rule for selected protected token-mode reads:

- `admin:all` bypasses tenant/resource ownership checks for the selected protected read routes covered by Milestones 55 through 57.
- Otherwise, the authenticated principal tenant must match the resource `tenant_id`.
- Selected list/query endpoints return only resources for the principal tenant.
- Selected detail endpoints return `403` for known cross-tenant access.
- `dev` and `disabled` modes keep the existing bypass behavior unchanged.

Current Milestone 57 limitations:

- This is only a first ownership skeleton for selected read routes.
- Write paths are not yet broadly tenant-authorized beyond existing storage scoping.
- Cross-tenant sharing is not implemented.
- Relationship-based authorization is not implemented.
- Some routes still rely on direct resource `tenant_id` rather than richer graph-aware ownership rules.

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
- endpoint enforcement is still intentionally partial
- local bootstrap for token administration uses `AIONCORE_BOOTSTRAP_ADMIN_TOKEN`, which must be at least 24 characters long

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
- `relationships:write`
- `observations:write`
- `observations:read`
- `ingestion:write`
- `connectors:read`
- `connectors:admin`
- `events:read`
- `raw-messages:read`
- `secrets:admin`
- `commands:read`
- `actions:read`
- `commands:create`
- `commands:approve`
- `commands:write`
- `commands:claim`
- `commands:lease`
- `rules:read`
- `rules:write`
- `policies:read`
- `policies:write`
- `capabilities:read`
- `capabilities:write`
- `mcp:tools`
- `smartsentinel:ingest`
- `adapters:register`
- `adapters:heartbeat`
- `executors:register`
- `executors:read`
- `executors:write`
- `executors:admin`
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

- `entities:write` covers selected generic entity creation.
- `relationships:write` covers selected generic relationship creation.
- `ingestion:write` protects both generic `/ingest/http` and connector-aware machine writes at `/ingestion/connectors/{connector_id}/ingest`.
- `connectors:read` covers selected connector and worker operational reads without granting mutation.
- `connectors:admin` covers connector lifecycle mutation, TTN device-mapping administration, worker reconciliation, TTN live validation preflight, validation-related operator actions, enable/disable, and configuration updates.
- `events:read` covers `/events` list and detail reads without broadening command, observation, or provenance access.
- `raw-messages:read` covers `/raw-messages` list and detail reads, which can expose raw payloads, ingestion headers, connector metadata, and provenance-linked evidence references.
- `commands:read` covers command visibility through generic, executor, and AI-context read paths.
- `actions:read` covers action and action-result inspection without granting command mutation or execution reporting rights.
- `commands:create` covers command creation.
- `commands:approve` covers generic approve and reject flows.
- `commands:write` covers selected generic command lifecycle writes such as cancel, release, mark-executed, and mark-failed.
- `commands:lease` covers selected generic lease refresh, release, and recovery flows.
- `rules:read` covers generic rule inspection while `rules:write` covers selected generic rule mutation and evaluation flows.
- `policies:read` covers policy inspection without granting policy mutation.
- `policies:write` covers selected policy replacement writes.
- `capabilities:read` covers capability inspection without granting semantic graph mutation.
- `capabilities:write` covers selected capability replacement writes.
- `commands:claim` and `executors:poll` separate execution workflow from broad command administration.
- `adapters:register` and `adapters:heartbeat` keep adapter self-registration separate from broader operator APIs.
- `executors:read`, `executors:register`, `executors:heartbeat`, `executors:poll`, `executors:claim`, and `executors:report` separate executor catalog inspection from executor lifecycle operations.
- `executors:write` and `executors:admin` cover selected executor capability and scope configuration writes.
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
| `/relationships` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `relationships:write`; future `relationships:read` if standalone read routes are added | Selected write authorization now applies in Milestone 60. |
| `/observations` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `observations:read` for GET, `observations:write` for POST | Direct write path should remain limited; most machine writers should prefer ingestion endpoints. |
| `/raw-messages` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `raw-messages:read` | Implemented in Milestone 55 for list and detail reads. Sensitive because raw payloads and headers may include operational metadata. |
| `/ingest/http` | `DevicePrincipal`, `ConnectorPrincipal`, `ServicePrincipal`, optionally `UserPrincipal` in development or tooling cases | `ingestion:write` | Primary generic ingestion write surface. |
| `/ingestion/connectors` POST, PATCH, enable, disable, and `/ingestion/workers/reconcile` | `UserPrincipal`, `AdminPrincipal`, selected `ServicePrincipal` | `connectors:admin` | Implemented in Milestone 51 for selected connector administration and worker reconciliation only. |
| `/ingestion/connectors` selected GET routes, TTN device-mapping GET/list routes, and `/ingestion/workers/plan`, `/ingestion/workers/status` | `UserPrincipal`, `AdminPrincipal`, selected `ServicePrincipal` | `connectors:read` | Implemented across Milestones 51 and 52 for selected connector and worker operational reads plus TTN mapping inspection. |
| `/ingestion/connectors/{connector_id}/ttn-device-mappings` POST, PATCH, enable, disable, and DELETE | `UserPrincipal`, `AdminPrincipal`, selected `ServicePrincipal` | `connectors:admin` | Implemented in Milestone 52 for TTN device-mapping administration. |
| `/ingestion/connectors/{connector_id}/ingest` and `/ingest/http` | `ConnectorPrincipal`, `DevicePrincipal`, `ServicePrincipal`, optionally `UserPrincipal` in tooling cases | `ingestion:write` | Implemented across Milestones 51 and 52. |
| `/secrets/connectors/*` | `UserPrincipal`, `AdminPrincipal` | `secrets:admin` | Implemented in Milestone 50. Highest sensitivity operator surface. |
| `/commands/*` | `UserPrincipal`, `ExecutorPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `commands:read` for GET, `commands:create` for creation, `commands:approve` for approve/reject, `commands:write` for selected lifecycle writes, `commands:claim` for generic claim, `commands:lease` for selected lease writes | Selected generic write authorization now applies in Milestone 60. Executor-specific flows still use executor scopes. |
| `/actions/*` | `UserPrincipal`, `ExecutorPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `actions:read` for GET, `actions:write` for selected POST routes | Selected generic write authorization now applies in Milestone 60. |
| `/executors` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:read` for GET, `executors:register` for POST | Implemented in Milestones 50 and 56 for registration plus selected inspection reads. |
| `/executors/{executor_id}/heartbeat` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:heartbeat` | Implemented in Milestone 50. |
| `/executors/{executor_id}/commands/pending` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:poll` | Implemented in Milestone 50. Existing executor capability and target-scope matching still applies. |
| `/executors/{executor_id}/commands/{command_id}/claim` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:claim` | Implemented in Milestone 50. Existing executor capability and target-scope matching still applies. |
| `/executors/{executor_id}/commands/{command_id}/complete` and `/fail` | `ExecutorPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `executors:report` | Implemented in Milestone 50. Existing executor capability and target-scope matching still applies. |
| `/integrations/smartsentinel/snapshots` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:ingest` | Implemented in Milestone 51. |
| `/integrations/smartsentinel/executors/register` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:executor_register` | Implemented in Milestone 50. |
| `/integrations/smartsentinel/executors/{executor_id}/commands` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:executor_poll` | Implemented in Milestone 50. |
| `/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/claim` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:executor_claim` | Implemented in Milestone 50. |
| `/integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/report` | `ExecutorPrincipal`, `ServicePrincipal`, `UserPrincipal`, `AdminPrincipal` depending on subpath | `smartsentinel:executor_report` | Implemented in Milestone 50. |
| `/adapters` and `/adapters/{adapter_id}` | `AdapterPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `adapters:register` for POST, `adapters:read` for GET | Implemented across Milestones 50, 51, and 52 for registration plus selected operational reads. |
| `/adapters/{adapter_id}/heartbeat` | `AdapterPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `adapters:heartbeat` | Implemented in Milestone 50. |
| `/adapters/{adapter_id}/status` | `AdapterPrincipal`, `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `adapters:read` | Implemented in Milestone 51. |
| `/mcp` | `UserPrincipal`, `ServicePrincipal`, `AdminPrincipal` | `mcp:tools` | Implemented in Milestone 54 for the current minimal JSON-RPC compatibility endpoint. Production still requires Origin validation and stronger transport hardening. |
| `/mcp/tools` | `UserPrincipal`, `ServicePrincipal`, `AdminPrincipal` | `mcp:tools` | Implemented in Milestone 54 for both listing and invocation. |
| `/ai/context/*` | `UserPrincipal`, `ServicePrincipal`, `AdminPrincipal` | `ai:context:read` | Implemented in Milestone 54 for entity AI context reads. AI context is read-only but aggregates sensitive topology and operational data. |
| `/rules/*` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `rules:read` for GET, `rules:write` for selected mutation and evaluation-style administration | Selected generic write authorization now applies in Milestone 60. |
| `/events/*` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `events:read` | Implemented in Milestone 55 for list and detail reads. Event data spans telemetry, control audit, and provenance-linked operational history. |
| `/provenance/search` | `UserPrincipal`, `AdminPrincipal`, limited `ServicePrincipal` | `provenance:read` | Implemented in Milestone 54. Aggregates events, raw messages, observations, and evidence-oriented metadata. |
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
- AI context and provenance search may remain easy to use in local development, but they are sensitive surfaces in production.
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
- `AIONCORE_BOOTSTRAP_ADMIN_TOKEN` is intended only for local bootstrap and development. Remove it after creating real admin tokens, and do not expose it publicly.
- Successful validation updates `last_used_at` and emits `aion:ApiTokenUsed`.
- Invalid, expired, revoked, or malformed presented tokens resolve to an anonymous context and emit `aion:ApiTokenRejected`.
- `GET /ready` reports token-mode diagnostics as `enforcement_level = partial`, not full, because only selected endpoint groups are protected in this milestone.
- `GET /ready` reports `protected_endpoint_groups` as:
  - `auth_tokens`
  - `connector_secrets`
  - `adapters`
  - `executors`
  - `smartsentinel_executor_bridge`
  - `ingestion_connectors`
  - `connector_workers`
  - `connector_aware_ingestion`
  - `generic_http_ingestion`
  - `ttn_device_mappings`
  - `ttn_live_validation`
  - `smartsentinel_snapshot_ingestion`
  - `mcp_tools`
  - `ai_context`
  - `provenance_search`
  - `events`
  - `raw_messages`
  - `entities`
  - `observations`
  - `commands`
  - `actions`
  - `rules`
  - `policies`
  - `capabilities`
  - `executors_read`
  - `entity_writes`
  - `relationship_writes`
  - `observation_writes`
  - `command_writes`
  - `action_writes`
  - `rule_writes`
  - `policy_writes`
  - `capability_writes`
  - `executor_config_writes`
- `GET /ready` reports only `bootstrap_admin_configured = true|false`; it never returns bootstrap token material.
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
- keep existing endpoints working in development mode without credentials

### Milestone 49

Add API token principal model, token hashing, storage model, operator-managed token issuance, `/auth/whoami`, and token-mode principal resolution without broad endpoint enforcement.

### Milestone 50

Protect selected machine-facing routes first because they already represent clear principal boundaries and higher-risk operational actions. This milestone starts real endpoint authentication and scope checks using the token model introduced in Milestone 49.

### Milestone 51

Protect connector administration, selected connector and worker operational reads, connector-aware ingestion, TTN live validation preflight, SmartSentinel snapshot ingestion, and selected adapter operational reads without broadly protecting the full API.

### Milestone 52

Protect remaining ingestion and connector-related operational gaps: generic `/ingest/http`, TTN device-mapping routes, and adapter detail reads, while keeping broader API enforcement deferred.

### Milestone 53

Harden auth readiness diagnostics and bootstrap-admin handling without broadening route protection.

Implemented shape:

- replace the old boolean readiness enforcement signal with `auth.enforcement_level`
- report `none` for `dev` and `disabled`, and `partial` for the currently selected `token`-mode protections
- expose the current protected endpoint groups explicitly through `/ready`
- report only `bootstrap_admin_configured = true|false` in readiness
- require `AIONCORE_BOOTSTRAP_ADMIN_TOKEN` to be at least 24 characters long
- emit startup warnings that the bootstrap admin token is for local bootstrap/development only, should be removed after creating real admin tokens, and must not be exposed publicly

### Milestone 54

Protect MCP-style local tool surfaces, the minimal MCP JSON-RPC compatibility endpoint, AI context, and provenance search in `token` mode without changing dev/disabled behavior.

Implemented shape:

- require `mcp:tools` for `GET /mcp/tools`, `POST /mcp/tools/{tool_name}`, and `POST /mcp`
- require `ai:context:read` for `GET /ai/context/entity/{entity_id}`
- require `provenance:read` for `GET /provenance/search`
- continue to satisfy all three scope checks with `admin:all`
- extend `GET /ready` `protected_endpoint_groups` with `mcp_tools`, `ai_context`, and `provenance_search`
- explicitly leave `/events*` and `/raw-messages*` open for a later milestone rather than widening this rollout

### Milestone 55

Protect event and raw-message operational reads in `token` mode without changing dev/disabled behavior or broadening to full API ownership enforcement.

Implemented shape:

- require `events:read` for `GET /events` and `GET /events/{event_id}`
- require `raw-messages:read` for `GET /raw-messages` and `GET /raw-messages/{raw_message_id}`
- continue to satisfy both scope checks with `admin:all`
- keep `/provenance/search` on `provenance:read`
- extend `GET /ready` `protected_endpoint_groups` with `events` and `raw_messages`

### Milestone 56

Protect broader generic read surfaces in `token` mode without changing dev/disabled behavior or introducing tenant/resource ownership enforcement.

Implemented shape:

- require `entities:read` for entity list, detail, and context reads
- require `observations:read` for `/observations`
- require `commands:read` for command list and detail reads
- require `actions:read` for `/actions`, `/actions/{action_id}`, and `/action-results`
- require `rules:read` for rule list and detail reads
- require `policies:read` for `/policies`
- require `capabilities:read` for `GET /entities/{entity_id}/capabilities`
- require `executors:read` for executor catalog, detail, capability, and scope reads
- continue to satisfy these scope checks with `admin:all`
- extend `GET /ready` `protected_endpoint_groups` with `entities`, `observations`, `commands`, `actions`, `rules`, `policies`, `capabilities`, and `executors_read`
- explicitly leave tenant/resource ownership enforcement and broad write-surface protection for later milestones

### Milestone 57

Add the first tenant/resource ownership skeleton for selected token-mode protected read surfaces without changing dev/disabled behavior or attempting full authorization.

Implemented shape:

- keep `dev` as the default auth mode and keep `dev` and `disabled` bypass behavior unchanged
- keep `admin:all` as the break-glass bypass for both scope checks and the new selected read-route ownership checks
- for selected protected read routes in `token` mode, require the principal tenant to match the resource `tenant_id` unless `admin:all` is present
- return only same-tenant resources for selected list/query reads in `token` mode unless `admin:all` is present
- return `403` for known cross-tenant detail reads on the selected protected surfaces
- apply this first-pass ownership enforcement to:
  - `/entities`, `/entities/{entity_id}`, `/entities/{entity_id}/context`
  - `/observations`
  - `/commands`, `/commands/{command_id}`
  - `/actions`, `/actions/{action_id}`, `/action-results`
  - `/rules`, `/rules/{rule_id}`
  - `/policies`
  - `/entities/{entity_id}/capabilities`
  - `/executors`, `/executors/{executor_id}`, `/executors/{executor_id}/capabilities`, `/executors/{executor_id}/scopes`
  - `/events`, `/events/{event_id}`, `/raw-messages`, `/raw-messages/{raw_message_id}`
- filter entity-context relationships so inconsistent cross-tenant references do not leak through context reads
- explicitly leave tenant-aware writes, cross-tenant sharing, relationship-based authorization, and remaining route coverage for later milestones

### Milestone 60

Add selected tenant-aware write authorization in `token` mode without changing `dev`/`disabled` behavior and without introducing cross-tenant sharing or a full policy engine.

Implemented shape:

- require explicit write scopes on selected generic write routes for entities, relationships, observations, commands, actions, rules, policies, capabilities, and executor configuration
- keep `admin:all` as the break-glass bypass for scope and tenant checks
- for non-admin token principals, require same-tenant target entities and same-tenant target resources on the selected write surfaces
- create selected generic resources under the authenticated tenant context instead of trusting caller-supplied tenant information
- return `403` for known cross-tenant writes on the selected protected write surfaces
- extend `GET /ready` `protected_endpoint_groups` with:
  - `entity_writes`
  - `relationship_writes`
  - `observation_writes`
  - `command_writes`
  - `action_writes`
  - `rule_writes`
  - `policy_writes`
  - `capability_writes`
  - `executor_config_writes`
- explicitly leave cross-tenant sharing, remaining write coverage, JWT/OIDC/OAuth, and a full authorization engine for later milestones

## Open Questions For Later Milestones

- Whether command approval should get its own dedicated scope.
- Whether connector-aware ingestion should distinguish connector administration from connector write traffic with a separate scope.
- Whether `ready` should be fully authenticated in all production deployments or only kept network-private.
- How the future write-authorization model should compose tenant ownership, role scopes, and policy decisions without overfitting the current modular-monolith deployment.
- Whether device credentials should bind directly to entity IDs, connector IDs, or both.
- Whether service-to-service auth should start with API tokens or skip directly to mTLS in distributed mode.
- How and when tenant/resource ownership checks should be layered onto the remaining unprotected write and read surfaces.
