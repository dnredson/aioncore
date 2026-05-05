# ADR 0048: Connector And Ingestion Auth Enforcement

## Status

Accepted

## Context

ADR 0045 introduced auth-mode plumbing, ADR 0046 introduced API tokens and token principal resolution, and ADR 0047 enforced selected machine-facing routes such as adapters, executors, SmartSentinel executor bridge paths, connector secret administration, and token administration.

After Milestone 50, important connector and machine-ingestion routes were still public in `AIONCORE_AUTH_MODE=token`:

- connector create, update, enable, and disable
- connector worker reconcile
- selected connector and worker operational reads
- connector-aware `POST /ingestion/connectors/{connector_id}/ingest`
- TTN live validation preflight
- SmartSentinel snapshot ingestion
- selected adapter operational reads

Those paths expose operational state or trigger side effects. They need token-mode protection without broadening enforcement to the entire API, and without changing the current local-development default of `AIONCORE_AUTH_MODE=dev`.

## Decision

Extend token-mode route enforcement to the following endpoint groups:

- connector administration:
  - `POST /ingestion/connectors`
  - `PATCH /ingestion/connectors/{connector_id}`
  - `PUT /ingestion/connectors/{connector_id}/enable`
  - `PUT /ingestion/connectors/{connector_id}/disable`
  - `POST /ingestion/workers/reconcile`
  - `POST /ingestion/connectors/{connector_id}/ttn-live-validate`
  - required scope: `connectors:admin`
- selected connector and worker operational reads:
  - `GET /ingestion/connectors`
  - `GET /ingestion/connectors/{connector_id}`
  - `GET /ingestion/connectors/{connector_id}/status`
  - `GET /ingestion/connectors/{connector_id}/validate`
  - `GET /ingestion/connectors/{connector_id}/ttn-live-readiness-plan`
  - `GET /ingestion/workers/plan`
  - `GET /ingestion/workers/status`
  - required scope: `connectors:read`
- connector-aware ingestion:
  - `POST /ingestion/connectors/{connector_id}/ingest`
  - required scope: `ingestion:write`
- SmartSentinel snapshot ingestion:
  - `POST /integrations/smartsentinel/snapshots`
  - required scope: `smartsentinel:ingest`
- selected adapter operational reads:
  - `GET /adapters`
  - `GET /adapters/{adapter_id}/status`
  - required scope: `adapters:read`

Scope matching remains exact except that `admin:all` satisfies any route scope check.

Mode behavior remains:

- `dev`: default, requests still pass with development bypass
- `disabled`: requests still pass with auth disabled context
- `token`: missing or invalid bearer token returns `401`; valid token missing required scope returns `403`

## Consequences

Positive:

- connector lifecycle and operator workflows are no longer publicly writable in token mode
- connector-aware ingestion now has a narrow machine-write scope without forcing protection onto generic `/ingest/http`
- TTN live validation preflight is no longer anonymously callable in token mode
- SmartSentinel snapshot ingestion now has a dedicated machine-ingest scope
- selected connector and adapter operational reads are no longer anonymously exposed in token mode

Tradeoffs:

- enforcement is still intentionally partial; many user-facing and generic API routes remain open in token mode
- connector TTN device-mapping routes are still outside the protected set in this milestone
- generic `POST /ingest/http` remains intentionally unprotected to avoid broad API protection before a fuller milestone
- this milestone does not add tenant/resource ownership checks beyond current tenant-bound token resolution

## Non-Goals

- broad API-wide authentication
- JWT, OAuth, or OIDC
- user/password login
- tenant/resource ownership enforcement
- exposing raw token values after issuance
- logging token values
- changing the default auth mode away from `dev`
