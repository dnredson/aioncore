# ADR 0049: Remaining Ingestion and Connector Auth Enforcement

## Status

Accepted

## Context

Milestone 50 introduced selective token-mode enforcement for adapter, executor, SmartSentinel executor bridge, connector secret, and token-management routes.

Milestone 51 extended that coverage to connector administration, selected connector operational reads, connector-aware ingestion, TTN live validation preflight, SmartSentinel snapshot ingestion, and selected adapter operational reads.

After Milestone 51, several machine-facing or connector-operational routes still remained open in `AIONCORE_AUTH_MODE=token`:

- `POST /ingest/http`
- TTN device-mapping administration and reads under `/ingestion/connectors/{connector_id}/ttn-device-mappings`
- `GET /adapters/{adapter_id}`

These routes can create raw messages, observations, and events, or expose connector and adapter operational state. They are narrower and higher-risk than the rest of the still-open API surface, so they should be closed before broader MCP and AI-context protection work.

## Decision

In `token` mode:

- protect `POST /ingest/http` with `ingestion:write`
- protect TTN mapping list and detail reads with `connectors:read`
- protect TTN mapping create, update, enable, disable, and delete with `connectors:admin`
- protect `GET /adapters/{adapter_id}` with `adapters:read`
- keep exact scope matching, except `admin:all` satisfies any route scope requirement
- keep `dev` and `disabled` behavior unchanged

This milestone does not introduce JWT, OAuth, OIDC, login, tenant/resource ownership enforcement, or broad protection across the rest of the API.

## Consequences

Positive:

- generic HTTP ingestion is no longer anonymously writable in `token` mode
- TTN mapping state can no longer be listed or mutated anonymously in `token` mode
- adapter detail reads no longer expose operational machine metadata anonymously in `token` mode
- the scope model stays incremental by reusing `ingestion:write`, `connectors:read`, `connectors:admin`, `adapters:read`, and `admin:all`

Tradeoffs:

- endpoint enforcement is still intentionally partial
- executor catalog/detail/capability/scope reads remain open because a dedicated executor read scope is not yet defined
- tenant/resource ownership checks are still deferred to a later milestone

## Alternatives Considered

Protect all remaining operational reads now:

- rejected because it would force new scope design, especially for executor read APIs, and would widen the milestone beyond the known ingestion and connector gaps

Leave generic `/ingest/http` open until MCP protection:

- rejected because it remains a direct machine-write path that creates stored raw payloads and downstream normalized data
