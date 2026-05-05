# ADR 0054: Tenant/Resource Ownership Skeleton

## Status

Accepted

## Context

Milestones 49 through 56 introduced auth-mode plumbing, API token principals, route scope checks, and broader token-mode protection for selected read surfaces such as entities, observations, commands, actions, rules, policies, capabilities, executors, events, and raw messages.

That work still had a major gap: a valid token with the correct scope could read protected resources even when the resource belonged to another tenant. AionCore needs a first ownership layer, but this milestone must stay narrow:

- keep `AIONCORE_AUTH_MODE=dev` as the default
- keep `dev` and `disabled` bypass behavior unchanged
- avoid a full authorization engine
- avoid cross-tenant sharing
- avoid graph-wide relationship authorization
- avoid broad write-surface changes

## Decision

Add a first tenant/resource ownership skeleton for selected token-mode protected read surfaces.

Implemented rules:

- in `dev` and `disabled` modes, keep the current bypass behavior unchanged
- in `token` mode, `admin:all` bypasses both scope checks and the new selected ownership checks
- otherwise, the principal `tenant_id` must match the resource `tenant_id`
- selected list/query endpoints return only resources from the principal tenant
- selected detail endpoints return `403` for known cross-tenant access

This milestone applies the ownership skeleton to these selected protected read surfaces:

- `/entities`, `/entities/{entity_id}`, `/entities/{entity_id}/context`
- `/observations`
- `/commands`, `/commands/{command_id}`
- `/actions`, `/actions/{action_id}`, `/action-results`
- `/rules`, `/rules/{rule_id}`
- `/policies`
- `/entities/{entity_id}/capabilities`
- `/executors`, `/executors/{executor_id}`, `/executors/{executor_id}/capabilities`, `/executors/{executor_id}/scopes`
- `/events`, `/events/{event_id}`
- `/raw-messages`, `/raw-messages/{raw_message_id}`

For entity context, relationships are filtered so inconsistent cross-tenant references do not leak through the response.

## Consequences

Positive:

- selected protected token-mode reads now enforce a real tenant boundary rather than only a scope boundary
- `admin:all` remains an explicit break-glass path
- dev-mode compatibility stays intact
- the implementation remains incremental and does not force a full authorization redesign yet

Negative:

- this is not full authorization
- write paths are still largely outside tenant-aware token authorization
- cross-tenant sharing remains unsupported
- relationship-based authorization remains future work
- some route families still need ownership coverage later

## Alternatives Considered

Return `404` for all cross-tenant detail reads:

- rejected for this milestone because the runtime can already distinguish between "not found" and "known cross-tenant resource" in the selected protected read paths, and a structured `403` is more explicit during this staged hardening phase

Add a full authorization engine now:

- rejected because it would widen the milestone significantly, increase churn, and force decisions about writes, role models, and graph relationships that are not yet stable

Restrict ownership enforcement to entities and observations only:

- rejected because the remaining selected read surfaces expose meaningful control-plane and provenance data, and the additional incremental coverage was low enough risk to include now
