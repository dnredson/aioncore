# ADR 0050: Auth Readiness and Bootstrap Hardening

## Status

Accepted

## Context

Milestones 48 through 52 introduced auth-mode parsing, API tokens, and selected token-mode protection for machine-facing, connector, ingestion, and secret-management routes.

`GET /ready` still reported auth state using the original skeleton-era fields:

- `auth.mode`
- `auth.enforced`
- `auth.dev_bypass`

That boolean `auth.enforced` signal no longer describes reality well. In `token` mode AionCore now enforces authentication on selected endpoint groups, but not across the full API.

The bootstrap admin environment token also needed harder startup behavior and clearer operator guidance. A short or casually reused bootstrap token is risky, and readiness must not expose token material.

## Decision

For auth readiness:

- replace the old boolean readiness signal with `auth.enforcement_level`
- use:
  - `none` in `dev`
  - `none` in `disabled`
  - `partial` in `token`
- explicitly report the currently protected endpoint groups in `auth.protected_endpoint_groups`
- report `auth.bootstrap_admin_configured` as a boolean only
- do not claim `full` enforcement yet

The protected endpoint groups reported in `token` mode are:

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

For bootstrap admin hardening:

- require `AIONCORE_BOOTSTRAP_ADMIN_TOKEN` to be at least 24 characters long when set
- fail startup/config parsing with a clear error if it is shorter
- never log or return the bootstrap token value
- emit an explicit startup warning that bootstrap admin is for local bootstrap and development only, should be removed after creating real admin tokens, and must not be exposed publicly

## Consequences

Positive:

- readiness now describes the current auth rollout honestly
- operators can distinguish `none` from `partial` enforcement without inferring behavior from a legacy boolean
- bootstrap admin presence is observable without leaking secrets
- startup fails early for obviously weak bootstrap tokens

Tradeoffs:

- readiness becomes slightly more verbose
- route-group reporting must be kept in sync with future enforcement milestones
- bootstrap admin remains an environment-managed escape hatch rather than a storage-managed credential

## Alternatives Considered

Keep the boolean `auth.enforced` field:

- rejected because `true` would imply more coverage than exists, while `false` would hide the selected token-mode protections that already ship

Claim full enforcement in `token` mode:

- rejected because most user-facing and MCP-related routes remain intentionally open in this stage

Allow short bootstrap tokens but warn only:

- rejected because weak bootstrap credentials should fail fast rather than rely on operator discipline
