# ADR 0038: SmartSentinel Snapshot Reconciliation Hardening

## Status

Accepted

## Context

ADR 0037 introduced the optional SmartSentinel snapshot adapter skeleton. The first skeleton preserved raw snapshots and materialized selected snapshot data into entities, relationships, observations, and events, but it did not de-duplicate relationships, explicitly update existing entities, or return structured diagnostics.

Repeated operational snapshots are normal for SmartSentinel-style integrations. AionCore needs deterministic mapping behavior so repeated snapshots do not grow duplicate graph edges and so operators can understand why a snapshot did or did not map.

## Decision

Keep SmartSentinel optional and harden the existing snapshot endpoint only:

```text
POST /integrations/smartsentinel/snapshots
```

Entity reconciliation uses stable keys:

```text
smartsentinel:{node_id}:{snapshot_entity_id}
```

For existing entities, AionCore preserves `id` and `entity_key`, rebuilds the SmartSentinel JSON-LD projection from the latest snapshot, updates mutable operational fields, and refreshes `updated_at` only when the materialized projection changes. Critical identity fields are not replaced with new identifiers.

Relationship reconciliation de-duplicates by:

- `tenant_id`
- `source_entity_id`
- `relationship_type`
- `target_entity_id`

Existing matching relationships are reused. Self-referential relationships are skipped.

Validation runs after raw-message preservation and before mapping. Fatal validation errors mark the raw message failed, record `aion:SmartSentinelSnapshotMappingFailed` when feasible, return structured validation errors, and avoid partial mapping.

The snapshot response now includes mapping diagnostics:

- `entities_created`
- `entities_updated`
- `entities_reused`
- `entities_skipped`
- `relationships_created`
- `relationships_reused`
- `relationships_skipped`
- `observations_created`
- `events_created`
- `validation_warnings`
- `validation_errors`
- `skipped_items`

## Consequences

Positive:

- Repeated snapshots do not duplicate identical relationships.
- Operators can see whether entities were created, updated, or reused.
- Invalid snapshots preserve raw evidence and return actionable validation details.
- The integration remains loosely coupled to generic AionCore storage interfaces.

Negative:

- Full graph reconciliation remains deferred.
- Relationship de-duplication is exact-match only.
- Existing entity updates are limited to the SmartSentinel JSON-LD projection.

## Non-Goals

- No SmartSentinel agent runtime.
- No live polling.
- No recovery action execution.
- No authentication.
- No dashboard, Cassandra adapter, production MCP transport, or external AI integration.
