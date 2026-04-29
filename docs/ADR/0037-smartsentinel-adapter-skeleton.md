# ADR 0037: SmartSentinel Adapter Skeleton

## Status

Accepted

## Context

SmartSentinel can provide operational snapshots containing hosts, containers, processes, services, endpoints, dependencies, events, and action results. AionCore should be able to preserve those snapshots and selectively materialize useful semantic context without making SmartSentinel mandatory or turning AionCore into a SmartSentinel agent runtime.

The integration must preserve AionCore's domain-agnostic core model. Operational data from SmartSentinel is treated as optional integration data that can enrich entities, relationships, observations, events, and AI context.

## Decision

Add an optional SmartSentinel snapshot ingestion endpoint:

```text
POST /integrations/smartsentinel/snapshots
```

The endpoint stores the original request as a raw message with:

```text
payload_format = smartsentinel-snapshot-json
```

After raw preservation, the adapter maps the simplified snapshot shape into:

- AionCore JSON-LD entities using stable keys under `smartsentinel:{node_id}:{snapshot_entity_id}`.
- AionCore relationships for snapshot relationships.
- AionCore observations for explicit snapshot observations and entity status fields.
- AionCore events for snapshot events.

The endpoint records lifecycle events:

- `aion:SmartSentinelSnapshotReceived`
- `aion:SmartSentinelSnapshotMapped`
- `aion:SmartSentinelSnapshotMappingFailed`

No recovery actions are executed. No polling, authentication, dashboard, Cassandra, production MCP transport, or external AI calls are introduced.

## Consequences

Positive:

- AionCore can ingest SmartSentinel/DATUM-like operational snapshots without a hard dependency.
- Raw snapshot preservation remains the first step.
- Operational context can appear in normal entity, relationship, observation, event, and AI-context queries.
- The adapter can evolve without changing storage backends or core domain crates.

Negative:

- Relationship de-duplication and graph reconciliation are deferred.
- Entity updates are conservative; existing entities are reused and missing entities are not deleted.
- Authentication and live polling must be designed separately before production exposure.

## Current Limitations

- The snapshot schema is intentionally simplified.
- The adapter is HTTP-only.
- Missing entities in later snapshots are not deleted.
- No SmartSentinel agent runtime or executor behavior is implemented.
