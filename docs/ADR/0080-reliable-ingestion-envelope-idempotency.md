# ADR 0080: Reliable Ingestion Envelope And Tenant-Scoped Idempotency

## Status

Accepted

## Context

AionCore already preserved raw messages first and already had a documented NiFi/MiNiFi provenance contract plus a DLQ record foundation.

The remaining gap was runtime adoption:

- no standard reliable HTTP envelope
- no tenant-scoped idempotency-key handling
- no replay-safe duplicate response for disconnected or store-and-forward senders

Disconnected IoT deployments need to buffer locally and resend after reconnect without creating duplicate `RawMessage` and `Observation` records.

## Decision

Milestone 85 adds:

- a typed `ReliableIngestionEnvelope` model
- `POST /ingest/reliable`
- tenant-scoped raw-message idempotency lookup support
- a PostgreSQL partial unique index on `(tenant_id, idempotency_key)` when the key is present
- preserved `external.*` provenance metadata in `RawMessage.headers` and `Event.metadata`

The endpoint remains additive. Existing `POST /ingest/http` behavior is unchanged.

## Consequences

Positive:

- replay-safe ingestion now has a stable runtime contract
- NiFi, MiNiFi, SmartSentinel, and future edge adapters can preserve provenance consistently
- deduplication is tenant-scoped instead of globally scoped
- future DLQ, replay, and backfill milestones can build on preserved provenance and idempotency fields

Tradeoffs:

- the current generic reliable endpoint still requires explicit producer and feature IDs
- batch/backfill APIs remain deferred
- replay execution remains deferred
- automatic DLQ routing remains deferred
- connector-aware reliable ingestion remains deferred

## Rejected Alternatives

### Change Existing `POST /ingest/http`

Rejected because Milestone 85 had to stay backward-compatible and not change current client behavior.

### Global Idempotency Keys

Rejected because tenant isolation is a core security and storage boundary. The same external key in different tenants must not collide.

### Automatic Replay Or DLQ Runtime In The Same Milestone

Rejected because it would couple storage and API foundation work to operational behavior changes that deserve separate rollout and validation.
