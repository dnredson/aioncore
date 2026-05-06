# ADR 0081: Batch And Backfill Reliable Ingestion API

## Status

Accepted

## Context

Milestone 83 documented the NiFi and MiNiFi integration boundary and provenance conventions.
Milestone 84 added a DLQ model and API foundation.
Milestone 85 added `ReliableIngestionEnvelope`, `POST /ingest/reliable`, tenant-scoped idempotency lookup, and replay-safe duplicate responses.

The remaining gap was reconnect and backfill handling for disconnected deployments:

- SmartSentinel or Aion Edge Adapter may buffer many records locally
- MiNiFi or NiFi may replay many envelopes after a WAN outage
- operators need one request that can submit multiple reliable items without changing the existing single-item endpoints

## Decision

Add an additive `POST /ingest/batch` API that accepts multiple reliable-ingestion items in one request.

Milestone 86 includes:

- batch-level request metadata such as `batch_id`, `sync_session_id`, `source_system`, `source_id`, `connectivity_state`, and optional external flow references
- per-item reuse of the existing reliable-ingestion envelope plus the current required semantic targeting fields
- sequential independent processing of items
- tenant-scoped per-item idempotency using the existing raw-message lookup foundation
- per-item result reporting as `accepted`, `duplicate`, or `failed`
- default `continue_on_error=true` with optional stop-on-first-failure behavior
- `aion:ReliableBatchIngested` batch-level audit events
- token-mode `batches:write` protection

The milestone intentionally does not add:

- replay execution
- automatic DLQ routing
- flow execution
- connector-aware reliable batching
- a persistent batch session table
- any behavior change to `POST /ingest/http` or `POST /ingest/reliable`

## Consequences

Positive:

- disconnected and store-and-forward deployments can submit backlog safely through one additive API
- NiFi, MiNiFi, SmartSentinel, and future edge adapters can preserve provenance consistently across batch reconnect scenarios
- duplicate handling remains replay-safe and tenant-scoped
- later replay, DLQ-routing, and dashboard work can build on stable batch metadata and audit events

Tradeoffs:

- batch session state is preserved only in raw-message and event metadata for now
- failed items do not create DLQ records automatically
- partial failure semantics are explicit and client-visible instead of being hidden inside a global transaction

## Rejected Alternatives

### Change Existing `POST /ingest/reliable`

Rejected because the single-item reliable API already exists and must remain stable for current clients.

### Add A Global Transaction For The Full Batch

Rejected because replay and reconnect workloads often contain mixed valid, duplicate, and invalid items. Independent per-item progress is more operationally useful and lower risk.

### Add Replay Or Automatic DLQ Routing In The Same Milestone

Rejected because those behaviors introduce runtime policy and execution concerns that deserve separate rollout and validation.
