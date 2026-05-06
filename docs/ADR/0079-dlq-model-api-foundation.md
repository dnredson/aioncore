# ADR 0079: DLQ Model And API Foundation

## Status

Accepted

## Context

AionCore already has raw messages, observations, events, connectors, flows, and NiFi/MiNiFi provenance conventions, but it had no first-class DLQ record for:

- failed decoding
- validation rejection
- mapping failure
- upstream quarantine or replay planning
- future store-and-forward catch-up workflows

Milestone 83 documented the external reliable-ingestion contract, including provenance, replay, retry, and sync-session metadata, but left runtime DLQ behavior intentionally open.

## Decision

Add a first-class tenant-scoped `DlqRecord` model plus explicit API and storage support now, without changing ingestion or flow execution behavior.

The milestone includes:

- new `aion-dlq` crate with typed enums and record model
- in-memory and PostgreSQL persistence
- list filtering for core operational dimensions
- token-mode `dlq:read` and `dlq:write` scopes
- tenant-aware list, detail, and status update behavior
- dashboard overview DLQ counts
- lifecycle audit events

The milestone intentionally excludes:

- automatic DLQ routing
- replay execution
- retry execution
- flow execution
- batch or backfill runtime

## Rationale

This keeps the first DLQ milestone storage-first and low-risk:

- operators and trusted machine clients can create and manage explicit DLQ records now
- provenance and evidence fields from NiFi or MiNiFi are preserved consistently
- future replay, backfill, and automatic-routing work can build on a stable contract
- current ingestion and flow behavior stays unchanged

## Consequences

Positive:

- AionCore now has an explicit place to preserve failed or quarantined records
- dashboard and audit surfaces can expose DLQ volume and lifecycle
- external reliable-ingestion systems can hand off provenance-rich DLQ material without waiting for replay execution

Tradeoffs:

- the platform still requires future work to route failures automatically
- replay intent is stored only as status, not as executable work
- delete semantics are intentionally omitted for now to preserve auditability and avoid premature retention-policy design
