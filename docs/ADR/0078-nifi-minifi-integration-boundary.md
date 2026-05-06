# ADR 0078: NiFi/MiNiFi Integration Boundary

## Status

Accepted

## Context

Milestone 80 added historical time-series query APIs.
Milestone 81 added read-only dashboard API foundations.
Milestone 82 added a storage-only flow and pipeline model foundation.

AionCore now needs a clear position on reliable external flow runtimes such as Apache NiFi and MiNiFi.

The platform direction is:

- AionCore remains the semantic IoT core.
- external runtimes may handle buffering, replay, provenance, and transport operations
- NiFi compatibility is desirable for larger or disconnected deployments
- embedding NiFi inside AionCore would over-couple the platform and conflict with the modular, pluggable architecture direction

## Decision

AionCore will treat NiFi and MiNiFi as optional external reliable flow layers rather than required runtime dependencies.

Milestone 83 therefore:

- documents the AionCore to NiFi/MiNiFi integration boundary
- defines a recommended reliable-ingestion envelope convention
- defines consistent external provenance metadata keys
- documents deployment patterns for standalone, edge, fog, and larger Kafka-backed topologies
- keeps runtime ingestion and flow behavior unchanged

AionCore will not in this milestone:

- add a NiFi dependency
- embed a NiFi runtime
- implement a NiFi client
- implement network calls to NiFi
- change existing ingestion behavior
- change existing flow execution behavior
- implement DLQ runtime or batch/backfill runtime

## Consequences

### Positive

- AionCore stays decoupled from one flow engine.
- Operators can use NiFi or MiNiFi where reliable transport and provenance are needed.
- SmartSentinel, future edge adapters, and custom collectors can follow the same metadata conventions.
- Future DLQ, replay, idempotency, and dashboard work can build on a stable documented contract.

### Negative

- No runtime convenience exists yet for automatic NiFi envelope interpretation.
- No typed API contract is enforced by the current ingest handlers.
- Provenance interoperability is convention-based until later milestones add runtime adoption.

### Neutral

- Existing generic metadata surfaces already provide a place to preserve the conventions later.
- NiFi flow definitions remain external; AionCore flows may only reference them through metadata.

## Alternatives Considered

### Embed NiFi Or Make It A Required Dependency

Rejected because it would:

- increase operational complexity
- over-constrain deployments
- weaken the modular-monolith-first direction
- blur the boundary between transport orchestration and semantic core responsibilities

### Ignore NiFi Compatibility Entirely

Rejected because it would:

- leave larger and disconnected deployments without a documented interoperability path
- make future replay, DLQ, and provenance features less coherent
- create avoidable divergence between SmartSentinel, future edge adapter, and external flow-engine conventions

### Add Runtime NiFi Client Support Now

Rejected because this milestone is intentionally low-risk and mostly documentation-focused. Runtime integration can follow once envelope, provenance, and security conventions stabilize.

## Follow-Up

Expected next related milestones:

- DLQ model and API foundation
- reliable ingestion envelope and idempotency-key handling
- batch and backfill ingestion API
- flow validation and dry-run API
- dashboard UI foundations for flows, connectors, and reliability state
