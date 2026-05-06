# NiFi/MiNiFi Integration Model

Milestone 83 defines how AionCore interoperates with Apache NiFi and MiNiFi without making either runtime a required dependency.

## Boundary

- AionCore does not depend on NiFi or MiNiFi.
- AionCore does not embed NiFi.
- AionCore does not implement a NiFi client in this milestone.
- AionCore does not change existing HTTP or MQTT ingestion behavior in this milestone.
- NiFi and MiNiFi are optional reliable flow layers that can sit before AionCore ingestion.

AionCore remains the semantic and IoT core platform:

- JSON-LD entities and relationships
- raw-message-first ingestion
- canonical observations
- events, commands, actions, and policies
- dashboard and MCP/AI context
- connector, adapter, and flow inventory

NiFi and MiNiFi remain external operational runtimes for:

- buffering
- routing
- retry
- backpressure
- replay
- provenance
- source-specific transforms outside the AionCore core runtime

## Recommended Deployment Patterns

### AionCore Standalone

Use direct HTTP ingestion or AionCore-managed MQTT ingestion when the deployment is simple and local buffering is not required.

### AionCore Plus Aion Edge Adapter

Use the future Aion Edge Adapter when local protocol support, local parsing, or site-local buffering is needed without adopting NiFi.

### AionCore Plus MiNiFi At The Edge

Use MiNiFi agents near devices, gateways, or local brokers when the site needs:

- store-and-forward during WAN or 4G loss
- local queueing and retry
- lightweight routing and transformation
- compact operational provenance

MiNiFi can forward payloads to AionCore HTTP ingestion directly or through an intermediate fog/cloud runtime.

### AionCore Plus NiFi At Fog Or Cloud

Use NiFi in a regional, plant, campus, or cloud layer when the deployment needs:

- route fan-in from multiple edge sites
- centralized retry and replay
- controlled transformation pipelines
- operational provenance and operator-managed replay

### AionCore Plus NiFi And Kafka

Use NiFi with Kafka or another durable event backbone for larger deployments where:

- buffering spans many sites
- replay and retention windows are longer
- ingestion fan-out and decoupling matter
- AionCore remains the semantic destination rather than the transport backbone

Kafka remains optional and external. AionCore does not require it.

## Mapping NiFi Concepts To AionCore

### NiFi FlowFile To RawMessage

A NiFi or MiNiFi FlowFile maps most directly to one inbound AionCore `RawMessage`.

Recommended interpretation:

- FlowFile content becomes the raw payload sent to AionCore.
- FlowFile attributes become AionCore raw-message headers or metadata when forwarded.
- FlowFile receive, queue, and route history remain external runtime provenance, but selected identifiers should be preserved inside AionCore metadata.

### NiFi Provenance To RawMessage And Event Metadata

NiFi provenance is not reimplemented by AionCore. Instead, AionCore should preserve stable external references such as:

- source runtime identity
- flow and processor identifiers
- FlowFile UUID
- replay and retry counters
- provenance URI or opaque lookup reference

These references should live in `RawMessage.headers`, `Event.metadata`, and where useful `Observation.metadata`.

### NiFi Flow Or Process Group To Flow

NiFi flow definitions remain external.

AionCore `Flow` records may reference external NiFi structures through metadata such as:

- external flow ID
- external flow name
- process group ID
- source system type

AionCore flows model operator intent and semantic integration boundaries. They do not execute NiFi graphs.

### NiFi Retry, Replay, And Backpressure To Future AionCore Reliability Work

NiFi queueing and replay remain upstream runtime concerns.

AionCore should preserve enough metadata to interoperate with future AionCore features:

- tenant-scoped idempotency keys
- replay counters
- retry counters
- sync or batch session identifiers
- DLQ handoff metadata
- backfill session markers

This milestone defines only the contract and metadata conventions. It does not implement runtime DLQ, replay, or batch ingestion logic.

## SmartSentinel And Store-And-Forward

SmartSentinel-style field deployments may already have operational logic for local buffering and intermittent connectivity.

When 4G or WAN connectivity is lost:

- SmartSentinel or another field component may retain data locally.
- MiNiFi may provide local queueing and later synchronized delivery.
- NiFi may orchestrate larger replay or catch-up sessions upstream.
- AionCore should receive the delayed payloads without pretending they were real-time arrivals.

Recommended preserved fields for these cases include:

- `stored_at_edge`
- `sent_at`
- `observed_at`
- `connectivity_state`
- `sync_session_id`
- `edge_sequence`
- `replay_count`
- `retry_count`

## Delayed And Backfilled Data

Delayed or replayed data should preserve at least three distinct time concepts when available:

- `observed_at`: when the sensor or source claims the measurement was observed
- `stored_at_edge`: when a field runtime buffered the payload locally
- `sent_at`: when the payload left the edge or external flow runtime toward AionCore

AionCore `RawMessage.received_at` remains the AionCore ingest time and should not be rewritten to hide transport delay.

Recommended interpretation:

- `RawMessage.received_at` is transport arrival at AionCore.
- `Observation.observed_at` should continue to represent source observation time when trustworthy and available.
- external queueing and replay timestamps belong in metadata, not core timestamp replacement.

Backfill sessions should also preserve:

- `external.sync_session_id`
- `external.replay_count`
- `external.idempotency_key`
- optional `external.provenance_uri`

## Late Data, Rules, And Commands

Late data can affect future rule and command behavior even when no runtime change is implemented yet.

Documented expectations:

- AionCore should preserve whether data arrived late or through replay.
- Future rules should be able to distinguish real-time ingestion from replay or backfill.
- Future command automation should avoid treating delayed historical data as a fresh operational trigger by default.
- Replay and backfill metadata are evidence for later policy decisions, not an instruction to execute actions now.

This milestone does not change rule evaluation or command behavior.

## Recommended Envelope Convention

NiFi, MiNiFi, Aion Edge Adapter, SmartSentinel, or custom reliable upstream producers should prefer an AionCore-compatible envelope when they want provenance and replay semantics preserved consistently.

### Classification

Required fields:

- `source_system`
- `payload_format`
- `payload`

Recommended fields:

- `idempotency_key`
- `source_id`
- `observed_at`
- `sent_at`
- `payload_hash`

Optional fields:

- `external_flow_id`
- `external_flow_name`
- `external_process_group_id`
- `external_processor_id`
- `flowfile_uuid`
- `edge_sequence`
- `sync_session_id`
- `stored_at_edge`
- `replay_count`
- `retry_count`
- `connectivity_state`
- `provenance`

### Field Semantics

- `source_system`: one of `nifi`, `minifi`, `aion-edge-adapter`, `smartsentinel`, or `custom`
- `external_flow_id`: stable ID for an external NiFi or equivalent flow
- `external_flow_name`: operator-facing name for the external flow
- `external_process_group_id`: NiFi process group ID when applicable
- `external_processor_id`: NiFi processor ID or equivalent runtime stage ID
- `flowfile_uuid`: NiFi FlowFile UUID when applicable
- `idempotency_key`: tenant-scoped deduplication key generated by the external sender
- `source_id`: external source identity such as adapter ID, site ID, broker source, or device grouping
- `edge_sequence`: monotonic sender-local ordering hint
- `sync_session_id`: identifier for a replay, sync, or backfill session
- `observed_at`: source observation time
- `stored_at_edge`: local buffering timestamp
- `sent_at`: timestamp when the envelope was forwarded to AionCore
- `payload_format`: decoder hint for the enclosed payload
- `payload`: the original payload content
- `payload_hash`: stable content hash for deduplication or audit support
- `replay_count`: how many times the record has been replayed
- `retry_count`: how many delivery retries occurred before success
- `connectivity_state`: optional hint such as `online`, `degraded`, `offline_buffered`, or `replayed_after_outage`
- `provenance`: nested free-form object for external runtime evidence that does not fit the core fields

### Example Envelope

```json
{
  "source_system": "minifi",
  "external_flow_id": "flow-edge-sync",
  "external_flow_name": "Edge Telemetry Sync",
  "external_process_group_id": "pg-plant-01",
  "external_processor_id": "proc-http-post",
  "flowfile_uuid": "2f8d0c30-7cf6-4d58-b9ac-4e53c9ad3a72",
  "idempotency_key": "tenant-a:plant-01:pump-07:2026-05-06T12:00:00Z:41",
  "source_id": "plant-01-edge-01",
  "edge_sequence": 41,
  "sync_session_id": "sync-2026-05-06-outage-recovery-01",
  "observed_at": "2026-05-06T12:00:00Z",
  "stored_at_edge": "2026-05-06T12:00:05Z",
  "sent_at": "2026-05-06T12:10:11Z",
  "payload_format": "senml-json",
  "payload": [
    {
      "bn": "pump-07",
      "n": "temperature",
      "u": "Cel",
      "v": 21.4,
      "t": 1746532800
    }
  ],
  "payload_hash": "sha256:5ce3b778...",
  "replay_count": 1,
  "retry_count": 3,
  "connectivity_state": "replayed_after_outage",
  "provenance": {
    "provenance_uri": "nifi://provenance/events/123456",
    "queue_name": "wan-retry",
    "route": "edge-http->cloud-sync->aioncore"
  }
}
```

## Provenance Metadata Conventions

When AionCore receives data from NiFi, MiNiFi, SmartSentinel, an edge adapter, or another reliable upstream, the following metadata keys are recommended.

### Common Keys

- `external.source_system`
- `external.flow_id`
- `external.flow_name`
- `external.flowfile_uuid`
- `external.process_group_id`
- `external.processor_id`
- `external.provenance_uri`
- `external.replay_count`
- `external.retry_count`
- `external.idempotency_key`
- `external.sync_session_id`
- `external.source_id`
- `external.edge_sequence`
- `external.stored_at_edge`
- `external.sent_at`
- `external.connectivity_state`
- `external.payload_hash`

### Placement

`RawMessage.headers` or equivalent metadata:

- primary storage for external provenance received at ingest time
- should preserve the upstream envelope as far as practical without changing core fields

`Event.metadata`:

- should preserve identifiers relevant to ingestion success, failure, replay, validation, or future DLQ workflows

`Observation.metadata`:

- should preserve selected external provenance when it helps operators interpret delayed or replayed observations
- avoid copying large opaque provenance blobs if lightweight references are enough

`Flow.metadata`:

- may carry external flow references for documentation and dashboard linking

Future DLQ records:

- should reuse the same `external.*` keys so replay paths stay explainable

Milestone 84 now adds a typed DLQ record foundation that preserves these fields directly, but it still does not add automatic routing from ingestion into DLQ records.

Future batch or backfill session records:

- should use `external.sync_session_id`, `external.idempotency_key`, and replay counters consistently

## Trust And Validation Notes

- External provenance is evidence, not proof.
- AionCore should not trust upstream provenance blindly.
- Tenant ownership, auth context, and future policy checks remain authoritative inside AionCore.
- `idempotency_key` values must be tenant-scoped.
- External systems may send partial or misleading timestamps; AionCore should preserve them as metadata rather than assuming correctness.

## Runtime Status

Milestones 83 and 84 together now provide the contract plus the first DLQ storage/API foundation for provenance-rich failures and replay planning.

The existing generic `RawMessage.headers`, `Event.metadata`, `Observation.metadata`, `Flow.metadata`, and now `DlqRecord` fields are flexible enough to carry the documented conventions without changing current ingestion runtime behavior.

Future milestones may add typed envelope or provenance structs once runtime adoption begins.
