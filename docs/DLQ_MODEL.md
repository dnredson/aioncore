# DLQ Model

Milestone 84 adds the first Dead Letter Queue model and API foundation for AionCore.

## Purpose

The DLQ model preserves records that could not be processed normally, or that operators and trusted machine integrations want to quarantine explicitly for later inspection, replay planning, or audit.

This foundation is aimed at disconnected IoT and store-and-forward deployments where:

- edge or fog systems may buffer data locally
- delayed or replayed data may arrive after reconnection
- external reliable flow engines such as NiFi or MiNiFi may keep retry and provenance state outside AionCore
- operators still need a stable tenant-scoped record of what failed, why it failed, and what external evidence existed at the time

## Current Scope

This milestone adds:

- a tenant-scoped `DlqRecord` model
- in-memory and PostgreSQL storage support
- explicit create, list, detail, and status-update APIs
- token-mode `dlq:read` and `dlq:write` scopes
- tenant-aware list, detail, and update behavior
- dashboard overview DLQ counts
- DLQ lifecycle audit events

This milestone does not add:

- automatic DLQ routing from ingestion
- flow-driven DLQ execution
- replay execution
- retry execution
- batch or backfill ingestion
- idempotency enforcement

Milestone 85 now adds reliable-ingestion runtime idempotency handling for `POST /ingest/reliable`, but it still does not add automatic routing from ingestion failures into `DlqRecord`.

Milestone 86 now adds `POST /ingest/batch` for reliable reconnect and backfill submission, but batch item failures still do not create `DlqRecord` automatically.

## Core Type

`DlqRecord`

- `id`
- `tenant_id`
- `dlq_key`
- `source_system`
- `source_id`
- `connector_id`
- `flow_id`
- `raw_message_id`
- `event_id`
- `command_id`
- `idempotency_key`
- `external_flow_id`
- `external_flow_name`
- `external_flowfile_uuid`
- `external_process_group_id`
- `external_processor_id`
- `external_provenance_uri`
- `sync_session_id`
- `payload_format`
- `payload`
- `payload_hash`
- `failure_stage`
- `failure_reason`
- `failure_detail`
- `retry_count`
- `replay_count`
- `status`
- `metadata`
- `created_at`
- `updated_at`
- `resolved_at`

## Failure Stages

`DlqFailureStage`

- `ingestion`
- `decoding`
- `validation`
- `mapping`
- `rule_evaluation`
- `flow_processing`
- `sink_delivery`
- `command_creation`
- `unknown`

These values are intentionally serde-friendly and extensible so future runtimes can reuse the same API and storage contract without breaking callers.

## Status Values

`DlqStatus`

- `pending`
- `inspecting`
- `resolved`
- `ignored`
- `replay_requested`
- `failed_replay`

`replay_requested` is only an operator or integration marker in this milestone. It does not execute replay.

## Relationship To Existing AionCore Models

- `RawMessage` remains the raw ingress record and is still stored first in normal ingestion flows.
- `Observation` remains the canonical telemetry record.
- `Event` remains the audit and provenance timeline surface.
- `Flow` remains the operator graph model and may reference DLQ paths conceptually or through metadata.
- `IngestionConnector` remains the current connector/runtime configuration boundary.

`DlqRecord` does not replace any of these models. It captures a failure, quarantine, or replay-planning record that may reference them.

## NiFi And MiNiFi Provenance Compatibility

The DLQ model includes typed fields for the provenance contract documented in Milestone 83:

- `source_system`
- `external_flow_id`
- `external_flow_name`
- `external_flowfile_uuid`
- `external_process_group_id`
- `external_processor_id`
- `external_provenance_uri`
- `idempotency_key`
- `sync_session_id`
- `retry_count`
- `replay_count`
- `payload_hash`

These fields are preserved as evidence and correlation material. They are not treated as trusted proof by themselves.

Reliable ingestion failures now preserve the same upstream provenance and idempotency context in `RawMessage.headers` and `Event.metadata` so a later routing milestone can create `DlqRecord` instances without losing upstream evidence.

## Why Automatic Routing Is Deferred

Automatic routing from ingestion, normalization, rules, or flows would change operational behavior and risk coupling this milestone to runtime policy decisions.

That work is intentionally deferred so the platform can first establish:

- a stable DLQ storage contract
- safe operator and machine APIs
- audit visibility
- tenant-aware access control

Later milestones can layer automatic routing, replay, or backfill execution on top of this foundation without redesigning the record format.

## Future Direction

This DLQ foundation is intended to support later work such as:

- automatic ingestion-to-DLQ routing
- replay request queues and replay workers
- batch and backfill sessions
- batch-to-DLQ automatic failure handoff
- dashboard DLQ inspection views
- idempotency-aware reprocessing
- external provenance drill-through for NiFi or MiNiFi
