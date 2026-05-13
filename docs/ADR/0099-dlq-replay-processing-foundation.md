# ADR 0099: DLQ Replay Processing Foundation

## Status

Accepted.

## Context

AionCore now has a first-class DLQ model, reliable single-message ingestion, batch/backfill ingestion, and guarded flow execution. Field deployments may buffer data during disconnections and later require operators to decide whether a DLQ record should be replayed through reliable ingestion, evaluated by a flow, or handled manually.

The platform needs a replay processing boundary before adding automated replay workers. The boundary must preserve provenance, tenant isolation, and operator intent while avoiding accidental side effects.

## Decision

AionCore adds two DLQ replay endpoints:

- `POST /dlq/records/{record_id}/replay-plan`
- `POST /dlq/records/{record_id}/replay`

The plan endpoint returns an eligibility report, blockers, warnings, suggested target, redacted payload preview, and external provenance metadata. It requires `dlq:read` in token mode.

The replay endpoint records replay intent by moving an eligible record to `replay_requested`. It requires `dlq:write` in token mode. It does not reingest payloads, execute flows, publish MQTT messages, forward HTTP requests, create commands, write observations, or create DLQ records.

Supported replay targets are:

- `reliable_ingestion`
- `flow_execution`
- `manual_review`

When no target is provided, AionCore chooses a conservative default from the DLQ record: `flow_execution` if `flow_id` is present, `reliable_ingestion` if a payload is present, otherwise `manual_review`.

## Consequences

This gives the dashboard, operators, NiFi/MiNiFi integrations, and future replay workers a stable replay-planning contract without enabling automatic replay. It also keeps replay intent auditable through existing DLQ status and event behavior.

Future milestones can implement actual replay workers on top of this contract.

## Non-goals

- No automatic replay execution.
- No reliable ingestion replay worker.
- No flow execution replay worker.
- No MQTT or HTTP side effects.
- No command creation.
- No automatic DLQ routing.
- No batch sync-session persistence.
