# ADR 0041: SmartSentinel Command Executor Bridge

## Status

Accepted

## Context

SmartSentinel snapshots can already be ingested into AionCore and mapped into entities, relationships, observations, events, provenance, and evidence metadata.

AionCore also has a generic ExecutorAgent model with capabilities, scopes, polling, command claiming, leases, approval checks, and action result reporting. SmartSentinel-like agents need ergonomic HTTP shapes for this lifecycle without making SmartSentinel a required dependency or allowing AionCore to execute operational recovery actions.

## Decision

Add optional SmartSentinel bridge endpoints:

- `POST /integrations/smartsentinel/executors/register`
- `GET /integrations/smartsentinel/executors/{executor_id}/commands`
- `POST /integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/claim`
- `POST /integrations/smartsentinel/executors/{executor_id}/commands/{command_id}/report`

Registration creates or reuses an ExecutorAgent with `agent_type = smartsentinel`, declares capabilities such as `sentinel:RunDiagnostic` or `sentinel:RestartService`, and stores generic executor scopes over target entity, entity type, or relationship type.

Polling and claiming reuse the existing executor compatibility, command approval, and lease semantics. Reporting creates Action and ActionResult records, marks the command executed or failed through the existing command lifecycle, updates the active lease, and emits `aion:SmartSentinelCommandReported`.

Report provenance and evidence fields are preserved in action result and event metadata.

## Consequences

- SmartSentinel remains optional.
- SmartSentinel-like agents get integration-specific ergonomics without a separate lifecycle model.
- AionCore can audit command attempts and results tied to SmartSentinel provenance.
- Generic ExecutorAgent endpoints remain unchanged.
- Commands that require approval cannot be claimed through the bridge until approved.
- AionCore still does not execute recovery actions; it only records external executor reports.

## Non-Goals

- No SmartSentinel runtime.
- No host command execution.
- No Docker, systemctl, kubectl, or recovery tool calls.
- No authentication changes.
- No dashboard.
- No Cassandra adapter.
- No production MCP transport.
- No external AI calls.
- No evidence URI fetching.
