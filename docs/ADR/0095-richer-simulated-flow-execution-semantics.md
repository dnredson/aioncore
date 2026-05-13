# ADR 0095: Richer Simulated Flow Execution Semantics

## Status

Accepted.

## Context

AionCore already supports simulated flow execution through `POST /flows/execute` and `POST /flows/{flow_id}/execute`. The first execution foundation intentionally avoided side effects and focused on safe preview behavior. After adding dashboard support for simulated execution, operators need more explanatory output before real execution is introduced: mapping previews should be clearer, rule conditions should support common compositions, and branches should show why paths were followed or skipped.

## Decision

AionCore extends simulated execution with richer semantics while keeping the execution boundary side-effect-free:

- execution responses now include `edge_results`;
- edge metadata may carry `condition`, `when`, or `filter` objects;
- conditions support `all`, `any`, `not`, `between`, `in`, `not_exists`, and `missing` in addition to existing simple operators;
- `json_map` supports nested target paths, source path objects, defaults, literal values, and simple templates;
- branch traversal is reported as `traversed`, `skipped`, or `failed` without executing external sinks.

## Consequences

Flow previews become more useful for the dashboard and for future Node-RED-like flow authoring. Operators can see both node-level and edge-level simulated behavior before enabling real execution.

This decision does not add MQTT publish, HTTP forwarding, command creation, event persistence, observation persistence, DLQ writes, broker subscriptions, or worker integration. Those remain future milestones and must be separately authorized.

## Non-goals

- arbitrary scripting;
- full expression language;
- real side effects;
- runtime source binding;
- persisted transformed payloads;
- automatic DLQ routing or replay.
