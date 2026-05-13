# ADR 0097: Safe Internal Flow Side Effects

## Status

Accepted.

## Context

Milestones 98 through 101 introduced simulated flow execution and an explicit side-effect authorization model. The execution engine could interpret nodes and report previews, but it still performed no writes even for internal sinks such as `internal_observation_store` and `event_create`.

Before enabling external side effects such as MQTT publish or HTTP forward, AionCore needs a smaller and auditable step: allow only selected internal side effects when an operator explicitly requests them and the principal has `flows:execute`.

## Decision

AionCore now supports a safe internal side-effect subset during flow execution:

- `internal_observation_store` may persist observations.
- `event_create` may persist events.

This is only allowed when:

- the request declares side-effect intent using `allow_side_effects=true` or `requested_sink_actions`, and
- token mode authorization grants `flows:execute` or `admin:all`.

Execution remains explicit and opt-in through `POST /flows/execute` or `POST /flows/{flow_id}/execute`. Flow enablement does not start runtime execution.

The execution response continues to expose `simulated=true` because broker/source runtime execution is still not active, but `side_effects_performed` may become `true` when an allowed internal sink writes observations or events. Per-sink `side_effect_performed` also records the exact sink that produced a write.

## Scope

Supported real internal actions:

- `store_observation`
- `create_event`

Still preview-only:

- `raw_message_store`
- `mqtt_publish`
- `http_forward`
- `command_create`
- `dlq`

## Consequences

This creates a gradual migration path from simulated execution to real execution without enabling external network side effects. It also gives the dashboard and API users a way to validate authorization, audit behavior, and persisted internal outputs before MQTT/HTTP delivery or DLQ replay automation exist.

## Non-goals

This ADR does not add:

- MQTT publish execution.
- HTTP forward execution.
- command creation execution.
- DLQ writes.
- broker subscriptions.
- flow workers.
- automatic execution for enabled flows.
