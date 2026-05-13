# ADR 0096: Flow Execution Side-Effect Authorization

## Status

Accepted.

## Context

AionCore now has a simulated flow execution foundation. The current execution engine interprets stored or proposed flows and returns node, edge, sink, observation, event, command, and DLQ previews. It deliberately does not perform real side effects.

Before real execution is introduced, AionCore needs an explicit authorization boundary for requests that intend to perform side effects such as storing observations, publishing MQTT messages, forwarding HTTP requests, creating events, creating commands, or writing DLQ records.

## Decision

Flow execution requests may now declare side-effect intent through additive request fields:

- `allow_side_effects`
- `requested_sink_actions`
- `operator_reason`
- `approval_reference`

In token mode, all simulated flow execution still requires `flows:read`. When a request asks for side effects by setting `allow_side_effects=true` or by providing non-empty `requested_sink_actions`, the route also requires `flows:execute` or `admin:all`.

The response now includes an `authorization` object that reports:

- whether side effects were requested;
- whether the caller was authorized for future side effects;
- whether real side effects are supported by the current runtime;
- the current policy;
- requested sink actions;
- whether operator reason and approval reference were supplied.

For this milestone, the runtime still always reports:

- `simulated=true`
- `side_effects_performed=false`
- `authorization.real_side_effects_supported=false`
- `authorization.policy="preview_only_no_side_effects"`

## Consequences

The platform can now distinguish three states:

1. ordinary simulated execution with `flows:read`;
2. a caller asking for future side effects but lacking `flows:execute`;
3. a caller authorized for future side effects, while real effects remain disabled by runtime policy.

This prepares the next milestones for real sink execution without making side effects accidental or implicit.

## Non-goals

This milestone does not implement MQTT publish, HTTP forward, observation writes, event writes, command creation, DLQ writes, broker subscriptions, replay execution, or flow scheduling.
