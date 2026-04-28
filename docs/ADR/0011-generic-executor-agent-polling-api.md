# ADR 0011: Generic Executor Agent Polling API

## Status

Accepted

## Context

AionCore creates Commands, but it must not execute real-world actions directly. External executors such as edge agents, building controllers, traffic-light controllers, irrigation gateways, or optional integrations like SmartSentinel need a generic way to discover compatible commands, claim them, and report outcomes.

The MVP must stay in-memory and domain-agnostic. SmartSentinel must remain optional, and command approval policies must continue to protect execution.

## Decision

Add generic ExecutorAgent, ExecutorCapability, and ExecutorScope models to the action layer.

Executors register through the API, heartbeat their status, declare command capabilities, and define scopes for target entities, entity types, or relationship types. The command polling API returns pending commands whose command type and target are compatible with the executor declarations.

Executor claim, complete, and fail endpoints reuse the existing command lifecycle. Commands that require approval remain unclaimable until approved. Completion and failure create Action, ActionResult, and executor audit Events, but AionCore still does not execute real commands.

## Consequences

- External agents can integrate without adding domain-specific dependencies.
- SmartSentinel can be modeled as one executor type later without becoming a hard dependency.
- Policy behavior remains centralized in the command lifecycle.
- Executor registration, scopes, capabilities, and command state remain in-memory only.
- Future work needs persistence, auth, executor credentials, leases/expiry, retry semantics, and transport-specific integrations.
