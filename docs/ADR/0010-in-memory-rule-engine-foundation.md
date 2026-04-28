# ADR 0010: In-Memory Rule Engine Foundation

## Status

Accepted

## Context

AionCore needs a simple closed-loop foundation that can react to canonical observations and events without hard-coding domain concepts such as agriculture, smart buildings, or smart cities.

The first rule engine milestone must remain in-memory, local-development focused, and constrained to creating Commands and Events. It must not execute commands directly, call LLMs, add authentication, or introduce persistence.

## Decision

Add a domain-agnostic `aion-rule` crate with Rule, RuleCondition, RuleAction, and RuleEvaluationResult models.

Rules support:

- Trigger types: `observation_created`, `event_created`, and `manual`.
- Optional filters for target entity, observed property, and event type.
- Simple comparison conditions.
- Actions that create Events or Commands only.

The API stores rules in the existing in-memory storage layer and evaluates enabled rules when observations or events are created. Rule-generated Events do not recursively trigger more rule evaluation in this milestone. Rule-generated Commands reuse the existing command policy path and remain pending until normal approval, claim, and execution APIs are used.

## Consequences

- Closed-loop scenarios can be modeled without domain-specific code.
- Command safety behavior remains centralized in the existing policy and command lifecycle.
- Rule state is lost when the process exits.
- Future production work still needs persistence, richer condition expressions, auth, audit hardening, recursion controls, and operational safeguards.
