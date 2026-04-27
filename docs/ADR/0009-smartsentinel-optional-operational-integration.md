# ADR 0009: SmartSentinel Optional Operational Integration

## Status

Accepted

## Context

SmartSentinel can provide operational visibility, snapshots, diagnostics, and remediation workflows. These capabilities are useful for infrastructure monitoring and closed-loop operations, but AionCore should not require SmartSentinel to run.

AionCore needs a clean integration boundary that allows SmartSentinel data to enrich semantic context without making SmartSentinel a platform dependency or forcing operational monitoring assumptions into the core model.

## Decision

SmartSentinel will be modeled as an optional operational integration.

SmartSentinel may act as:

- An observer that provides snapshots, events, and summaries.
- An executor that performs diagnostics or remediation when explicitly configured and policy-approved.
- An evidence source referenced by AionCore records.

SmartSentinel snapshots can be ingested as raw messages. Relevant elements may be materialized as entities, relationships, observations, events, commands, actions, and action results.

High-frequency operational metrics should remain in specialized metric backends when needed. AionCore stores semantic state, summaries, events, decisions, commands, action results, and references.

## Consequences

Positive:

- AionCore can support operational monitoring use cases without depending on SmartSentinel.
- SmartSentinel evidence can enrich context and closed-loop verification.
- High-volume metrics and traces remain in systems designed for that workload.
- Critical remediation can be governed by AionCore policies and capabilities.

Negative:

- The integration needs careful mapping from SmartSentinel snapshot structures to AionCore semantic records.
- Operators must decide which snapshot elements should be materialized.
- Action execution requires future policy, authorization, and audit implementation.

## MVP Simplification

Document the integration boundary first. Do not implement SmartSentinel runtime code, database-specific integration tables, or automatic remediation in the MVP documentation update.
