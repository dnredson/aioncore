# ADR 0005: MCP-Ready AI Integration

## Status

Accepted

## Context

AionCore is intended to be AI-native. AI clients need access to domain context and observations, but unrestricted action execution would create safety and operational risks.

## Decision

AionCore will provide MCP-ready integration focused on read-only tools for MVP 1.

Initial MCP capabilities:

- Query entities.
- Query relationships.
- Query observations.

Critical actions will not be executable by LLMs by default.

## Consequences

Positive:

- AI clients can use grounded semantic context.
- Observation queries can support analysis and decision support.
- The safety boundary is clear from the beginning.

Negative:

- AI-driven closed-loop control is postponed.
- Future action tools will require separate authorization, auditing, and approval design.

## MVP Simplification

Expose read-only MCP tool designs backed by the same query services used by the public API. Do not add write or actuation tools in MVP 1.
