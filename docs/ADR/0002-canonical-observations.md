# ADR 0002: Canonical Observations

## Status

Accepted

## Context

AionCore must support multiple telemetry payload formats. Applications, query APIs, analytics, and AI tools should not need to understand each original payload format.

The platform needs one normalized representation for valid telemetry.

## Decision

All valid telemetry will be normalized into canonical observations.

A canonical observation records:

- Tenant.
- Entity.
- Observed property.
- Observation time.
- Typed value.
- Unit.
- Optional quality metadata.
- Optional source raw message reference.

Canonical observations will be stored in PostgreSQL with TimescaleDB for time-series behavior.

## Consequences

Positive:

- Query APIs can operate across all payload formats.
- Observations can be linked to semantic entities.
- Time-series storage can be optimized through TimescaleDB.

Negative:

- Decoder quality directly affects observation quality.
- Some payload-specific nuance may need to be stored in metadata.

## MVP Simplification

Use nullable typed value columns for number, text, boolean, and JSON values. Enforce exactly one populated value through application logic first.
