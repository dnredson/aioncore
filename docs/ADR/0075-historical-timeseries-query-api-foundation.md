# ADR 0075: Historical Time-Series Query API Foundation

## Status

Accepted

## Context

AionCore already stores canonical observations and exposes `GET /observations`, but that surface is intentionally generic. It does not provide an explicit dashboard-oriented historical query contract with first-class entity, observed-property, and time-range semantics.

The next product direction includes InfluxDB/Grafana-style exploration:

- choose an entity
- discover its observed properties
- fetch historical series for one property
- later reuse the same query model from MCP and AI tooling

At the same time, this milestone must stay narrow:

- do not change existing `/observations` behavior
- do not add dashboard UI yet
- do not add MCP time-series tools yet
- do not add a new storage backend
- do not change ingestion behavior

## Decision

Add a dedicated historical time-series API foundation in `aion-api`:

- `GET /timeseries/query`
- `GET /timeseries/entities/{entity_id}/properties`

Key decisions:

- `entity_id` in the time-series API maps to observation `feature_of_interest_id`.
- `/observations` remains unchanged for backward compatibility and lower rollout risk.
- token mode protects both routes with `timeseries:read`.
- non-admin token reads reuse the existing tenant/resource ownership pattern by resolving the target entity first.
- the first implementation uses current observation storage and filtering behavior, with a small dedicated chronological query path so the new API can return ascending series without changing `/observations`.

## Aggregation Scope

This milestone supports only whole-range aggregations:

- `last`
- `count`
- `avg`
- `min`
- `max`

`interval` bucket aggregation is intentionally deferred. The API accepts the parameter shape but currently returns a clear request error rather than silently pretending to support bucketed aggregation.

Numeric aggregations operate only on numeric observation values. The API returns a request error when no numeric values exist for the selected series instead of coercing strings or JSON.

## Consequences

Positive:

- dashboard work can start against a stable historical read surface
- `/observations` clients are not disrupted
- tenant-aware historical reads now have explicit auth scope coverage
- in-memory and PostgreSQL backends remain aligned without schema changes

Trade-offs:

- no interval downsampling yet
- no TimescaleDB-specific optimization yet
- property discovery currently relies on observation scans rather than precomputed indexes or materialized summaries

## Future Work

- add bucketed interval aggregation
- add dashboard exploration UI
- add MCP and AI time-series query tools on top of the same API semantics
- evaluate TimescaleDB-specific optimizations once usage patterns are validated
