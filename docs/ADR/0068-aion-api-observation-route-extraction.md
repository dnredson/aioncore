# ADR 0068: aion-api observation route extraction

## Status

Accepted

## Context

Milestones 61 through 72 established the staged `aion-api` modularization pattern by extracting shared auth and error foundations first, then moving bounded route groups and read/query surfaces out of `apps/aion-api/src/lib.rs`.

After Milestone 72, `lib.rs` still contained the core observation HTTP surface:

- `POST /observations`
- `GET /observations`

Observations are central to AionCore and are the base runtime model for later historical time-series query APIs, dashboard exploration, provenance, and AI context assembly. Extracting this route surface before adding new time-series features reduces future churn while keeping the current semantics stable.

## Decision

Extract the observation HTTP surface from `apps/aion-api/src/lib.rs` into:

- `apps/aion-api/src/routes/observations.rs`

Move into `routes/observations.rs`:

- route registration for `/observations`
- `CreateObservationRequest`
- `ObservationQuery`
- the existing create and list handlers
- the observation-route-local empty-object helper used by request defaults

Keep in `lib.rs`:

- shared application state and route assembly
- central auth middleware wiring
- shared tenant/resource ownership helpers
- shared rule evaluation helpers
- ingestion flows that also create observations
- centralized tests

## Consequences

Positive:

- `lib.rs` continues shrinking through narrow, behavior-preserving extraction.
- observation routes now live in a dedicated module with their route-local DTOs.
- future historical time-series APIs can be added beside the current observation route surface without first untangling route code from the main application module.

Neutral / intentional:

- No endpoint paths, auth semantics, tenant/resource ownership behavior, rule-evaluation behavior, ingestion behavior, or JSON shapes changed.
- observation creation still stores the observation first and then triggers the same rule-evaluation path.
- observation query behavior, including existing tenant filtering and current supported filters, remains unchanged.
- shared helpers that are also used by ingestion or other route groups intentionally remain in `lib.rs` to avoid premature movement and extra modularization risk.

Future work:

- add historical time-series query APIs as a separate milestone
- continue extracting only cohesive remaining route groups or shared helpers when another concrete dependency justifies it
