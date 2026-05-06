# ADR 0066: aion-api provenance route extraction

## Status

Accepted

## Context

Milestones 61 through 70 established the staged `aion-api` modularization pattern by extracting shared auth and error foundations first, then moving bounded route groups and shared AI context logic out of `apps/aion-api/src/lib.rs`.

After Milestone 70, `lib.rs` still contained one cohesive provenance-oriented read surface that was narrower than the larger remaining ingestion and entity route groups:

- `GET /provenance/search`
- provenance-search-local query and response DTOs
- provenance-search-local event, raw-message, and observation matching helpers
- provenance search query metadata shaping

This endpoint is closely related to SmartSentinel provenance and evidence metadata, but it is still a bounded read/query surface with no evidence fetching and no external network behavior. Extracting it now continues route-level modularization while keeping risk below a broader ingestion or entity split.

## Decision

Extract the provenance search HTTP surface from `apps/aion-api/src/lib.rs` into `apps/aion-api/src/routes/provenance.rs`.

Move into `routes/provenance.rs`:

- route registration for `GET /provenance/search`
- the handler and its existing `provenance:read` scope enforcement
- `ProvenanceSearchQuery`
- `ProvenanceSearchResponse`
- `ProvenanceSearchCounts`
- provenance-search-specific event, raw-message, and observation matching helpers
- provenance search query metadata shaping

Keep in `lib.rs`:

- the lower-level metadata and raw-message filter primitives that are also used by `/events` and `/raw-messages`
- shared raw-message response shaping
- centralized tests

Expose only a minimal `pub(crate)` helper surface from `lib.rs` so the new provenance route module can reuse the existing filtering behavior without duplicating shared logic.

## Consequences

Positive:

- `lib.rs` continues shrinking through narrow, behavior-preserving extraction.
- provenance search now lives in a dedicated route module with its route-local DTOs and matching logic.
- SmartSentinel provenance compatibility stays intact because the extracted route still uses the same metadata/header matching paths, raw-message shaping, and local storage queries.

Neutral / intentional:

- No endpoint paths, auth semantics, tenant/resource ownership behavior, filtering behavior, JSON shapes, or count behavior changed.
- No evidence URLs are fetched and no external network calls were introduced.
- Shared helper primitives intentionally remain in `lib.rs` because moving them now would increase scope and risk for `/events` and `/raw-messages`.
- Tests remain in `lib.rs` for now to minimize churn during staged modularization.

Future work:

- continue incremental extraction of other cohesive remaining route groups
- revisit shared filter-helper placement only when more than one extracted route module needs the same surface
