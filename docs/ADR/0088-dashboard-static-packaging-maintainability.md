# ADR 0088: Dashboard Static Packaging And Maintainability

## Status

Accepted

## Context

Milestones 89 through 92 established a no-build static dashboard under `apps/aion-dashboard/` using plain HTML, CSS, and JavaScript. That approach kept the operator UI easy to inspect and easy to serve locally while backend APIs were still evolving.

By Milestone 92, the single `apps/aion-dashboard/dashboard.js` file had grown large enough to make future maintenance riskier, especially before adding any execution-oriented or graph-editor-oriented flow features.

The immediate problem in Milestone 93 is maintainability and local packaging, not runtime expansion:

- backend API behavior must remain unchanged
- no frontend build toolchain should be introduced
- no `node_modules` should be added
- no external CDN dependencies should be added
- flow execution must remain out of scope

## Decision

Keep the dashboard as a no-build static frontend, but refactor the JavaScript into smaller native browser ES modules:

- `apps/aion-dashboard/dashboard.js`
- `apps/aion-dashboard/js/constants.js`
- `apps/aion-dashboard/js/state.js`
- `apps/aion-dashboard/js/utils.js`
- `apps/aion-dashboard/js/api.js`
- `apps/aion-dashboard/js/timeseries.js`
- `apps/aion-dashboard/js/connectors.js`
- `apps/aion-dashboard/js/flows.js`

`index.html` now loads `dashboard.js` with `type="module"`.

The module boundaries are intentionally pragmatic:

- app bootstrap and section switching in `dashboard.js`
- shared config and cache state in `state.js`
- shared API request logic in `api.js`
- shared formatting, redaction, and error/status helpers in `utils.js`
- section-specific UI logic in `timeseries.js`, `connectors.js`, and `flows.js`

## Static Serving Decision

Optional static serving from `aion-api` is deferred in this milestone.

Reason:

- the dashboard can already be served locally with a one-line static file server
- the JavaScript modularization solves the main maintainability concern directly
- deferring backend static hosting keeps Milestone 93 focused and avoids Rust-side validation churn for a packaging-only improvement

## Consequences

Positive:

- the frontend remains dependency-free
- the dashboard is easier to read and change
- browser-native module loading remains simple for local demos
- API behavior and route contracts stay unchanged

Negative:

- the dashboard still relies on a separate static file server for local serving
- there is still no typed build pipeline, bundling, or component-level test harness
- browser support remains tied to modern ES module support

## When To Revisit

Consider a full frontend toolchain only if:

- module count and UI complexity keep growing despite the current split
- the dashboard needs stronger asset processing or component-level testing
- the operator UI becomes a larger product surface rather than a focused static admin tool

Consider optional `aion-api` static hosting later only if:

- local demo ergonomics become a recurring friction point
- packaging the dashboard with the API materially improves deployment simplicity
- the implementation can remain explicitly optional and cannot interfere with existing API routes
