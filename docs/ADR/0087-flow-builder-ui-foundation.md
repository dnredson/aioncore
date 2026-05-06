# ADR 0087: Flow Builder UI Foundation

## Status

Accepted

## Context

Milestone 82 introduced stored flow CRUD.

Milestone 87 introduced read-only flow validation and dry-run planning APIs.

Milestone 88 introduced dashboard-oriented flow inventory and detail reads.

Milestones 89, 90, and 91 established the no-build static dashboard and extended it with connector management and time-series exploration.

Operators still lacked any UI path to draft a flow definition, validate it, preview the resulting JSON, and inspect planning behavior before saving.

The project direction remains a future Node-RED-like experience, but this milestone needed to stay low-risk:

- no React, Vite, Next, npm, or external CDNs
- no backend API changes
- no drag-and-drop or visual graph editing
- no flow execution
- no broker subscriptions from flows
- no MQTT publish or HTTP forward side effects
- no observation, event, command, or DLQ writes from UI actions
- no secret exposure

## Decision

Extend `apps/aion-dashboard/` with a static form-based Flow Builder foundation that consumes only the existing APIs:

- `POST /flows`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`
- `POST /flows/validate`
- `GET /flows/{flow_id}/validation`
- `POST /flows/dry-run`
- `POST /flows/{flow_id}/dry-run`
- `PUT /flows/{flow_id}/enable`
- `PUT /flows/{flow_id}/disable`
- `DELETE /flows/{flow_id}`
- `GET /ingestion/connectors`
- `GET /dashboard/connectors/overview`

Key UI decisions:

- keep the dashboard as plain HTML, CSS, and JavaScript
- add a guided source -> transform -> sink builder rather than arbitrary graph editing
- generate linear edges automatically
- show a redacted JSON preview before create
- allow an optional advanced JSON override textarea for low-risk manual edits
- require explicit operator actions for validate, dry-run, create, enable, disable, and delete
- use browser `confirm()` before delete
- reuse the existing bearer-token configuration and surface scope guidance for `flows:read`, `flows:write`, `dashboard:read`, and `connectors:read`
- redact secret-like keys in preview and stored flow detail output

## Consequences

Positive:

- operators can draft and inspect flow definitions without leaving the dashboard
- existing validation and dry-run APIs now have a practical UI consumer
- the project gets a safe authoring step before a future visual builder exists
- the dashboard still avoids frontend build tooling and backend changes

Trade-offs:

- the builder is intentionally linear and not a general graph editor
- advanced JSON editing is text-based rather than graphical
- dry-run remains conceptual and planning-only because execution does not exist
- stored flow updates still remain outside this milestone's UI

## Non-Goals

This ADR does not introduce:

- drag-and-drop flow editing
- visual node graph editing
- flow execution
- broker subscriptions
- MQTT publish or HTTP forward execution
- observation, event, command, or DLQ writes
- secret-management workflows
- external AI calls

## Future Work

- add a Node-RED-like visual builder on top of the same flow model and validation contracts
- expand typed node-kind guidance and node-specific config forms
- consider safe stored-flow patch workflows after the visual editing direction is clearer
