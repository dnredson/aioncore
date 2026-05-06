# AionCore Dashboard Skeleton

This app is the Milestone 89 static frontend skeleton for the AionCore dashboard.

## Why Static

The first dashboard frontend intentionally uses plain HTML, CSS, and JavaScript:

- no Node.js toolchain
- no build step
- no framework lock-in
- low-risk local development against the existing read-only dashboard APIs

This keeps the first UI milestone lightweight while the backend dashboard, flow, and reliability contracts are still stabilizing.

## Files

- `index.html`
- `styles.css`
- `dashboard.js`

## APIs Consumed

- `GET /dashboard/overview`
- `GET /dashboard/timeseries/entities`
- `GET /dashboard/connectors/overview`
- `GET /dashboard/flows`
- `GET /dashboard/flows/{flow_id}`

## Local Run

From the repository root:

```powershell
python -m http.server 5173 --directory apps/aion-dashboard
```

Then open:

```text
http://127.0.0.1:5173
```

Default API base URL:

```text
http://127.0.0.1:8080
```

The UI also supports an optional bearer token for local development. Both the API base URL and token can be stored in browser `localStorage`.

## Deferred Work

This skeleton does not implement:

- charting libraries
- flow editing
- drag-and-drop graph building
- flow execution
- connector mutation workflows
- broker subscription changes
- MQTT publish or HTTP forward actions

If the dashboard grows beyond this lightweight phase, a later milestone can migrate the app to React/Vite or another UI stack without changing the existing dashboard API contracts.
