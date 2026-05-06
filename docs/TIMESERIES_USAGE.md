# Time-Series Usage

This guide covers the Milestone 80 historical observation/time-series query API foundation and the Milestone 91 static dashboard explorer that consumes it. It adds dashboard-ready historical reads, but it still does not add MCP time-series tools yet.

For the compact dashboard discovery endpoints added later, use [Dashboard Usage](DASHBOARD_USAGE.md).

## Scope

- `GET /timeseries/query`
- `GET /timeseries/entities/{entity_id}/properties`

In this API, `entity_id` means the observation `feature_of_interest_id`.

## Discover Properties For An Entity

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/entities/<entity_id>/properties"
```

Example response:

```json
{
  "entity_id": "7d3df2c8-d59c-4b78-9686-f81cfc313ca5",
  "properties": [
    {
      "observed_property": "soil.moisture",
      "units": ["%"],
      "count": 42,
      "first_observed_at": "2026-05-05T12:00:00Z",
      "last_observed_at": "2026-05-05T12:42:00Z"
    }
  ]
}
```

The static dashboard explorer uses this property list to populate the property selector after an entity is chosen from `GET /dashboard/timeseries/entities`.

## Query Raw Series

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture"
```

Optional query parameters:

- `from`
- `to`
- `aggregation`
- `interval`
- `limit`

Raw-point queries return chronological ascending points. The default `limit` is `1000` and the maximum accepted `limit` is `10000`.

Example response shape:

```json
{
  "entity_id": "7d3df2c8-d59c-4b78-9686-f81cfc313ca5",
  "observed_property": "soil.moisture",
  "from": null,
  "to": null,
  "aggregation": "none",
  "interval": null,
  "points": [
    {
      "time": "2026-05-05T12:00:00Z",
      "value": { "type": "number", "value": 15.0 },
      "unit": "%",
      "quality": {},
      "observation_id": "11111111-1111-1111-1111-111111111111",
      "raw_message_id": "22222222-2222-2222-2222-222222222222"
    }
  ],
  "count": 1,
  "limit": 1000,
  "truncated": false
}
```

## Query Whole-Range Aggregations

Last point:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture&aggregation=last"
```

Count:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture&aggregation=count"
```

Average, minimum, and maximum:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture&aggregation=avg"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture&aggregation=min"
Invoke-RestMethod -Method Get -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture&aggregation=max"
```

Current limitations:

- Aggregations are over the whole selected range only.
- `interval` bucket aggregation is not implemented yet and returns a clear request error.
- `avg`, `min`, and `max` require numeric observation values.

## Static Dashboard Explorer

The static dashboard in `apps/aion-dashboard/` now consumes:

- `GET /dashboard/timeseries/entities`
- `GET /timeseries/entities/{entity_id}/properties`
- `GET /timeseries/query`

Explorer behavior:

- defaults to `aggregation=none`
- defaults to `limit=1000`
- supports optional `from` and `to`
- shows raw rows directly from `points`
- renders aggregation summaries from the same `points` array
- shows backend `count`, `limit`, and `truncated`
- renders a simple dependency-free SVG chart only for numeric raw points

Current UI limitations:

- no interval bucketing controls because the backend does not implement it yet
- no external chart dependency
- no zooming, panning, or downsampling
- no Grafana integration in this milestone

## Token Mode

Both time-series endpoints require `timeseries:read` in `AIONCORE_AUTH_MODE=token`.

Example:

```powershell
$headers = @{ Authorization = "Bearer <token-with-timeseries-read>" }
Invoke-RestMethod -Method Get -Headers $headers -Uri "http://localhost:8080/timeseries/query?entity_id=<entity_id>&observed_property=soil.moisture"
```

Token-mode behavior:

- missing or invalid bearer token: `401`
- valid token without `timeseries:read`: `403`
- `admin:all`: allowed
- non-admin principals: limited to entities owned by the principal tenant
