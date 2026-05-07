import {
  DEFAULT_TIMESERIES_LIMIT,
  TIMESERIES_AGGREGATION_NONE,
} from "./constants.js";
import { apiGet } from "./api.js";
import { state } from "./state.js";
import {
  buildEmptyRow,
  buildTimeseriesChartSvg,
  cleanString,
  escapeHtml,
  formatDateTime,
  formatNumber,
  formatObservationValue,
  formatTimeseriesRange,
  handleError,
  normalizeDateTimeInput,
  normalizeTimeseriesLimit,
  observationNumber,
  setStatus,
  withGeneratedAt,
} from "./utils.js";

export function bindTimeseriesEvents() {
  document.getElementById("refresh-timeseries-button").addEventListener("click", () => {
    loadTimeseries(true);
  });

  document.getElementById("timeseries-entity-select").addEventListener("change", (event) => {
    selectTimeseriesEntity(event.target.value, { loadProperties: true, forceProperties: false });
  });

  document.getElementById("timeseries-property-select").addEventListener("change", (event) => {
    state.selectedTimeseriesProperty = cleanString(event.target.value);
    renderTimeseriesSelectionSummary();
  });

  document.getElementById("load-timeseries-properties-button").addEventListener("click", () => {
    if (!state.selectedTimeseriesEntityId) {
      handleError(new Error("Select an entity before loading observed properties."));
      return;
    }
    loadTimeseriesProperties(state.selectedTimeseriesEntityId, true).catch(handleError);
  });

  document.getElementById("timeseries-query-form").addEventListener("submit", (event) => {
    event.preventDefault();
    runTimeseriesQuery().catch(handleError);
  });

  document.getElementById("reset-timeseries-query-button").addEventListener("click", () => {
    resetTimeseriesFilters();
  });
}

export async function loadTimeseries(force) {
  setStatus("Loading time-series entities...");
  const data = force || !state.cache.timeseries
    ? await apiGet("/dashboard/timeseries/entities")
    : state.cache.timeseries;
  state.cache.timeseries = data;
  ensureSelectedTimeseriesEntity(data.entities || []);
  renderTimeseries(data);
  await syncTimeseriesPropertiesAfterInventory(force);
  setStatus(withGeneratedAt("Time-series entities loaded.", data.generated_at));
}

async function selectTimeseriesEntity(entityId, options = {}) {
  const { loadProperties = true, forceProperties = false } = options;
  state.selectedTimeseriesEntityId = cleanString(entityId) || null;
  state.selectedTimeseriesProperty = "";
  state.cache.timeseriesQuery = null;
  populateTimeseriesEntitySelect(state.cache.timeseries?.entities || []);
  resetTimeseriesResults("Select an observed property and query the series.");
  renderTimeseriesSelectionSummary();

  if (!state.selectedTimeseriesEntityId) {
    renderTimeseriesProperties(null);
    return;
  }

  if (loadProperties) {
    setStatus("Loading observed properties...");
    await loadTimeseriesProperties(state.selectedTimeseriesEntityId, forceProperties);
    setStatus("Observed properties loaded.");
  }
}

async function loadTimeseriesProperties(entityId, force) {
  const cacheKey = String(entityId);
  const data = force || !state.cache.timeseriesProperties.has(cacheKey)
    ? await apiGet(`/timeseries/entities/${encodeURIComponent(entityId)}/properties`)
    : state.cache.timeseriesProperties.get(cacheKey);
  state.cache.timeseriesProperties.set(cacheKey, data);

  const properties = data?.properties || [];
  if (!properties.some((item) => item.observed_property === state.selectedTimeseriesProperty)) {
    state.selectedTimeseriesProperty = properties[0]?.observed_property || "";
  }

  populateTimeseriesPropertySelect(properties);
  renderTimeseriesProperties(data);
  renderTimeseriesSelectionSummary();
  setTimeseriesLocalStatus(
    properties.length > 0
      ? `Loaded ${formatNumber(properties.length)} observed propert${properties.length === 1 ? "y" : "ies"} for the selected entity.`
      : "The selected entity currently has no observed properties."
  );
}

async function runTimeseriesQuery() {
  const entityId = cleanString(document.getElementById("timeseries-entity-select").value);
  const observedProperty = cleanString(document.getElementById("timeseries-property-select").value);

  if (!entityId) {
    throw new Error("Select an entity before querying time-series data.");
  }
  if (!observedProperty) {
    throw new Error("Select an observed property before querying time-series data.");
  }

  state.selectedTimeseriesEntityId = entityId;
  state.selectedTimeseriesProperty = observedProperty;

  const aggregation = cleanString(document.getElementById("timeseries-aggregation").value) || TIMESERIES_AGGREGATION_NONE;
  const limit = normalizeTimeseriesLimit(document.getElementById("timeseries-limit").value);
  const from = normalizeDateTimeInput(document.getElementById("timeseries-from").value);
  const to = normalizeDateTimeInput(document.getElementById("timeseries-to").value);

  const params = new URLSearchParams({
    entity_id: entityId,
    observed_property: observedProperty,
    limit: String(limit),
  });

  if (aggregation !== TIMESERIES_AGGREGATION_NONE) {
    params.set("aggregation", aggregation);
  }
  if (from) {
    params.set("from", from);
  }
  if (to) {
    params.set("to", to);
  }

  setStatus("Querying time-series data...");
  setTimeseriesLocalStatus("Running /timeseries/query...");
  const response = await apiGet(`/timeseries/query?${params.toString()}`);
  state.cache.timeseriesQuery = response;
  renderTimeseriesResults(response);
  renderTimeseriesSelectionSummary();
  setTimeseriesLocalStatus("Time-series query completed.");
  setStatus("Time-series query completed.");
}

function resetTimeseriesFilters() {
  document.getElementById("timeseries-from").value = "";
  document.getElementById("timeseries-to").value = "";
  document.getElementById("timeseries-aggregation").value = TIMESERIES_AGGREGATION_NONE;
  document.getElementById("timeseries-limit").value = String(DEFAULT_TIMESERIES_LIMIT);
  state.cache.timeseriesQuery = null;
  resetTimeseriesResults("Filters reset. Query again to load points.");
}

function ensureSelectedTimeseriesEntity(entities) {
  const available = entities || [];
  const existing = available.find((entity) => entity.entity_id === state.selectedTimeseriesEntityId);
  if (existing) {
    return;
  }
  state.selectedTimeseriesEntityId = available[0]?.entity_id || null;
  state.selectedTimeseriesProperty = "";
}

async function syncTimeseriesPropertiesAfterInventory(force) {
  populateTimeseriesEntitySelect(state.cache.timeseries?.entities || []);

  if (!state.selectedTimeseriesEntityId) {
    renderTimeseriesProperties(null);
    renderTimeseriesSelectionSummary();
    return;
  }

  await loadTimeseriesProperties(state.selectedTimeseriesEntityId, force);
}

function renderTimeseries(data) {
  populateTimeseriesEntitySelect(data.entities || []);
  renderTimeseriesSelectionSummary();

  const rows = (data.entities || []).map((entity) => `
    <tr class="interactive-row${state.selectedTimeseriesEntityId === entity.entity_id ? " selected" : ""}">
      <td>
        <button class="button subtle table-select" type="button" data-timeseries-entity-select="${escapeHtml(entity.entity_id)}">${escapeHtml(entity.entity_key)}</button>
        <div class="status-meta">${escapeHtml(entity.entity_id)}</div>
      </td>
      <td>${escapeHtml(entity.entity_type || "n/a")}</td>
      <td>${escapeHtml(entity.display_name || "n/a")}</td>
      <td>${formatNumber(entity.observed_property_count)}</td>
      <td>${formatNumber(entity.observation_count)}</td>
      <td>${escapeHtml(formatDateTime(entity.last_observed_at))}</td>
    </tr>
  `);

  document.getElementById("timeseries-table-body").innerHTML = rows.join("") || buildEmptyRow(6, "No time-series entities returned.");

  document.querySelectorAll("[data-timeseries-entity-select]").forEach((button) => {
    button.addEventListener("click", () => {
      selectTimeseriesEntity(button.dataset.timeseriesEntitySelect, { loadProperties: true, forceProperties: false }).catch(handleError);
    });
  });
}

function populateTimeseriesEntitySelect(entities) {
  const select = document.getElementById("timeseries-entity-select");
  const options = (entities || []).map((entity) => `
    <option value="${escapeHtml(entity.entity_id)}"${entity.entity_id === state.selectedTimeseriesEntityId ? " selected" : ""}>${escapeHtml(entity.display_name || entity.entity_key || entity.entity_id)}</option>
  `);
  select.innerHTML = options.join("") || '<option value="">No entities available</option>';
  select.disabled = options.length === 0;
}

function populateTimeseriesPropertySelect(properties) {
  const select = document.getElementById("timeseries-property-select");
  const options = (properties || []).map((property) => `
    <option value="${escapeHtml(property.observed_property)}"${property.observed_property === state.selectedTimeseriesProperty ? " selected" : ""}>${escapeHtml(property.observed_property)}</option>
  `);
  select.innerHTML = options.join("") || '<option value="">No properties available</option>';
  select.disabled = options.length === 0;
}

function renderTimeseriesProperties(data) {
  const properties = data?.properties || [];
  const rows = properties.map((property) => `
    <tr class="interactive-row${state.selectedTimeseriesProperty === property.observed_property ? " selected" : ""}">
      <td>
        <button class="button subtle table-select" type="button" data-timeseries-property-select="${escapeHtml(property.observed_property)}">${escapeHtml(property.observed_property)}</button>
      </td>
      <td>${escapeHtml((property.units || []).join(", ") || "n/a")}</td>
      <td>${formatNumber(property.count)}</td>
      <td>${escapeHtml(formatDateTime(property.first_observed_at))}</td>
      <td>${escapeHtml(formatDateTime(property.last_observed_at))}</td>
    </tr>
  `);

  document.getElementById("timeseries-properties-table-body").innerHTML = rows.join("") || buildEmptyRow(5, state.selectedTimeseriesEntityId ? "No observed properties returned for the selected entity." : "Select an entity to load observed properties.");

  document.querySelectorAll("[data-timeseries-property-select]").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedTimeseriesProperty = button.dataset.timeseriesPropertySelect;
      populateTimeseriesPropertySelect(properties);
      renderTimeseriesProperties(data);
      renderTimeseriesSelectionSummary();
    });
  });
}

function renderTimeseriesSelectionSummary() {
  const entity = findSelectedTimeseriesEntity();
  const property = findSelectedTimeseriesProperty();
  const summaryItems = [
    ["Selected Entity", entity?.display_name || entity?.entity_key || "None selected"],
    ["Entity ID", entity?.entity_id || "n/a"],
    ["Selected Property", property?.observed_property || state.selectedTimeseriesProperty || "None selected"],
    ["Property Units", (property?.units || []).join(", ") || "n/a"],
    ["Query Scope", state.bearerToken ? "Token mode enabled" : "Dev mode / no token"],
    ["Latest Query", summarizeTimeseriesQuery(state.cache.timeseriesQuery)],
  ];

  document.getElementById("timeseries-selection-summary").innerHTML = summaryItems.map(([label, value]) => `
    <div class="detail-item${label === "Selected Property" && (property?.observed_property || state.selectedTimeseriesProperty) ? " selected" : ""}">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(String(value))}</span>
    </div>
  `).join("");
}

function renderTimeseriesResults(response) {
  const aggregation = response?.aggregation || TIMESERIES_AGGREGATION_NONE;
  const points = response?.points || [];
  const isRawQuery = aggregation === TIMESERIES_AGGREGATION_NONE;

  document.getElementById("timeseries-results-empty").classList.add("hidden");
  document.getElementById("timeseries-results-meta").textContent = buildTimeseriesResultsMeta(response);

  if (isRawQuery) {
    renderTimeseriesRawResults(response);
    renderTimeseriesAggregationSummary(null);
    renderTimeseriesChart(response);
    if (points.length === 0) {
      document.getElementById("timeseries-results-empty").classList.remove("hidden");
      document.getElementById("timeseries-results-empty").textContent = "No raw points matched the selected filters.";
    }
    return;
  }

  renderTimeseriesAggregationSummary(response);
  renderTimeseriesChart(null);
  renderTimeseriesRawResults(null);

  if (points.length === 0) {
    document.getElementById("timeseries-results-empty").classList.remove("hidden");
    document.getElementById("timeseries-results-empty").textContent = "The aggregation returned no points for the selected range.";
  }
}

function renderTimeseriesRawResults(response) {
  const tableWrap = document.getElementById("timeseries-results-table-wrap");
  const tableBody = document.getElementById("timeseries-results-table-body");
  if (!response) {
    tableWrap.classList.add("hidden");
    tableBody.innerHTML = "";
    return;
  }

  const rows = (response.points || []).map((point) => `
    <tr>
      <td>${escapeHtml(formatDateTime(point.time))}</td>
      <td>${escapeHtml(formatObservationValue(point.value))}</td>
      <td>${escapeHtml(point.unit || "n/a")}</td>
      <td>${escapeHtml(point.observation_id || "n/a")}</td>
      <td>${escapeHtml(point.raw_message_id || "n/a")}</td>
    </tr>
  `);

  tableBody.innerHTML = rows.join("") || buildEmptyRow(5, "No raw points returned.");
  tableWrap.classList.remove("hidden");
}

function renderTimeseriesAggregationSummary(response) {
  const container = document.getElementById("timeseries-aggregation-summary");
  if (!response) {
    container.classList.add("hidden");
    container.innerHTML = "";
    return;
  }

  const point = response.points?.[0];
  const cards = [
    ["Aggregation", response.aggregation || "n/a"],
    ["Value", point ? formatObservationValue(point.value) : "n/a"],
    ["Count", response.aggregation === "count" ? formatObservationValue(point?.value) : formatNumber(response.count)],
    ["Unit", point?.unit || "n/a"],
    ["Time / Range", point?.time ? formatDateTime(point.time) : formatTimeseriesRange(response.from, response.to)],
  ];

  container.innerHTML = cards.map(([label, value]) => `
    <div class="detail-item">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(String(value))}</span>
    </div>
  `).join("");
  container.classList.remove("hidden");
}

function renderTimeseriesChart(response) {
  const panel = document.getElementById("timeseries-chart-panel");
  const chart = document.getElementById("timeseries-chart");
  const note = document.getElementById("timeseries-chart-note");

  if (!response) {
    panel.classList.add("hidden");
    chart.innerHTML = "";
    note.textContent = "Chart appears only for numeric raw points.";
    return;
  }

  const numericPoints = (response.points || []).filter((point) => typeof observationNumber(point.value) === "number");
  const skippedPoints = (response.points || []).length - numericPoints.length;

  panel.classList.remove("hidden");

  if (numericPoints.length === 0) {
    chart.innerHTML = '<div class="chart-placeholder">Chart unavailable because the raw points are non-numeric.</div>';
    note.textContent = "Only numeric raw points are plotted. Use the table below for non-numeric values.";
    return;
  }

  chart.innerHTML = buildTimeseriesChartSvg(numericPoints);
  note.textContent = skippedPoints > 0
    ? `Plotting ${formatNumber(numericPoints.length)} numeric points. ${formatNumber(skippedPoints)} non-numeric points remain table-only.`
    : `Plotting ${formatNumber(numericPoints.length)} numeric raw points.`;
}

function resetTimeseriesResults(message) {
  document.getElementById("timeseries-results-meta").textContent = message;
  document.getElementById("timeseries-results-empty").classList.remove("hidden");
  document.getElementById("timeseries-results-empty").textContent = message;
  document.getElementById("timeseries-results-table-wrap").classList.add("hidden");
  document.getElementById("timeseries-results-table-body").innerHTML = "";
  document.getElementById("timeseries-aggregation-summary").classList.add("hidden");
  document.getElementById("timeseries-aggregation-summary").innerHTML = "";
  document.getElementById("timeseries-chart-panel").classList.add("hidden");
  document.getElementById("timeseries-chart").innerHTML = "";
  document.getElementById("timeseries-chart-note").textContent = "Chart appears only for numeric raw points.";
}

function setTimeseriesLocalStatus(message) {
  document.getElementById("timeseries-local-status").textContent = message;
}

function findSelectedTimeseriesEntity() {
  return (state.cache.timeseries?.entities || []).find((entity) => entity.entity_id === state.selectedTimeseriesEntityId) || null;
}

function findSelectedTimeseriesProperty() {
  const entityId = state.selectedTimeseriesEntityId;
  if (!entityId) {
    return null;
  }
  const properties = state.cache.timeseriesProperties.get(String(entityId))?.properties || [];
  return properties.find((property) => property.observed_property === state.selectedTimeseriesProperty) || null;
}

function summarizeTimeseriesQuery(response) {
  if (!response) {
    return "No query yet";
  }
  return `${response.aggregation || TIMESERIES_AGGREGATION_NONE} / ${formatNumber(response.count)} point${response.count === 1 ? "" : "s"}${response.truncated ? " / truncated" : ""}`;
}

function buildTimeseriesResultsMeta(response) {
  const pointsLabel = `${formatNumber(response.count)} point${response.count === 1 ? "" : "s"}`;
  const truncationLabel = response.truncated ? " Results truncated by limit." : "";
  return `Aggregation: ${response.aggregation || TIMESERIES_AGGREGATION_NONE}. Returned ${pointsLabel}. Limit ${formatNumber(response.limit)}.${truncationLabel}`;
}
