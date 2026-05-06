const DEFAULT_API_BASE_URL = "http://127.0.0.1:8080";
const STORAGE_KEYS = {
  apiBaseUrl: "aion.dashboard.apiBaseUrl",
  bearerToken: "aion.dashboard.bearerToken",
};
const DEFAULT_TIMESERIES_LIMIT = 1000;
const MAX_TIMESERIES_LIMIT = 10000;
const TIMESERIES_AGGREGATION_NONE = "none";
const DEFAULT_FLOW_SAMPLE_PAYLOAD = '{\n  "temperature": 21.4\n}';

const FLOW_SOURCE_KINDS_WITH_CONNECTOR = new Set([
  "mqtt_subscribe",
  "http_input",
  "ttn_uplink",
]);

const FLOW_SINK_KINDS_WITH_CONNECTOR = new Set([
  "mqtt_publish",
  "http_forward",
]);

const CONNECTOR_TYPE_OPTIONS = [
  { value: "mqtt", label: "MQTT" },
  { value: "http", label: "HTTP" },
  { value: "future", label: "Future / unsupported" },
];

const CONNECTOR_PROFILE_OPTIONS = [
  { value: "generic-mqtt", label: "Generic MQTT" },
  { value: "generic-aion-mqtt", label: "Generic Aion MQTT" },
  { value: "ttn-v3", label: "TTN v3" },
  { value: "custom", label: "Custom / HTTP" },
];

const SECRET_LIKE_KEYS = new Set([
  "password",
  "secret",
  "token",
  "api_key",
  "access_key",
  "private_key",
  "credential",
]);

const state = {
  activeSection: "overview",
  apiBaseUrl: DEFAULT_API_BASE_URL,
  bearerToken: "",
  selectedTimeseriesEntityId: null,
  selectedTimeseriesProperty: "",
  selectedFlowId: null,
  selectedConnectorId: null,
  flowDraft: null,
  cache: {
    overview: null,
    timeseries: null,
    timeseriesQuery: null,
    timeseriesProperties: new Map(),
    connectorsOverview: null,
    connectorsList: null,
    workerPlan: null,
    workerStatus: null,
    connectorDetail: new Map(),
    connectorStatus: new Map(),
    connectorValidation: new Map(),
    connectorLivePlan: new Map(),
    flows: null,
    flowDetail: new Map(),
    flowConnectors: null,
    flowConnectorOverview: null,
    flowProposedValidation: null,
    flowProposedDryRun: null,
    flowStoredValidation: new Map(),
    flowStoredDryRun: new Map(),
  },
};

const sectionTitles = {
  overview: "Overview",
  timeseries: "Time Series",
  connectors: "Connectors",
  flows: "Flows",
};

const sectionModes = {
  overview: "Read-only",
  timeseries: "Read-only",
  connectors: "Read and admin",
  flows: "Read and admin",
};

document.addEventListener("DOMContentLoaded", () => {
  hydrateConfig();
  populateConnectorOptionInputs();
  initializeFlowBuilder();
  bindEvents();
  renderConfig();
  switchSection(state.activeSection);
});

function hydrateConfig() {
  const storedApiBaseUrl = localStorage.getItem(STORAGE_KEYS.apiBaseUrl);
  const storedBearerToken = localStorage.getItem(STORAGE_KEYS.bearerToken);

  state.apiBaseUrl = normalizeApiBaseUrl(storedApiBaseUrl || DEFAULT_API_BASE_URL);
  state.bearerToken = storedBearerToken || "";
}

function populateConnectorOptionInputs() {
  renderSelectOptions("create-connector-type", CONNECTOR_TYPE_OPTIONS, "mqtt");
  renderSelectOptions("create-connector-profile", CONNECTOR_PROFILE_OPTIONS, "generic-mqtt");
}

function initializeFlowBuilder() {
  const form = document.getElementById("flow-builder-form");
  form.elements.flow_key.value = "mqtt-normalize-store";
  form.elements.name.value = "MQTT Normalize Store";
  form.elements.description.value = "MQTT uplink to canonical observation planning";
  form.elements.source_node_id.value = "source-1";
  form.elements.source_name.value = "MQTT Source";
  form.elements.transform_node_id.value = "decoder-1";
  form.elements.transform_name.value = "SenML Decode";
  form.elements.sink_node_id.value = "sink-1";
  form.elements.sink_name.value = "Observation Store";
  form.elements.metadata.value = '{\n  "category": "ingestion",\n  "notes": "execution not implemented"\n}';
  document.getElementById("flow-proposed-sample-payload").value = DEFAULT_FLOW_SAMPLE_PAYLOAD;
  document.getElementById("flow-stored-sample-payload").value = DEFAULT_FLOW_SAMPLE_PAYLOAD;
  populateFlowConnectorSelects([]);
  syncFlowDraftFromForm({ resetAdvancedOverride: false });
}

function renderSelectOptions(elementId, options, selectedValue) {
  const element = document.getElementById(elementId);
  element.innerHTML = options.map((option) => `
    <option value="${escapeHtml(option.value)}"${option.value === selectedValue ? " selected" : ""}>${escapeHtml(option.label)}</option>
  `).join("");
}

function bindEvents() {
  document.querySelectorAll(".nav-tab").forEach((button) => {
    button.addEventListener("click", () => switchSection(button.dataset.section));
  });

  document.getElementById("config-form").addEventListener("submit", (event) => {
    event.preventDefault();
    const apiBaseUrlInput = document.getElementById("api-base-url");
    const bearerTokenInput = document.getElementById("bearer-token");

    state.apiBaseUrl = normalizeApiBaseUrl(apiBaseUrlInput.value || DEFAULT_API_BASE_URL);
    state.bearerToken = bearerTokenInput.value.trim();

    localStorage.setItem(STORAGE_KEYS.apiBaseUrl, state.apiBaseUrl);
    if (state.bearerToken) {
      localStorage.setItem(STORAGE_KEYS.bearerToken, state.bearerToken);
    } else {
      localStorage.removeItem(STORAGE_KEYS.bearerToken);
    }

    clearCaches();
    renderConfig();
    clearError();
    setStatus("API configuration saved.");
    refreshCurrentSection({ force: true });
  });

  document.getElementById("clear-token").addEventListener("click", () => {
    state.bearerToken = "";
    localStorage.removeItem(STORAGE_KEYS.bearerToken);
    document.getElementById("bearer-token").value = "";
    setStatus("Bearer token cleared.");
  });

  document.getElementById("refresh-button").addEventListener("click", () => {
    refreshCurrentSection({ force: true });
  });

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

  document.getElementById("refresh-connectors-button").addEventListener("click", () => {
    loadConnectorsSection(true);
  });

  document.getElementById("refresh-workers-button").addEventListener("click", () => {
    loadWorkerData(true);
  });

  document.getElementById("reconcile-workers-button").addEventListener("click", async () => {
    try {
      setStatus("Reconciling connector workers...");
      const response = await apiPost("/ingestion/workers/reconcile", {});
      state.cache.workerStatus = response;
      state.cache.connectorStatus.clear();
      await loadConnectorsSection(true);
      setStatus("Connector workers reconciled.");
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("connector-refresh-detail").addEventListener("click", () => {
    if (state.selectedConnectorId) {
      loadConnectorDetail(state.selectedConnectorId, true);
    }
  });

  document.getElementById("connector-refresh-status").addEventListener("click", () => {
    if (state.selectedConnectorId) {
      loadConnectorStatus(state.selectedConnectorId, true);
    }
  });

  document.getElementById("connector-enable-button").addEventListener("click", () => {
    if (state.selectedConnectorId) {
      setConnectorEnabled(state.selectedConnectorId, true);
    }
  });

  document.getElementById("connector-disable-button").addEventListener("click", () => {
    if (state.selectedConnectorId) {
      setConnectorEnabled(state.selectedConnectorId, false);
    }
  });

  document.getElementById("connector-validate-button").addEventListener("click", async () => {
    if (!state.selectedConnectorId) {
      return;
    }
    try {
      setStatus("Loading connector validation...");
      await loadConnectorValidation(state.selectedConnectorId, true);
      setStatus("Connector validation loaded.");
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("connector-live-plan-button").addEventListener("click", async () => {
    if (!state.selectedConnectorId) {
      return;
    }
    try {
      setStatus("Loading TTN live readiness dry run...");
      await loadConnectorLivePlan(state.selectedConnectorId, true);
      setStatus("TTN live readiness dry run loaded.");
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("connector-create-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const payload = readConnectorFormPayload(event.currentTarget, { requireKey: true });
      setStatus("Creating connector...");
      await apiPost("/ingestion/connectors", payload);
      event.currentTarget.reset();
      event.currentTarget.querySelector("[name='enabled']").checked = true;
      await loadConnectorsSection(true);
      setStatus(`Connector ${payload.connector_key} created.`);
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("connector-update-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!state.selectedConnectorId) {
      return;
    }
    try {
      const payload = readConnectorFormPayload(event.currentTarget, { requireKey: false });
      setStatus("Patching connector...");
      await apiPatch(`/ingestion/connectors/${encodeURIComponent(state.selectedConnectorId)}`, payload);
      await loadConnectorsSection(true);
      await loadConnectorDetail(state.selectedConnectorId, true);
      setStatus("Connector patched.");
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("connector-update-reset").addEventListener("click", () => {
    if (state.selectedConnectorId) {
      populateUpdateForm(findConnectorDetail(state.selectedConnectorId));
    }
  });

  document.getElementById("refresh-flows-button").addEventListener("click", () => {
    loadFlows(true);
  });

  document.getElementById("refresh-flow-connectors-button").addEventListener("click", () => {
    loadFlowBuilderConnectors(true);
  });

  document.getElementById("flow-source-kind").addEventListener("change", () => {
    syncFlowConnectorFieldState();
    syncFlowDraftFromForm({ resetAdvancedOverride: false });
  });

  document.getElementById("flow-sink-kind").addEventListener("change", () => {
    syncFlowConnectorFieldState();
    syncFlowDraftFromForm({ resetAdvancedOverride: false });
  });

  document.getElementById("flow-builder-form").addEventListener("input", () => {
    syncFlowDraftFromForm({ resetAdvancedOverride: false });
  });

  document.getElementById("flow-builder-preview-button").addEventListener("click", () => {
    syncFlowDraftFromForm({ resetAdvancedOverride: false });
    setStatus("Flow preview refreshed.");
  });

  document.getElementById("flow-builder-reset-button").addEventListener("click", () => {
    resetFlowBuilder();
  });

  document.getElementById("flow-builder-validate-button").addEventListener("click", async () => {
    try {
      const flow = getEffectiveFlowDraft();
      setStatus("Validating proposed flow...");
      const response = await apiPost("/flows/validate", flow);
      state.cache.flowProposedValidation = response;
      renderProposedFlowValidation(response);
      setStatus("Proposed flow validation completed.");
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("flow-builder-dry-run-button").addEventListener("click", async () => {
    try {
      const flow = getEffectiveFlowDraft();
      const samplePayload = readOptionalJsonTextarea("flow-proposed-sample-payload", "Proposed sample payload JSON");
      const body = { ...flow };
      if (samplePayload !== undefined) {
        body.sample_payload = samplePayload;
      }
      setStatus("Running proposed flow dry-run...");
      const response = await apiPost("/flows/dry-run", body);
      state.cache.flowProposedDryRun = response;
      renderProposedFlowDryRun(response);
      setStatus("Proposed flow dry-run completed.");
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("flow-builder-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const flow = getEffectiveFlowDraft();
      setStatus("Creating flow...");
      const created = await apiPost("/flows", flow);
      state.selectedFlowId = created?.id || null;
      state.cache.flowProposedValidation = null;
      state.cache.flowProposedDryRun = null;
      renderProposedFlowValidation(null);
      renderProposedFlowDryRun(null);
      await loadFlows(true);
      if (created?.id) {
        await loadFlowDetail(created.id, true);
      }
      setStatus(`Flow ${flow.flow_key} created.`);
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("flow-refresh-detail-button").addEventListener("click", () => {
    if (state.selectedFlowId) {
      loadFlowDetail(state.selectedFlowId, true);
    }
  });

  document.getElementById("flow-validate-stored-button").addEventListener("click", async () => {
    if (!state.selectedFlowId) {
      return;
    }
    try {
      setStatus("Loading stored flow validation...");
      const response = await apiGet(`/flows/${encodeURIComponent(state.selectedFlowId)}/validation`);
      state.cache.flowStoredValidation.set(String(state.selectedFlowId), response);
      renderStoredFlowOutputs();
      setStatus("Stored flow validation loaded.");
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("flow-dry-run-stored-button").addEventListener("click", async () => {
    if (!state.selectedFlowId) {
      return;
    }
    try {
      const samplePayload = readOptionalJsonTextarea("flow-stored-sample-payload", "Stored sample payload JSON");
      const body = {};
      if (samplePayload !== undefined) {
        body.sample_payload = samplePayload;
      }
      setStatus("Running stored flow dry-run...");
      const response = await apiPost(`/flows/${encodeURIComponent(state.selectedFlowId)}/dry-run`, body);
      state.cache.flowStoredDryRun.set(String(state.selectedFlowId), response);
      renderStoredFlowOutputs();
      setStatus("Stored flow dry-run completed.");
    } catch (error) {
      handleError(error);
    }
  });

  document.getElementById("flow-enable-button").addEventListener("click", () => {
    if (state.selectedFlowId) {
      setFlowEnabled(state.selectedFlowId, true);
    }
  });

  document.getElementById("flow-disable-button").addEventListener("click", () => {
    if (state.selectedFlowId) {
      setFlowEnabled(state.selectedFlowId, false);
    }
  });

  document.getElementById("flow-delete-button").addEventListener("click", () => {
    if (state.selectedFlowId) {
      deleteSelectedFlow();
    }
  });
}

function renderConfig() {
  document.getElementById("api-base-url").value = state.apiBaseUrl;
  document.getElementById("bearer-token").value = state.bearerToken;
  document.getElementById("current-api-base").textContent = state.apiBaseUrl;
}

function switchSection(section) {
  state.activeSection = section;

  document.querySelectorAll(".nav-tab").forEach((button) => {
    button.classList.toggle("active", button.dataset.section === section);
  });

  document.querySelectorAll(".content-section").forEach((panel) => {
    panel.classList.toggle("active", panel.id === `${section}-section`);
  });

  document.getElementById("section-title").textContent = sectionTitles[section];
  document.getElementById("section-mode").textContent = sectionModes[section];
  clearError();
  refreshCurrentSection();
}

async function refreshCurrentSection(options = {}) {
  const { force = false } = options;

  try {
    clearError();

    if (state.activeSection === "overview") {
      await loadOverview(force);
      return;
    }

    if (state.activeSection === "timeseries") {
      await loadTimeseries(force);
      return;
    }

    if (state.activeSection === "connectors") {
      await loadConnectorsSection(force);
      return;
    }

    if (state.activeSection === "flows") {
      await loadFlows(force);
    }
  } catch (error) {
    handleError(error);
  }
}

async function loadOverview(force) {
  setStatus("Loading overview...");
  const data = force || !state.cache.overview
    ? await apiGet("/dashboard/overview")
    : state.cache.overview;
  state.cache.overview = data;
  renderOverview(data);
  setStatus(withGeneratedAt("Overview loaded.", data.generated_at));
}

async function loadTimeseries(force) {
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

async function loadConnectorsSection(force) {
  setStatus("Loading connector overview and worker state...");
  const [overview, connectors, workerPlan, workerStatus] = await Promise.all([
    force || !state.cache.connectorsOverview
      ? apiGet("/dashboard/connectors/overview")
      : Promise.resolve(state.cache.connectorsOverview),
    force || !state.cache.connectorsList
      ? apiGet("/ingestion/connectors")
      : Promise.resolve(state.cache.connectorsList),
    force || !state.cache.workerPlan
      ? apiGet("/ingestion/workers/plan")
      : Promise.resolve(state.cache.workerPlan),
    force || !state.cache.workerStatus
      ? apiGet("/ingestion/workers/status")
      : Promise.resolve(state.cache.workerStatus),
  ]);

  state.cache.connectorsOverview = overview;
  state.cache.connectorsList = connectors;
  state.cache.workerPlan = workerPlan;
  state.cache.workerStatus = workerStatus;

  renderConnectors(overview, connectors);
  renderWorkerPanels(workerPlan, workerStatus);

  if (!state.selectedConnectorId && connectors.length > 0) {
    state.selectedConnectorId = connectors[0].id;
  }

  if (state.selectedConnectorId) {
    const connector = connectors.find((item) => item.id === state.selectedConnectorId);
    if (connector) {
      await loadConnectorDetail(state.selectedConnectorId, force);
    } else {
      state.selectedConnectorId = null;
      renderConnectorDetail(null);
    }
  } else {
    renderConnectorDetail(null);
  }

  setStatus(withGeneratedAt("Connectors loaded.", overview.generated_at));
}

async function loadWorkerData(force) {
  try {
    setStatus("Refreshing worker plan and runtime state...");
    const [workerPlan, workerStatus] = await Promise.all([
      force || !state.cache.workerPlan ? apiGet("/ingestion/workers/plan") : Promise.resolve(state.cache.workerPlan),
      force || !state.cache.workerStatus ? apiGet("/ingestion/workers/status") : Promise.resolve(state.cache.workerStatus),
    ]);
    state.cache.workerPlan = workerPlan;
    state.cache.workerStatus = workerStatus;
    renderWorkerPanels(workerPlan, workerStatus);
    renderConnectorDetail(findConnectorDetail(state.selectedConnectorId));
    setStatus("Worker plan and runtime state refreshed.");
  } catch (error) {
    handleError(error);
  }
}

async function loadConnectorDetail(connectorId, force) {
  const cacheKey = String(connectorId);
  const connector = force || !state.cache.connectorDetail.has(cacheKey)
    ? await apiGet(`/ingestion/connectors/${encodeURIComponent(connectorId)}`)
    : state.cache.connectorDetail.get(cacheKey);
  state.cache.connectorDetail.set(cacheKey, connector);

  if (force || !state.cache.connectorStatus.has(cacheKey)) {
    const status = await apiGet(`/ingestion/connectors/${encodeURIComponent(connectorId)}/status`);
    state.cache.connectorStatus.set(cacheKey, status);
  }

  renderConnectorDetail(connector);
}

async function loadConnectorStatus(connectorId, force) {
  const cacheKey = String(connectorId);
  const status = force || !state.cache.connectorStatus.has(cacheKey)
    ? await apiGet(`/ingestion/connectors/${encodeURIComponent(connectorId)}/status`)
    : state.cache.connectorStatus.get(cacheKey);
  state.cache.connectorStatus.set(cacheKey, status);
  renderConnectorDetail(findConnectorDetail(connectorId));
}

async function loadConnectorValidation(connectorId, force) {
  const cacheKey = String(connectorId);
  const validation = force || !state.cache.connectorValidation.has(cacheKey)
    ? await apiGet(`/ingestion/connectors/${encodeURIComponent(connectorId)}/validate`)
    : state.cache.connectorValidation.get(cacheKey);
  state.cache.connectorValidation.set(cacheKey, validation);
  renderConnectorDetail(findConnectorDetail(connectorId));
}

async function loadConnectorLivePlan(connectorId, force) {
  const cacheKey = String(connectorId);
  const plan = force || !state.cache.connectorLivePlan.has(cacheKey)
    ? await apiGet(`/ingestion/connectors/${encodeURIComponent(connectorId)}/ttn-live-readiness-plan`)
    : state.cache.connectorLivePlan.get(cacheKey);
  state.cache.connectorLivePlan.set(cacheKey, plan);
  renderConnectorDetail(findConnectorDetail(connectorId));
}

async function setConnectorEnabled(connectorId, enabled) {
  try {
    setStatus(`${enabled ? "Enabling" : "Disabling"} connector...`);
    await apiPut(`/ingestion/connectors/${encodeURIComponent(connectorId)}/${enabled ? "enable" : "disable"}`);
    await loadConnectorsSection(true);
    if (state.selectedConnectorId) {
      await loadConnectorDetail(state.selectedConnectorId, true);
    }
    setStatus(`Connector ${enabled ? "enabled" : "disabled"}.`);
  } catch (error) {
    handleError(error);
  }
}

async function loadFlows(force) {
  setStatus("Loading flow inventory...");
  await loadFlowBuilderConnectors(force);
  const data = force || !state.cache.flows
    ? await apiGet("/dashboard/flows")
    : state.cache.flows;
  state.cache.flows = data;
  renderFlows(data);
  setStatus(withGeneratedAt("Flow inventory loaded.", data.generated_at));

  if (state.selectedFlowId) {
    const selectedStillExists = (data.flows || []).some((flow) => flow.flow_id === state.selectedFlowId);
    if (selectedStillExists) {
      await loadFlowDetail(state.selectedFlowId, force);
    } else {
      state.selectedFlowId = null;
      renderFlowDetail(null);
      renderFlows(data);
    }
  }
}

async function loadFlowDetail(flowId, force) {
  try {
    setStatus("Loading flow detail...");
    state.selectedFlowId = flowId;
    const cacheKey = String(flowId);
    const data = force || !state.cache.flowDetail.has(cacheKey)
      ? await apiGet(`/dashboard/flows/${encodeURIComponent(flowId)}`)
      : state.cache.flowDetail.get(cacheKey);
    state.cache.flowDetail.set(cacheKey, data);
    renderFlowDetail(data);
    renderFlows(state.cache.flows || { flows: [] });
    renderStoredFlowOutputs();
    setStatus(withGeneratedAt("Flow detail loaded.", data.generated_at));
  } catch (error) {
    handleError(error);
  }
}

async function loadFlowBuilderConnectors(force) {
  const connectorPromise = force || !state.cache.flowConnectors
    ? apiGet("/ingestion/connectors")
    : Promise.resolve(state.cache.flowConnectors);
  const overviewPromise = force || !state.cache.flowConnectorOverview
    ? apiGet("/dashboard/connectors/overview")
    : Promise.resolve(state.cache.flowConnectorOverview);

  const [connectorsResult, overviewResult] = await Promise.allSettled([connectorPromise, overviewPromise]);
  const connectors = connectorsResult.status === "fulfilled" ? connectorsResult.value : [];
  const overview = overviewResult.status === "fulfilled" ? overviewResult.value : { connectors: [] };

  state.cache.flowConnectors = connectors;
  state.cache.flowConnectorOverview = overview;
  populateFlowConnectorSelects(connectors, overview);

  if (connectorsResult.status === "rejected") {
    document.getElementById("flow-builder-status").textContent = `Connector selectors unavailable: ${connectorsResult.reason.message}`;
  }
}

async function setFlowEnabled(flowId, enabled) {
  try {
    setStatus(`${enabled ? "Enabling" : "Disabling"} flow...`);
    await apiPut(`/flows/${encodeURIComponent(flowId)}/${enabled ? "enable" : "disable"}`);
    state.cache.flowDetail.delete(String(flowId));
    state.cache.flowStoredValidation.delete(String(flowId));
    state.cache.flowStoredDryRun.delete(String(flowId));
    await loadFlows(true);
    await loadFlowDetail(flowId, true);
    setStatus(`Flow ${enabled ? "enabled" : "disabled"}.`);
  } catch (error) {
    handleError(error);
  }
}

async function deleteSelectedFlow() {
  const detail = state.selectedFlowId ? state.cache.flowDetail.get(String(state.selectedFlowId)) : null;
  const label = detail?.flow?.flow_key || state.selectedFlowId;
  if (!state.selectedFlowId || !window.confirm(`Delete flow ${label}? This cannot be undone.`)) {
    return;
  }

  try {
    setStatus("Deleting flow...");
    await apiDelete(`/flows/${encodeURIComponent(state.selectedFlowId)}`);
    state.cache.flowDetail.delete(String(state.selectedFlowId));
    state.cache.flowStoredValidation.delete(String(state.selectedFlowId));
    state.cache.flowStoredDryRun.delete(String(state.selectedFlowId));
    state.selectedFlowId = null;
    await loadFlows(true);
    renderFlowDetail(null);
    setStatus("Flow deleted.");
  } catch (error) {
    handleError(error);
  }
}

async function apiGet(path) {
  return apiRequest("GET", path);
}

async function apiPost(path, body) {
  return apiRequest("POST", path, body);
}

async function apiPatch(path, body) {
  return apiRequest("PATCH", path, body);
}

async function apiPut(path, body) {
  return apiRequest("PUT", path, body);
}

async function apiDelete(path) {
  return apiRequest("DELETE", path);
}

async function apiRequest(method, path, body) {
  const headers = {
    Accept: "application/json",
  };

  const options = { method, headers };

  if (state.bearerToken) {
    headers.Authorization = `Bearer ${state.bearerToken}`;
  }

  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(body);
  }

  const response = await fetch(`${state.apiBaseUrl}${path}`, options);
  if (!response.ok) {
    throw await buildApiError(path, response);
  }

  if (response.status === 204) {
    return null;
  }

  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    return null;
  }

  return response.json();
}

async function buildApiError(path, response) {
  let detail = "";
  try {
    const body = await response.json();
    detail = body.error || body.message || JSON.stringify(redactSecrets(body));
  } catch (_error) {
    try {
      detail = await response.text();
    } catch (_ignored) {
      detail = "";
    }
  }

  if (response.status === 401) {
    return new Error(`Missing or invalid token for ${path}.${detail ? ` ${detail}` : ""}`);
  }

  if (response.status === 403) {
    return new Error(`Token lacks required scope for ${path}.${detail ? ` ${detail}` : ""}`);
  }

  return new Error(`Request failed for ${path}: ${response.status} ${response.statusText}${detail ? ` - ${detail}` : ""}`);
}

function renderOverview(data) {
  const metrics = [
    ["Entities", data.entities_count],
    ["Observations", data.observations_count],
    ["Raw Messages", data.raw_messages_count],
    ["Events", data.events_count],
    ["Connectors", data.connectors_count, `Enabled: ${data.enabled_connectors_count}`],
    ["Workers", data.workers_running_count, `Degraded: ${data.workers_degraded_count}`],
    ["Flows", data.flows_count, `Enabled: ${data.enabled_flows_count}`],
    ["Invalid Flows", data.invalid_flows_count, `Warnings: ${data.flow_validation_warning_count}`],
    ["DLQ Pending", data.dlq_pending_count, `Total: ${data.dlq_total_count}`],
  ];

  const cards = metrics.map(([label, value, note]) => `
    <article>
      <p class="metric-label">${escapeHtml(label)}</p>
      <p class="metric-value">${formatNumber(value)}</p>
      <p class="metric-note">${note ? escapeHtml(note) : "&nbsp;"}</p>
    </article>
  `);

  document.getElementById("overview-cards").innerHTML = cards.join("");
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

function renderConnectors(overviewData, connectors) {
  const detailsById = new Map((connectors || []).map((connector) => [connector.id, connector]));
  const rows = (overviewData.connectors || []).map((connector) => {
    const detail = detailsById.get(connector.connector_id);
    const selected = state.selectedConnectorId === connector.connector_id ? " selected" : "";
    return `
      <tr class="interactive-row${selected}" data-connector-id="${escapeHtml(connector.connector_id)}">
        <td>
          <button class="button subtle table-select" type="button" data-connector-select="${escapeHtml(connector.connector_id)}">${escapeHtml(connector.connector_key)}</button>
          <div class="status-meta">${escapeHtml(detail?.display_name || detail?.connector_key || connector.connector_id)}</div>
        </td>
        <td>${escapeHtml(connector.connector_type || "n/a")}</td>
        <td>${escapeHtml(connector.connector_profile || "n/a")}</td>
        <td>${badge(connector.enabled ? "enabled" : "disabled", connector.enabled ? "success" : "warning")}</td>
        <td>${badge(connector.readiness || connector.status || "unknown", connectorBadgeTone(connector))}</td>
        <td>${escapeHtml(safeBrokerUrl(connector.broker_url) || "n/a")}</td>
        <td>${escapeHtml(connector.topic_filter || "n/a")}</td>
        <td>${escapeHtml(connector.payload_format || "n/a")}</td>
        <td>${escapeHtml(`${connector.worker_kind || "n/a"} / ${connector.worker_status || "n/a"}`)}</td>
        <td>${escapeHtml(connector.last_error || "n/a")}</td>
        <td>${badge(connector.secret_configured ? "configured" : "not configured", connector.secret_configured ? "success" : "warning")}</td>
      </tr>
    `;
  });

  document.getElementById("connectors-table-body").innerHTML = rows.join("") || buildEmptyRow(11, "No connectors returned.");

  document.querySelectorAll("[data-connector-select]").forEach((button) => {
    button.addEventListener("click", () => {
      selectConnector(button.dataset.connectorSelect);
    });
  });
}

function renderWorkerPanels(plan, status) {
  renderWorkerSummaryCards(plan, status);

  const specsByConnectorId = new Map((plan.specs || []).map((spec) => [spec.connector_id, spec]));
  const rows = (status.workers || []).map((worker) => {
    const spec = specsByConnectorId.get(worker.connector_id);
    return `
      <tr class="interactive-row${state.selectedConnectorId === worker.connector_id ? " selected" : ""}" data-worker-connector-id="${escapeHtml(worker.connector_id)}">
        <td>
          <button class="button subtle table-select" type="button" data-worker-select="${escapeHtml(worker.connector_id)}">${escapeHtml(worker.connector_key)}</button>
          <div class="status-meta">${escapeHtml(worker.connector_id)}</div>
        </td>
        <td>${escapeHtml(worker.worker_kind || "n/a")}</td>
        <td>${badge(spec?.status || "n/a", workerPlanTone(spec?.status))}</td>
        <td>${badge(worker.status || "unknown", workerRuntimeTone(worker.status))}</td>
        <td>${badge(worker.enabled ? "enabled" : "disabled", worker.enabled ? "success" : "warning")}</td>
        <td>${badge(worker.connected ? "connected" : "not connected", worker.connected ? "success" : "warning")}</td>
        <td>${badge(worker.subscribed ? "subscribed" : "not subscribed", worker.subscribed ? "success" : "warning")}</td>
        <td>${escapeHtml(String(worker.reconnect_attempts ?? 0))}</td>
        <td>${escapeHtml(formatDateTime(worker.started_at))}</td>
        <td>${escapeHtml(formatDateTime(worker.stopped_at))}</td>
        <td>${escapeHtml(formatDateTime(worker.last_reconciled_at))}</td>
        <td>${escapeHtml(worker.last_error || joinIssueMessages(spec?.validation_issues) || "n/a")}</td>
      </tr>
    `;
  });

  document.getElementById("workers-table-body").innerHTML = rows.join("") || buildEmptyRow(12, "No worker runtime entries returned.");

  document.querySelectorAll("[data-worker-select]").forEach((button) => {
    button.addEventListener("click", () => {
      selectConnector(button.dataset.workerSelect);
    });
  });
}

function renderWorkerSummaryCards(plan, status) {
  const readiness = status.connector_workers || {};
  const cards = [
    ["Worker Runtime Enabled", readiness.enabled ? "true" : "false", "GET /ingestion/workers/status"],
    ["Planned Workers", plan.planned_workers, "GET /ingestion/workers/plan"],
    ["Skipped Workers", plan.skipped_workers, "GET /ingestion/workers/plan"],
    ["Invalid Workers", plan.invalid_workers, "GET /ingestion/workers/plan"],
    ["Running Workers", readiness.running, "GET /ingestion/workers/status"],
    ["Degraded Workers", readiness.degraded, "GET /ingestion/workers/status"],
    ["Stopped Workers", readiness.stopped, "GET /ingestion/workers/status"],
    ["Error Count", readiness.errors, "GET /ingestion/workers/status"],
  ];

  document.getElementById("worker-summary-cards").innerHTML = cards.map(([label, value, note]) => `
    <article>
      <p class="metric-label">${escapeHtml(label)}</p>
      <p class="metric-value">${escapeHtml(String(value ?? 0))}</p>
      <p class="metric-note">${escapeHtml(note)}</p>
    </article>
  `).join("");
}

function selectConnector(connectorId) {
  state.selectedConnectorId = connectorId;
  clearError();
  if (state.cache.connectorsOverview && state.cache.connectorsList) {
    renderConnectors(state.cache.connectorsOverview, state.cache.connectorsList);
  }
  if (state.cache.workerPlan && state.cache.workerStatus) {
    renderWorkerPanels(state.cache.workerPlan, state.cache.workerStatus);
  }
  loadConnectorDetail(connectorId, false).catch(handleError);
}

function renderConnectorDetail(connector) {
  const emptyState = document.getElementById("connector-detail-empty");
  const content = document.getElementById("connector-detail-content");
  const updateForm = document.getElementById("connector-update-form");
  const updateReset = document.getElementById("connector-update-reset");
  const enableButton = document.getElementById("connector-enable-button");
  const disableButton = document.getElementById("connector-disable-button");
  const refreshDetailButton = document.getElementById("connector-refresh-detail");
  const refreshStatusButton = document.getElementById("connector-refresh-status");
  const validateButton = document.getElementById("connector-validate-button");
  const livePlanButton = document.getElementById("connector-live-plan-button");

  const hasConnector = Boolean(connector);
  refreshDetailButton.disabled = !hasConnector;
  refreshStatusButton.disabled = !hasConnector;
  validateButton.disabled = !hasConnector;
  livePlanButton.disabled = !hasConnector;

  Array.from(updateForm.elements).forEach((element) => {
    element.disabled = !hasConnector;
  });
  updateReset.disabled = !hasConnector;

  if (!connector) {
    emptyState.classList.remove("hidden");
    content.classList.add("hidden");
    document.getElementById("connector-detail-title").textContent = "Select a connector to inspect details, runtime status, validation, and safe update actions.";
    document.getElementById("connector-detail-metadata").innerHTML = "";
    document.getElementById("connector-status-summary").innerHTML = "";
    document.getElementById("connector-worker-plan-summary").innerHTML = "";
    document.getElementById("connector-worker-runtime-summary").innerHTML = "";
    document.getElementById("connector-validation-summary").textContent = "TTN validation is manual and loaded only when requested.";
    document.getElementById("connector-live-plan-summary").textContent = "Dry-run readiness planning is manual. No live validation is triggered automatically.";
    document.getElementById("connector-json-preview").textContent = "";
    enableButton.disabled = true;
    disableButton.disabled = true;
    updateForm.reset();
    return;
  }

  emptyState.classList.add("hidden");
  content.classList.remove("hidden");

  const connectorStatus = findConnectorStatus(connector.id);
  const workerPlan = findWorkerPlan(connector.id);
  const workerRuntime = findWorkerRuntime(connector.id);
  const validation = findConnectorValidation(connector.id);
  const livePlan = findConnectorLivePlan(connector.id);

  document.getElementById("connector-detail-title").textContent = `${connector.connector_key} (${connector.id})`;

  const metadata = [
    ["Connector Key", connector.connector_key],
    ["Display Name", connector.display_name || "n/a"],
    ["Type", connector.connector_type],
    ["Profile", connector.connector_profile],
    ["Enabled", connector.enabled ? "true" : "false"],
    ["Protocol", connector.protocol || "n/a"],
    ["Broker URL", safeBrokerUrl(connector.broker_url) || "n/a"],
    ["Topic Filter", connector.topic_filter || "n/a"],
    ["Payload Format", connector.payload_format || "n/a"],
    ["Content Type", connector.content_type || "n/a"],
    ["Secret Ref ID", connector.secret_ref_id || "n/a"],
    ["Updated", formatDateTime(connector.updated_at)],
  ];

  document.getElementById("connector-detail-metadata").innerHTML = metadata.map(([label, value]) => `
    <div class="detail-item">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(String(value))}</span>
    </div>
  `).join("");

  document.getElementById("connector-status-summary").innerHTML = renderKeyValueBlock([
    ["Status", badge(connectorStatus?.status || "unknown", workerRuntimeTone(connectorStatus?.status))],
    ["Last Error", connectorStatus?.last_error || "n/a"],
    ["Last Message", formatDateTime(connectorStatus?.last_message_at)],
    ["Last Successful Ingest", formatDateTime(connectorStatus?.last_successful_ingest_at)],
    ["Last Failed Ingest", formatDateTime(connectorStatus?.last_failed_ingest_at)],
  ], true);

  document.getElementById("connector-worker-plan-summary").innerHTML = renderKeyValueBlock([
    ["Worker Kind", workerPlan?.worker_kind || "n/a"],
    ["Planned Status", workerPlan?.status || "n/a"],
    ["Client ID", workerPlan?.client_id || connector.client_id || "n/a"],
    ["HTTP Path", workerPlan?.http_path || connector.http_path || connector.endpoint || "n/a"],
    ["Validation Issues", joinIssueMessages(workerPlan?.validation_issues) || "n/a"],
  ]);

  document.getElementById("connector-worker-runtime-summary").innerHTML = renderKeyValueBlock([
    ["Runtime Status", badge(workerRuntime?.status || "n/a", workerRuntimeTone(workerRuntime?.status))],
    ["Connected", String(workerRuntime?.connected ?? false)],
    ["Subscribed", String(workerRuntime?.subscribed ?? false)],
    ["Reconnect Attempts", String(workerRuntime?.reconnect_attempts ?? 0)],
    ["Started At", formatDateTime(workerRuntime?.started_at)],
    ["Stopped At", formatDateTime(workerRuntime?.stopped_at)],
    ["Last Reconciled", formatDateTime(workerRuntime?.last_reconciled_at)],
  ], true);

  document.getElementById("connector-validation-summary").innerHTML = validation
    ? renderValidationSummary(validation)
    : "TTN validation is manual and loaded only when requested.";

  document.getElementById("connector-live-plan-summary").innerHTML = livePlan
    ? renderLivePlanSummary(livePlan)
    : "Dry-run readiness planning is manual. No live validation is triggered automatically.";

  document.getElementById("connector-json-preview").textContent = JSON.stringify(redactSecrets(connector), null, 2);

  enableButton.disabled = connector.enabled;
  disableButton.disabled = !connector.enabled;
  populateUpdateForm(connector);
}

function renderValidationSummary(validation) {
  const issues = joinIssueCodes(validation.issues);
  const warnings = joinIssueCodes(validation.warnings);
  return renderKeyValueBlock([
    ["Readiness", badge(validation.readiness || "unknown", workerRuntimeTone(validation.readiness))],
    ["Valid", String(validation.valid)],
    ["Mappings", `${validation.enabled_mapping_count ?? 0} enabled / ${validation.mapping_count ?? 0} total`],
    ["Secret Configured", String(validation.secret_configured ?? false)],
    ["Issues", issues || "none"],
    ["Warnings", warnings || "none"],
  ], true);
}

function renderLivePlanSummary(plan) {
  const blockers = joinIssueCodes(plan.blockers);
  const warnings = joinIssueCodes(plan.warnings);
  return renderKeyValueBlock([
    ["Dry Run", String(plan.dry_run)],
    ["Readiness", badge(plan.readiness || "unknown", workerRuntimeTone(plan.readiness))],
    ["Safe To Connect", String(plan.safe_to_connect)],
    ["Can Attempt Live Validation", String(plan.can_attempt_live_validation)],
    ["Blockers", blockers || "none"],
    ["Warnings", warnings || "none"],
  ], true);
}

function populateUpdateForm(connector) {
  const form = document.getElementById("connector-update-form");
  const redactedMetadata = connector.metadata ? redactSecrets(connector.metadata) : null;
  const metadataWasRedacted = connector.metadata && JSON.stringify(redactedMetadata) !== JSON.stringify(connector.metadata);
  form.elements.display_name.value = connector.display_name || "";
  form.elements.broker_url.value = connector.broker_url || "";
  form.elements.client_id.value = connector.client_id || "";
  form.elements.topic_filter.value = connector.topic_filter || "";
  form.elements.http_path.value = connector.http_path || "";
  form.elements.endpoint.value = connector.endpoint || "";
  form.elements.payload_format.value = connector.payload_format || "";
  form.elements.content_type.value = connector.content_type || "";
  form.elements.default_producer_entity_id.value = connector.default_producer_entity_id || "";
  form.elements.default_feature_of_interest_id.value = connector.default_feature_of_interest_id || "";
  form.elements.secret_ref_id.value = connector.secret_ref_id || "";
  form.elements.metadata.value = metadataWasRedacted ? "" : (connector.metadata ? JSON.stringify(connector.metadata, null, 2) : "");
  form.elements.metadata.placeholder = metadataWasRedacted
    ? "Metadata contains redacted keys in preview. Enter replacement JSON only if you intend to overwrite metadata."
    : '{"note":"safe operational metadata only"}';
}

function readConnectorFormPayload(form, options) {
  const requireKey = Boolean(options?.requireKey);
  const formData = new FormData(form);
  const payload = {};

  if (requireKey) {
    const connectorKey = cleanString(formData.get("connector_key"));
    if (!connectorKey) {
      throw new Error("Connector key is required.");
    }
    payload.connector_key = connectorKey;
  }

  if (requireKey) {
    payload.connector_type = cleanString(formData.get("connector_type"));
    payload.connector_profile = cleanString(formData.get("connector_profile"));
    payload.enabled = Boolean(form.elements.enabled?.checked);
  }

  addOptionalField(payload, "display_name", formData.get("display_name"));
  addOptionalField(payload, "protocol", formData.get("protocol"));
  addOptionalField(payload, "endpoint", formData.get("endpoint"));
  addOptionalField(payload, "broker_url", formData.get("broker_url"));
  addOptionalField(payload, "client_id", formData.get("client_id"));
  addOptionalField(payload, "topic_filter", formData.get("topic_filter"));
  addOptionalField(payload, "http_path", formData.get("http_path"));
  addOptionalField(payload, "payload_format", formData.get("payload_format"));
  addOptionalField(payload, "content_type", formData.get("content_type"));
  addOptionalUuidField(payload, "secret_ref_id", formData.get("secret_ref_id"));
  addOptionalUuidField(payload, "default_producer_entity_id", formData.get("default_producer_entity_id"));
  addOptionalUuidField(payload, "default_feature_of_interest_id", formData.get("default_feature_of_interest_id"));

  const metadataText = cleanString(formData.get("metadata"));
  if (metadataText) {
    payload.metadata = parseJson(metadataText, "Metadata JSON");
  }

  if (!requireKey && Object.keys(payload).length === 0) {
    throw new Error("No patch fields were provided.");
  }

  return payload;
}

function addOptionalField(target, key, value, transform) {
  const cleaned = cleanString(value);
  if (cleaned) {
    target[key] = transform ? transform(cleaned) : cleaned;
  }
}

function addOptionalUuidField(target, key, value) {
  const cleaned = cleanString(value);
  if (!cleaned) {
    return;
  }
  if (!isUuid(cleaned)) {
    throw new Error(`${labelFromFieldName(key)} must be a valid UUID.`);
  }
  target[key] = cleaned;
}

function populateFlowConnectorSelects(connectors, overview) {
  const sourceSelect = document.getElementById("flow-source-connector");
  const sinkSelect = document.getElementById("flow-sink-connector");
  const selectedSource = sourceSelect.value;
  const selectedSink = sinkSelect.value;
  const overviewById = new Map((overview?.connectors || []).map((item) => [item.connector_id, item]));
  const options = (connectors || []).map((connector) => {
    const summary = overviewById.get(connector.id);
    const label = connector.display_name || connector.connector_key || connector.id;
    const suffix = [connector.connector_type, summary?.payload_format].filter(Boolean).join(" / ");
    return {
      value: connector.id,
      label: suffix ? `${label} (${suffix})` : label,
    };
  });
  const html = ['<option value="">None</option>'].concat(options.map((option) => `
    <option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>
  `)).join("");
  sourceSelect.innerHTML = html;
  sinkSelect.innerHTML = html;
  sourceSelect.value = options.some((item) => item.value === selectedSource) ? selectedSource : "";
  sinkSelect.value = options.some((item) => item.value === selectedSink) ? selectedSink : "";
  syncFlowConnectorFieldState();
}

function syncFlowConnectorFieldState() {
  const sourceKind = cleanString(document.getElementById("flow-source-kind").value);
  const sinkKind = cleanString(document.getElementById("flow-sink-kind").value);
  const sourceConnector = document.getElementById("flow-source-connector");
  const sinkConnector = document.getElementById("flow-sink-connector");
  sourceConnector.disabled = !FLOW_SOURCE_KINDS_WITH_CONNECTOR.has(sourceKind);
  sinkConnector.disabled = !FLOW_SINK_KINDS_WITH_CONNECTOR.has(sinkKind);
  if (sourceConnector.disabled) {
    sourceConnector.value = "";
  }
  if (sinkConnector.disabled) {
    sinkConnector.value = "";
  }
}

function resetFlowBuilder() {
  document.getElementById("flow-builder-form").reset();
  document.getElementById("flow-advanced-json").value = "";
  document.getElementById("flow-proposed-sample-payload").value = DEFAULT_FLOW_SAMPLE_PAYLOAD;
  initializeFlowBuilder();
  state.cache.flowProposedValidation = null;
  state.cache.flowProposedDryRun = null;
  renderProposedFlowValidation(null);
  renderProposedFlowDryRun(null);
  setStatus("Flow builder reset.");
}

function syncFlowDraftFromForm(options = {}) {
  const { resetAdvancedOverride = false } = options;
  if (resetAdvancedOverride) {
    document.getElementById("flow-advanced-json").value = "";
  }
  state.flowDraft = buildFlowDraftFromForm();
  renderFlowDraftPreview();
}

function buildFlowDraftFromForm() {
  const form = document.getElementById("flow-builder-form");
  const flowKey = requireNonEmpty(form.elements.flow_key.value, "Flow key");
  const name = requireNonEmpty(form.elements.name.value, "Name");
  const sourceNodeId = requireNonEmpty(form.elements.source_node_id.value, "Source node ID");
  const transformNodeId = requireNonEmpty(form.elements.transform_node_id.value, "Transform node ID");
  const sinkNodeId = requireNonEmpty(form.elements.sink_node_id.value, "Sink node ID");
  const sourceKind = requireNonEmpty(form.elements.source_kind.value, "Source kind");
  const transformKind = requireNonEmpty(form.elements.transform_kind.value, "Transform kind");
  const sinkKind = requireNonEmpty(form.elements.sink_kind.value, "Sink kind");
  const metadata = readOptionalJsonText(form.elements.metadata.value, "Metadata JSON");

  const sourceConnectorId = cleanString(form.elements.source_connector_id.value);
  const sinkConnectorId = cleanString(form.elements.sink_connector_id.value);

  const sourceNodeType = sourceKind === "internal_observation" ? "source" : "source";
  const transformNodeType = transformKind === "filter_condition" ? "filter" : "decoder";
  const sinkNodeType = sinkKind === "dlq" ? "dlq" : "sink";

  const sourceConfig = { kind: sourceKind };
  if (FLOW_SOURCE_KINDS_WITH_CONNECTOR.has(sourceKind) && sourceConnectorId) {
    sourceConfig.connector_id = sourceConnectorId;
  }

  const transformConfig = { kind: transformKind };
  const sinkConfig = { kind: sinkKind };
  if (FLOW_SINK_KINDS_WITH_CONNECTOR.has(sinkKind) && sinkConnectorId) {
    sinkConfig.connector_id = sinkConnectorId;
  }

  return {
    flow_key: flowKey,
    name,
    description: cleanString(form.elements.description.value) || undefined,
    enabled: Boolean(form.elements.enabled.checked),
    nodes: [
      {
        node_id: sourceNodeId,
        node_type: sourceNodeType,
        name: cleanString(form.elements.source_name.value) || undefined,
        config: sourceConfig,
        position: { x: 60, y: 120 },
      },
      {
        node_id: transformNodeId,
        node_type: transformNodeType,
        name: cleanString(form.elements.transform_name.value) || undefined,
        config: transformConfig,
        position: { x: 300, y: 120 },
      },
      {
        node_id: sinkNodeId,
        node_type: sinkNodeType,
        name: cleanString(form.elements.sink_name.value) || undefined,
        config: sinkConfig,
        position: { x: 540, y: 120 },
      },
    ],
    edges: [
      {
        edge_id: `${sourceNodeId}-to-${transformNodeId}`,
        source_node_id: sourceNodeId,
        target_node_id: transformNodeId,
      },
      {
        edge_id: `${transformNodeId}-to-${sinkNodeId}`,
        source_node_id: transformNodeId,
        target_node_id: sinkNodeId,
      },
    ],
    metadata,
  };
}

function getEffectiveFlowDraft() {
  const advanced = cleanString(document.getElementById("flow-advanced-json").value);
  if (advanced) {
    const parsed = parseJson(advanced, "Advanced JSON override");
    validateFlowDraftShape(parsed);
    state.flowDraft = parsed;
    renderFlowDraftPreview();
    return parsed;
  }

  if (!state.flowDraft) {
    syncFlowDraftFromForm({ resetAdvancedOverride: false });
  }
  validateFlowDraftShape(state.flowDraft);
  return state.flowDraft;
}

function validateFlowDraftShape(draft) {
  if (!draft || typeof draft !== "object") {
    throw new Error("Flow draft must be a JSON object.");
  }
  if (!cleanString(draft.flow_key)) {
    throw new Error("Flow draft requires flow_key.");
  }
  if (!cleanString(draft.name)) {
    throw new Error("Flow draft requires name.");
  }
  if (!Array.isArray(draft.nodes) || draft.nodes.length === 0) {
    throw new Error("Flow draft requires nodes.");
  }
  if (!Array.isArray(draft.edges) || draft.edges.length === 0) {
    throw new Error("Flow draft requires edges.");
  }
}

function renderFlowDraftPreview() {
  const preview = document.getElementById("flow-preview-json");
  const builderStatus = document.getElementById("flow-builder-status");
  try {
    const draft = cleanString(document.getElementById("flow-advanced-json").value)
      ? parseJson(document.getElementById("flow-advanced-json").value, "Advanced JSON override")
      : state.flowDraft || buildFlowDraftFromForm();
    preview.textContent = JSON.stringify(redactSecrets(draft), null, 2);
    builderStatus.textContent = cleanString(document.getElementById("flow-advanced-json").value)
      ? "Advanced JSON override is active. Preview stays redacted for display."
      : "Guided builder output is active. Preview stays redacted for display.";
  } catch (error) {
    preview.textContent = error.message;
    builderStatus.textContent = "Fix the builder fields or advanced JSON before validating, dry-running, or creating the flow.";
  }
}

function renderProposedFlowValidation(response) {
  const container = document.getElementById("flow-proposed-validation-summary");
  if (!response) {
    container.textContent = "Validate the proposed flow to inspect structured issues before saving.";
    return;
  }
  container.innerHTML = renderFlowValidationResponse(response);
}

function renderProposedFlowDryRun(response) {
  const container = document.getElementById("flow-proposed-dry-run-summary");
  if (!response) {
    container.textContent = "Dry-run stays planning-only. No side effects are performed.";
    return;
  }
  container.innerHTML = renderFlowDryRunResponse(response);
}

function renderStoredFlowOutputs() {
  const validation = state.selectedFlowId ? state.cache.flowStoredValidation.get(String(state.selectedFlowId)) : null;
  const dryRun = state.selectedFlowId ? state.cache.flowStoredDryRun.get(String(state.selectedFlowId)) : null;
  document.getElementById("flow-stored-validation-summary").innerHTML = validation
    ? renderFlowValidationResponse(validation)
    : "Load validation only when explicitly requested.";
  document.getElementById("flow-stored-dry-run-summary").innerHTML = dryRun
    ? renderFlowDryRunResponse(dryRun)
    : "Load dry-run only when explicitly requested.";
}

function renderFlowValidationResponse(response) {
  const errors = (response.validation_issues || []).filter((issue) => issue.severity === "error");
  const warnings = (response.validation_issues || []).filter((issue) => issue.severity === "warning");
  return `
    <div class="result-stack">
      <div class="detail-grid">
        ${renderDetailItem("Valid", String(response.valid))}
        ${renderDetailItem("Errors", formatNumber(errors.length))}
        ${renderDetailItem("Warnings", formatNumber(warnings.length))}
        ${renderDetailItem("Connectors", formatNumber((response.referenced_connectors || []).length))}
        ${renderDetailItem("Planned Sinks", formatNumber((response.planned_sinks || []).length))}
      </div>
      <div>${renderIssueList(response.validation_issues, "No validation issues returned.")}</div>
    </div>
  `;
}

function renderFlowDryRunResponse(response) {
  return `
    <div class="result-stack">
      <div class="detail-grid">
        ${renderDetailItem("Valid", String(response.valid))}
        ${renderDetailItem("Execution Supported", String(response.execution_supported))}
        ${renderDetailItem("Side Effects Performed", String(response.side_effects_performed))}
        ${renderDetailItem("Sample Payload", String(response.sample_payload_provided))}
        ${renderDetailItem("Would Store Observation", String(response.would_store_observation))}
        ${renderDetailItem("Would Publish MQTT", String(response.would_publish_mqtt))}
        ${renderDetailItem("Would Forward HTTP", String(response.would_forward_http))}
        ${renderDetailItem("Would Create Event", String(response.would_create_event))}
        ${renderDetailItem("Would Create Command", String(response.would_create_command))}
        ${renderDetailItem("Would Use DLQ", String(response.would_use_dlq))}
      </div>
      <p class="preview-note"><strong>Planned Path:</strong> ${escapeHtml((response.planned_path || []).join(" -> ") || "None")}</p>
      <p class="preview-note"><strong>Planned Sinks:</strong> ${escapeHtml((response.planned_sinks || []).map(formatPlannedSink).join(" | ") || "None")}</p>
      <p class="preview-note"><strong>Referenced Connectors:</strong> ${escapeHtml((response.referenced_connectors || []).map(formatReferencedConnector).join(" | ") || "None")}</p>
      <div>${renderIssueList(response.validation_issues, "No validation issues returned.")}</div>
    </div>
  `;
}

function renderIssueList(issues, emptyMessage) {
  if (!issues || issues.length === 0) {
    return `<p>${escapeHtml(emptyMessage)}</p>`;
  }
  return `<ul class="issue-list">${issues.map((issue) => `
    <li>${escapeHtml(formatFlowIssue(issue))}</li>
  `).join("")}</ul>`;
}

function formatFlowIssue(issue) {
  if (!issue || typeof issue !== "object") {
    return "unknown issue";
  }
  return [
    issue.severity || "unknown",
    issue.code || "code?",
    issue.message || "message?",
    issue.node_id ? `node=${issue.node_id}` : "",
    issue.edge_id ? `edge=${issue.edge_id}` : "",
    issue.field ? `field=${issue.field}` : "",
  ].filter(Boolean).join(" | ");
}

function renderDetailItem(label, value) {
  return `
    <div class="detail-item">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(String(value))}</span>
    </div>
  `;
}

function readOptionalJsonTextarea(elementId, label) {
  return readOptionalJsonText(document.getElementById(elementId).value, label);
}

function readOptionalJsonText(value, label) {
  const text = cleanString(value);
  if (!text) {
    return undefined;
  }
  return parseJson(text, label);
}

function requireNonEmpty(value, label) {
  const cleaned = cleanString(value);
  if (!cleaned) {
    throw new Error(`${label} is required.`);
  }
  return cleaned;
}

function renderFlows(data) {
  const rows = (data.flows || []).map((flow) => `
    <tr class="interactive-row${state.selectedFlowId === flow.flow_id ? " selected" : ""}" data-flow-id="${escapeHtml(flow.flow_id)}">
      <td>
        <button class="button subtle flow-select-button" type="button" data-flow-id="${escapeHtml(flow.flow_id)}">${escapeHtml(flow.flow_key)}</button>
        <div class="status-meta">${escapeHtml(flow.name || "n/a")}</div>
      </td>
      <td>${badge(flow.enabled ? "enabled" : "disabled", flow.enabled ? "success" : "warning")}</td>
      <td>${formatNumber(flow.node_count)}</td>
      <td>${formatNumber(flow.edge_count)}</td>
      <td>${formatNumber(flow.source_count)}</td>
      <td>${formatNumber(flow.sink_count)}</td>
      <td>${formatNumber(flow.dlq_count)}</td>
      <td>${badge(flow.validation_status || "unknown", validationBadgeTone(flow.validation_status))}</td>
      <td>${formatNumber(flow.validation_error_count)}</td>
      <td>${formatNumber(flow.validation_warning_count)}</td>
    </tr>
  `);

  document.getElementById("flows-table-body").innerHTML = rows.join("") || buildEmptyRow(10, "No flows returned.");

  document.querySelectorAll(".flow-select-button").forEach((button) => {
    button.addEventListener("click", () => {
      loadFlowDetail(button.dataset.flowId, false);
    });
  });
}

function renderFlowDetail(data) {
  const emptyState = document.getElementById("flow-detail-empty");
  const content = document.getElementById("flow-detail-content");
  const refreshButton = document.getElementById("flow-refresh-detail-button");
  const validateButton = document.getElementById("flow-validate-stored-button");
  const dryRunButton = document.getElementById("flow-dry-run-stored-button");
  const enableButton = document.getElementById("flow-enable-button");
  const disableButton = document.getElementById("flow-disable-button");
  const deleteButton = document.getElementById("flow-delete-button");

  if (!data) {
    emptyState.classList.remove("hidden");
    content.classList.add("hidden");
    document.getElementById("flow-detail-title").textContent = "Select a flow to inspect metadata, validation, graph summary, nodes, and edges.";
    document.getElementById("flow-detail-metadata").innerHTML = "";
    document.getElementById("flow-validation-summary").innerHTML = "";
    document.getElementById("flow-execution-summary").innerHTML = "";
    document.getElementById("flow-stored-validation-summary").textContent = "Load validation only when explicitly requested.";
    document.getElementById("flow-stored-dry-run-summary").textContent = "Load dry-run only when explicitly requested.";
    document.getElementById("flow-nodes-table-body").innerHTML = "";
    document.getElementById("flow-edges-table-body").innerHTML = "";
    renderTokenList("flow-planned-path", [], (item) => item);
    renderTokenList("flow-referenced-connectors", [], formatReferencedConnector);
    renderTokenList("flow-planned-sinks", [], formatPlannedSink);
    refreshButton.disabled = true;
    validateButton.disabled = true;
    dryRunButton.disabled = true;
    enableButton.disabled = true;
    disableButton.disabled = true;
    deleteButton.disabled = true;
    return;
  }

  emptyState.classList.add("hidden");
  content.classList.remove("hidden");
  refreshButton.disabled = false;
  validateButton.disabled = false;
  dryRunButton.disabled = false;
  enableButton.disabled = Boolean(data.flow.enabled);
  disableButton.disabled = !data.flow.enabled;
  deleteButton.disabled = false;

  document.getElementById("flow-detail-title").textContent = `${data.flow.flow_key} (${data.flow.flow_id})`;

  const metadata = [
    ["Flow Key", data.flow.flow_key],
    ["Name", data.flow.name],
    ["Enabled", data.flow.enabled ? "true" : "false"],
    ["Description", data.flow.description || "n/a"],
    ["Created", formatDateTime(data.flow.created_at)],
    ["Updated", formatDateTime(data.flow.updated_at)],
    ["Nodes", data.graph_summary.node_count],
    ["Edges", data.graph_summary.edge_count],
    ["Sources", data.graph_summary.source_count],
    ["Sinks", data.graph_summary.sink_count],
    ["DLQ", data.graph_summary.dlq_count],
    ["Validation", data.validation_summary.status],
  ];

  document.getElementById("flow-detail-metadata").innerHTML = metadata.map(([label, value]) => `
    <div class="detail-item">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(String(value))}</span>
    </div>
  `).join("");

  document.getElementById("flow-validation-summary").innerHTML = `
    <p>${badge(data.validation_summary.status || "unknown", validationBadgeTone(data.validation_summary.status))}</p>
    <p>Valid: <strong>${escapeHtml(String(data.validation_summary.valid))}</strong></p>
    <p>Errors: <strong>${formatNumber(data.validation_summary.error_count)}</strong></p>
    <p>Warnings: <strong>${formatNumber(data.validation_summary.warning_count)}</strong></p>
    <p>Issues: <strong>${formatNumber((data.validation_summary.issues || []).length)}</strong></p>
  `;

  document.getElementById("flow-execution-summary").innerHTML = `
    <p>Execution supported: <strong>${escapeHtml(String(data.execution_supported))}</strong></p>
    <p>Status: <strong>${escapeHtml(data.execution_status || "n/a")}</strong></p>
    <p>Side effects performed: <strong>${escapeHtml(String(data.side_effects_performed))}</strong></p>
  `;

  renderTokenList("flow-planned-path", data.planned_path, (item) => item);
  renderTokenList("flow-referenced-connectors", data.referenced_connectors, formatReferencedConnector);
  renderTokenList("flow-planned-sinks", data.planned_sinks, formatPlannedSink);

  const nodeRows = (data.nodes || []).map((node) => `
    <tr>
      <td>${escapeHtml(node.node_id)}</td>
      <td>${escapeHtml(node.node_type || "n/a")}</td>
      <td>${escapeHtml(node.name || "n/a")}</td>
      <td>${escapeHtml(formatPosition(node.position))}</td>
      <td><pre class="mono-block">${escapeHtml(JSON.stringify(redactSecrets(node.config), null, 2))}</pre></td>
    </tr>
  `);
  document.getElementById("flow-nodes-table-body").innerHTML = nodeRows.join("") || buildEmptyRow(5, "No nodes returned.");

  const edgeRows = (data.edges || []).map((edge) => `
    <tr>
      <td>${escapeHtml(edge.edge_id || "n/a")}</td>
      <td>${escapeHtml(edge.source_node_id || "n/a")}</td>
      <td>${escapeHtml(edge.target_node_id || "n/a")}</td>
      <td>${escapeHtml(edge.label || "n/a")}</td>
    </tr>
  `);
  document.getElementById("flow-edges-table-body").innerHTML = edgeRows.join("") || buildEmptyRow(4, "No edges returned.");
  renderStoredFlowOutputs();
}

function renderTokenList(elementId, values, formatter) {
  const items = (values || []).map((value) => `<li>${escapeHtml(formatter(value))}</li>`);
  document.getElementById(elementId).innerHTML = items.join("") || "<li>None</li>";
}

function renderKeyValueBlock(items, allowHtml = false) {
  return items.map(([label, value]) => `
    <p><strong>${escapeHtml(label)}:</strong> ${allowHtml ? (value || "n/a") : escapeHtml(value || "n/a")}</p>
  `).join("");
}

function formatReferencedConnector(connector) {
  if (!connector || typeof connector !== "object") {
    return "n/a";
  }

  return [
    connector.node_id || "node?",
    connector.connector_id || "connector?",
    connector.connector_key || "key?",
    connector.role || "role?",
  ].join(" | ");
}

function formatPlannedSink(sink) {
  if (!sink || typeof sink !== "object") {
    return "n/a";
  }

  return [
    sink.node_id || "node?",
    sink.kind || sink.sink_kind || "sink?",
    sink.description || sink.summary || "",
  ].filter(Boolean).join(" | ");
}

function formatPosition(position) {
  if (!position || typeof position !== "object") {
    return "n/a";
  }

  return `x=${position.x ?? "?"}, y=${position.y ?? "?"}`;
}

function findConnectorDetail(connectorId) {
  return connectorId ? state.cache.connectorDetail.get(String(connectorId)) || null : null;
}

function findConnectorStatus(connectorId) {
  return connectorId ? state.cache.connectorStatus.get(String(connectorId)) || null : null;
}

function findConnectorValidation(connectorId) {
  return connectorId ? state.cache.connectorValidation.get(String(connectorId)) || null : null;
}

function findConnectorLivePlan(connectorId) {
  return connectorId ? state.cache.connectorLivePlan.get(String(connectorId)) || null : null;
}

function findWorkerPlan(connectorId) {
  return (state.cache.workerPlan?.specs || []).find((spec) => spec.connector_id === connectorId) || null;
}

function findWorkerRuntime(connectorId) {
  return (state.cache.workerStatus?.workers || []).find((worker) => worker.connector_id === connectorId) || null;
}

function joinIssueMessages(issues) {
  return (issues || []).map((issue) => issue.message).join("; ");
}

function joinIssueCodes(issues) {
  return (issues || []).map((issue) => issue.code).join(", ");
}

function normalizeDateTimeInput(value) {
  const cleaned = cleanString(value);
  if (!cleaned) {
    return "";
  }
  const date = new Date(cleaned);
  if (Number.isNaN(date.getTime())) {
    throw new Error("Date filters must be valid date/time values.");
  }
  return date.toISOString();
}

function normalizeTimeseriesLimit(value) {
  const numeric = Number.parseInt(String(value || DEFAULT_TIMESERIES_LIMIT), 10);
  if (!Number.isFinite(numeric) || numeric < 1) {
    throw new Error("Limit must be a positive integer.");
  }
  return Math.min(numeric, MAX_TIMESERIES_LIMIT);
}

function formatObservationValue(value) {
  if (!value || typeof value !== "object") {
    return "n/a";
  }

  if (value.type === "number") {
    return formatMaybeNumber(value.value);
  }
  if (value.type === "string") {
    return String(value.value ?? "");
  }
  if (value.type === "boolean") {
    return String(Boolean(value.value));
  }
  if (value.type === "json") {
    return JSON.stringify(value.value);
  }
  return JSON.stringify(value);
}

function observationNumber(value) {
  if (value && value.type === "number" && typeof value.value === "number" && Number.isFinite(value.value)) {
    return value.value;
  }
  return null;
}

function formatMaybeNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 6 });
  }
  return String(value ?? "n/a");
}

function formatTimeseriesRange(from, to) {
  if (from && to) {
    return `${formatDateTime(from)} -> ${formatDateTime(to)}`;
  }
  if (from) {
    return `from ${formatDateTime(from)}`;
  }
  if (to) {
    return `to ${formatDateTime(to)}`;
  }
  return "full range";
}

function buildTimeseriesChartSvg(points) {
  const width = 960;
  const height = 280;
  const margin = { top: 24, right: 24, bottom: 40, left: 56 };
  const values = points.map((point) => observationNumber(point.value));
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const valueSpan = maxValue - minValue || 1;
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const xStep = points.length > 1 ? innerWidth / (points.length - 1) : innerWidth / 2;

  const coordinates = points.map((point, index) => {
    const numericValue = observationNumber(point.value);
    const x = margin.left + (points.length > 1 ? xStep * index : innerWidth / 2);
    const y = margin.top + innerHeight - (((numericValue - minValue) / valueSpan) * innerHeight);
    return { x, y, point, numericValue };
  });

  const polyline = coordinates.map((coordinate) => `${coordinate.x},${coordinate.y}`).join(" ");
  const yTicks = [0, 0.5, 1].map((ratio) => ({
    value: maxValue - (valueSpan * ratio),
    y: margin.top + (innerHeight * ratio),
  }));
  const firstTime = formatDateTime(points[0]?.time);
  const lastTime = formatDateTime(points[points.length - 1]?.time);

  return `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Numeric time-series line chart">
      ${yTicks.map((tick) => `
        <line class="chart-grid-line" x1="${margin.left}" y1="${tick.y}" x2="${width - margin.right}" y2="${tick.y}"></line>
        <text class="chart-label" x="${margin.left - 10}" y="${tick.y + 4}" text-anchor="end">${escapeHtml(formatMaybeNumber(tick.value))}</text>
      `).join("")}
      <line class="chart-axis" x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${height - margin.bottom}"></line>
      <line class="chart-axis" x1="${margin.left}" y1="${height - margin.bottom}" x2="${width - margin.right}" y2="${height - margin.bottom}"></line>
      <polyline class="chart-line" points="${polyline}"></polyline>
      ${coordinates.map((coordinate) => `
        <circle class="chart-point" cx="${coordinate.x}" cy="${coordinate.y}" r="3.5">
          <title>${escapeHtml(`${formatDateTime(coordinate.point.time)} | ${formatMaybeNumber(coordinate.numericValue)} ${coordinate.point.unit || ""}`.trim())}</title>
        </circle>
      `).join("")}
      <text class="chart-label" x="${margin.left}" y="${height - 12}" text-anchor="start">${escapeHtml(firstTime)}</text>
      <text class="chart-label" x="${width - margin.right}" y="${height - 12}" text-anchor="end">${escapeHtml(lastTime)}</text>
    </svg>
  `;
}

function formatDateTime(value) {
  if (!value) {
    return "n/a";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return date.toLocaleString();
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString();
}

function normalizeApiBaseUrl(value) {
  return String(value || DEFAULT_API_BASE_URL).trim().replace(/\/+$/, "");
}

function badge(label, tone) {
  return `<span class="badge ${tone || ""}">${escapeHtml(String(label))}</span>`;
}

function connectorBadgeTone(connector) {
  if (!connector.enabled) {
    return "warning";
  }
  if (connector.degraded || connector.last_error) {
    return "danger";
  }
  if (connector.running) {
    return "success";
  }
  if (connector.reconnecting) {
    return "warning";
  }
  return "info";
}

function workerPlanTone(status) {
  if (status === "planned") {
    return "info";
  }
  if (status === "skipped") {
    return "warning";
  }
  if (status === "invalid" || status === "unsupported") {
    return "danger";
  }
  return "info";
}

function workerRuntimeTone(status) {
  if (status === "running" || status === "ready" || status === "success") {
    return "success";
  }
  if (status === "planned" || status === "starting") {
    return "info";
  }
  if (status === "disabled" || status === "skipped" || status === "reconnecting" || status === "degraded") {
    return "warning";
  }
  return "danger";
}

function validationBadgeTone(status) {
  if (status === "valid") {
    return "success";
  }
  if (status === "warning") {
    return "warning";
  }
  return "danger";
}

function withGeneratedAt(message, generatedAt) {
  return generatedAt ? `${message} Generated at ${formatDateTime(generatedAt)}.` : message;
}

function setStatus(message) {
  document.getElementById("global-status").textContent = message;
}

function clearError() {
  const banner = document.getElementById("error-banner");
  banner.textContent = "";
  banner.classList.add("hidden");
}

function handleError(error) {
  const banner = document.getElementById("error-banner");
  banner.textContent = error.message || "Unexpected error.";
  banner.classList.remove("hidden");
  setStatus("Last request failed.");
}

function buildEmptyRow(columnCount, message) {
  return `<tr><td colspan="${columnCount}">${escapeHtml(message)}</td></tr>`;
}

function clearCaches() {
  state.cache.overview = null;
  state.cache.timeseries = null;
  state.cache.timeseriesQuery = null;
  state.cache.timeseriesProperties.clear();
  state.cache.connectorsOverview = null;
  state.cache.connectorsList = null;
  state.cache.workerPlan = null;
  state.cache.workerStatus = null;
  state.cache.connectorDetail.clear();
  state.cache.connectorStatus.clear();
  state.cache.connectorValidation.clear();
  state.cache.connectorLivePlan.clear();
  state.cache.flows = null;
  state.cache.flowDetail.clear();
  state.cache.flowConnectors = null;
  state.cache.flowConnectorOverview = null;
  state.cache.flowProposedValidation = null;
  state.cache.flowProposedDryRun = null;
  state.cache.flowStoredValidation.clear();
  state.cache.flowStoredDryRun.clear();
}

function cleanString(value) {
  return String(value || "").trim();
}

function parseJson(value, label) {
  try {
    return JSON.parse(value);
  } catch (_error) {
    throw new Error(`${label} must be valid JSON.`);
  }
}

function labelFromFieldName(name) {
  return name
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function isUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function safeBrokerUrl(value) {
  if (!value) {
    return value;
  }
  const trimmed = String(value).trim();
  const match = trimmed.match(/^([a-z]+:\/\/)([^@/]+@)(.+)$/i);
  if (match) {
    return `${match[1]}***REDACTED***@${match[3]}`;
  }
  return trimmed;
}

function redactSecrets(value, parentKey = "") {
  if (Array.isArray(value)) {
    return value.map((item) => redactSecrets(item, parentKey));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, childValue]) => {
      const normalized = key.toLowerCase();
      if (normalized === "broker_url" && typeof childValue === "string") {
        return [key, safeBrokerUrl(childValue)];
      }
      if (SECRET_LIKE_KEYS.has(normalized) || normalized.endsWith("_token") || normalized.endsWith("_secret")) {
        return [key, "***REDACTED***"];
      }
      return [key, redactSecrets(childValue, key)];
    }));
  }

  if (typeof value === "string" && parentKey.toLowerCase() === "broker_url") {
    return safeBrokerUrl(value);
  }

  return value;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
