const DEFAULT_API_BASE_URL = "http://127.0.0.1:8080";
const STORAGE_KEYS = {
  apiBaseUrl: "aion.dashboard.apiBaseUrl",
  bearerToken: "aion.dashboard.bearerToken",
};

const state = {
  activeSection: "overview",
  apiBaseUrl: DEFAULT_API_BASE_URL,
  bearerToken: "",
  selectedFlowId: null,
  cache: {
    overview: null,
    timeseries: null,
    connectors: null,
    flows: null,
    flowDetail: new Map(),
  },
};

const sectionTitles = {
  overview: "Overview",
  timeseries: "Time-Series Entities",
  connectors: "Connectors",
  flows: "Flows",
};

document.addEventListener("DOMContentLoaded", () => {
  hydrateConfig();
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

    renderConfig();
    clearError();
    setStatus("API configuration saved.");
    refreshCurrentSection();
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
      await loadConnectors(force);
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
  renderTimeseries(data);
  setStatus(withGeneratedAt("Time-series entities loaded.", data.generated_at));
}

async function loadConnectors(force) {
  setStatus("Loading connectors overview...");
  const data = force || !state.cache.connectors
    ? await apiGet("/dashboard/connectors/overview")
    : state.cache.connectors;
  state.cache.connectors = data;
  renderConnectors(data);
  setStatus(withGeneratedAt("Connectors overview loaded.", data.generated_at));
}

async function loadFlows(force) {
  setStatus("Loading flow inventory...");
  const data = force || !state.cache.flows
    ? await apiGet("/dashboard/flows")
    : state.cache.flows;
  state.cache.flows = data;
  renderFlows(data);
  setStatus(withGeneratedAt("Flow inventory loaded.", data.generated_at));

  if (state.selectedFlowId) {
    await loadFlowDetail(state.selectedFlowId, force);
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
    setStatus(withGeneratedAt("Flow detail loaded.", data.generated_at));
  } catch (error) {
    handleError(error);
  }
}

async function apiGet(path) {
  const headers = {
    Accept: "application/json",
  };

  if (state.bearerToken) {
    headers.Authorization = `Bearer ${state.bearerToken}`;
  }

  const response = await fetch(`${state.apiBaseUrl}${path}`, { headers });
  if (!response.ok) {
    let detail = "";
    try {
      const body = await response.json();
      detail = body.error || body.message || JSON.stringify(body);
    } catch (_error) {
      detail = await response.text();
    }

    throw new Error(`Request failed for ${path}: ${response.status} ${response.statusText}${detail ? ` - ${detail}` : ""}`);
  }

  return response.json();
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
  const rows = (data.entities || []).map((entity) => `
    <tr>
      <td>
        <strong>${escapeHtml(entity.entity_key)}</strong><br>
        <span class="status-meta">${escapeHtml(entity.entity_id)}</span>
      </td>
      <td>${escapeHtml(entity.entity_type || "n/a")}</td>
      <td>${escapeHtml(entity.display_name || "n/a")}</td>
      <td>${formatNumber(entity.observed_property_count)}</td>
      <td>${formatNumber(entity.observation_count)}</td>
      <td>${escapeHtml(formatDateTime(entity.last_observed_at))}</td>
    </tr>
  `);

  document.getElementById("timeseries-table-body").innerHTML = rows.join("") || buildEmptyRow(6, "No time-series entities returned.");
}

function renderConnectors(data) {
  const rows = (data.connectors || []).map((connector) => `
    <tr>
      <td>
        <strong>${escapeHtml(connector.connector_key)}</strong><br>
        <span class="status-meta">${escapeHtml(connector.connector_id)}</span>
      </td>
      <td>${escapeHtml(connector.connector_type || "n/a")}</td>
      <td>${escapeHtml(connector.connector_profile || "n/a")}</td>
      <td>${badge(connector.enabled ? "enabled" : "disabled", connector.enabled ? "success" : "warning")}</td>
      <td>${badge(connector.readiness || connector.status || "unknown", connectorBadgeTone(connector))}</td>
      <td>${escapeHtml(connector.broker_url || "n/a")}</td>
      <td>${escapeHtml(connector.topic_filter || "n/a")}</td>
      <td>${escapeHtml(connector.payload_format || "n/a")}</td>
      <td>${escapeHtml(`${connector.worker_kind || "n/a"} / ${connector.worker_status || "n/a"}`)}</td>
      <td>${escapeHtml(connector.last_error || "n/a")}</td>
      <td>${badge(connector.secret_configured ? "configured" : "not configured", connector.secret_configured ? "success" : "warning")}</td>
    </tr>
  `);

  document.getElementById("connectors-table-body").innerHTML = rows.join("") || buildEmptyRow(11, "No connectors returned.");
}

function renderFlows(data) {
  const rows = (data.flows || []).map((flow) => `
    <tr data-flow-id="${escapeHtml(flow.flow_id)}">
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
  document.getElementById("flow-detail-empty").classList.add("hidden");
  document.getElementById("flow-detail-content").classList.remove("hidden");
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
      <td><pre class="mono-block">${escapeHtml(JSON.stringify(node.config, null, 2))}</pre></td>
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
}

function renderTokenList(elementId, values, formatter) {
  const items = (values || []).map((value) => `<li>${escapeHtml(formatter(value))}</li>`);
  document.getElementById(elementId).innerHTML = items.join("") || "<li>None</li>";
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
  return "warning";
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

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
