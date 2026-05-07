import { DEFAULT_API_BASE_URL, STORAGE_KEYS } from "./js/constants.js";
import { apiGet } from "./js/api.js";
import { bindConnectorEvents, loadConnectorsSection, populateConnectorOptionInputs } from "./js/connectors.js";
import { bindFlowEvents, initializeFlowBuilder, loadFlows } from "./js/flows.js";
import { bindTimeseriesEvents, loadTimeseries } from "./js/timeseries.js";
import { clearCaches, sectionModes, sectionTitles, state } from "./js/state.js";
import {
  clearError,
  escapeHtml,
  formatNumber,
  handleError,
  normalizeApiBaseUrl,
  setStatus,
  withGeneratedAt,
} from "./js/utils.js";

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

  bindTimeseriesEvents();
  bindConnectorEvents();
  bindFlowEvents();
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
