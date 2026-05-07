import { CONNECTOR_PROFILE_OPTIONS, CONNECTOR_TYPE_OPTIONS } from "./constants.js";
import { apiGet, apiPatch, apiPost, apiPut } from "./api.js";
import { state } from "./state.js";
import {
  badge,
  buildEmptyRow,
  cleanString,
  connectorBadgeTone,
  escapeHtml,
  formatDateTime,
  formatNumber,
  handleError,
  isUuid,
  labelFromFieldName,
  parseJson,
  redactSecrets,
  renderKeyValueBlock,
  safeBrokerUrl,
  setStatus,
  withGeneratedAt,
  workerPlanTone,
  workerRuntimeTone,
} from "./utils.js";

export function populateConnectorOptionInputs() {
  renderSelectOptions("create-connector-type", CONNECTOR_TYPE_OPTIONS, "mqtt");
  renderSelectOptions("create-connector-profile", CONNECTOR_PROFILE_OPTIONS, "generic-mqtt");
}

export function bindConnectorEvents() {
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
}

export async function loadConnectorsSection(force) {
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

function renderSelectOptions(elementId, options, selectedValue) {
  const element = document.getElementById(elementId);
  element.innerHTML = options.map((option) => `
    <option value="${escapeHtml(option.value)}"${option.value === selectedValue ? " selected" : ""}>${escapeHtml(option.label)}</option>
  `).join("");
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
