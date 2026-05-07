import {
  DEFAULT_FLOW_SAMPLE_PAYLOAD,
  FLOW_SINK_KINDS_WITH_CONNECTOR,
  FLOW_SOURCE_KINDS_WITH_CONNECTOR,
} from "./constants.js";
import { apiDelete, apiGet, apiPost, apiPut } from "./api.js";
import { state } from "./state.js";
import {
  badge,
  buildEmptyRow,
  cleanString,
  escapeHtml,
  formatDateTime,
  formatNumber,
  handleError,
  parseJson,
  readOptionalJsonText,
  readOptionalJsonTextarea,
  redactSecrets,
  renderDetailItem,
  requireNonEmpty,
  setStatus,
  validationBadgeTone,
  withGeneratedAt,
} from "./utils.js";

export function initializeFlowBuilder() {
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

export function bindFlowEvents() {
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

export async function loadFlows(force) {
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
        node_type: "source",
        name: cleanString(form.elements.source_name.value) || undefined,
        config: sourceConfig,
        position: { x: 60, y: 120 },
      },
      {
        node_id: transformNodeId,
        node_type: transformKind === "filter_condition" ? "filter" : "decoder",
        name: cleanString(form.elements.transform_name.value) || undefined,
        config: transformConfig,
        position: { x: 300, y: 120 },
      },
      {
        node_id: sinkNodeId,
        node_type: sinkKind === "dlq" ? "dlq" : "sink",
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
