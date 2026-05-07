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

const GRAPH_IDS = {
  proposed: {
    graph: "flow-proposed-graph",
    graphStatus: "flow-proposed-graph-status",
    nodeDetail: "flow-proposed-node-detail",
    issues: "flow-proposed-issues",
    effects: "flow-proposed-effects",
  },
  stored: {
    graph: "flow-stored-graph",
    graphStatus: "flow-stored-graph-status",
    nodeDetail: "flow-stored-node-detail",
    issues: "flow-stored-issues",
    effects: "flow-stored-effects",
  },
};

const GRAPH_SELECTION = {
  proposed: null,
  stored: null,
};

const DRY_RUN_EFFECTS = [
  ["would_store_observation", "Store Observation"],
  ["would_publish_mqtt", "Publish MQTT"],
  ["would_forward_http", "Forward HTTP"],
  ["would_create_event", "Create Event"],
  ["would_create_command", "Create Command"],
  ["would_use_dlq", "Use DLQ"],
];

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
  syncFlowDraftFromForm({ resetAdvancedOverride: false, invalidateChecks: true });
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
    syncFlowDraftFromForm({ resetAdvancedOverride: false, invalidateChecks: true });
  });

  document.getElementById("flow-sink-kind").addEventListener("change", () => {
    syncFlowConnectorFieldState();
    syncFlowDraftFromForm({ resetAdvancedOverride: false, invalidateChecks: true });
  });

  document.getElementById("flow-builder-form").addEventListener("input", () => {
    syncFlowDraftFromForm({ resetAdvancedOverride: false, invalidateChecks: true });
  });

  document.getElementById("flow-builder-preview-button").addEventListener("click", () => {
    syncFlowDraftFromForm({ resetAdvancedOverride: false, invalidateChecks: true });
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
      renderProposedGraphState();
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
      renderProposedGraphState();
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
      invalidateProposedFlowChecks();
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
    GRAPH_SELECTION.stored = null;
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
    GRAPH_SELECTION.stored = null;
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
  GRAPH_SELECTION.proposed = null;
  initializeFlowBuilder();
  invalidateProposedFlowChecks();
  setStatus("Flow builder reset.");
}

function syncFlowDraftFromForm(options = {}) {
  const { resetAdvancedOverride = false, invalidateChecks = false } = options;
  if (resetAdvancedOverride) {
    document.getElementById("flow-advanced-json").value = "";
  }
  state.flowDraft = buildFlowDraftFromForm();
  if (invalidateChecks) {
    invalidateProposedFlowChecks({ preserveGraph: true });
  }
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
    syncFlowDraftFromForm({ resetAdvancedOverride: false, invalidateChecks: false });
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
    renderProposedGraphState(draft);
  } catch (error) {
    preview.textContent = error.message;
    builderStatus.textContent = "Fix the builder fields or advanced JSON before validating, dry-running, or creating the flow.";
    renderGraphEmpty("proposed", error.message, "Preview is unavailable until the draft JSON parses and matches the flow shape.");
    renderGraphIssues("proposed", null, "Fix the draft JSON before validation issues can be shown.");
    renderGraphEffects("proposed", null);
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
  renderStoredGraphState();
}

function renderFlowValidationResponse(response) {
  const issues = getValidationIssues(response);
  const errors = issues.filter((issue) => issue.severity === "error");
  const warnings = issues.filter((issue) => issue.severity === "warning");
  return `
    <div class="result-stack">
      <div class="detail-grid">
        ${renderDetailItem("Valid", String(Boolean(response.valid)))}
        ${renderDetailItem("Errors", formatNumber(errors.length))}
        ${renderDetailItem("Warnings", formatNumber(warnings.length))}
        ${renderDetailItem("Connectors", formatNumber((response.referenced_connectors || []).length))}
        ${renderDetailItem("Planned Sinks", formatNumber((response.planned_sinks || []).length))}
      </div>
      <div>${renderIssueList(issues, "No validation issues returned.")}</div>
    </div>
  `;
}

function renderFlowDryRunResponse(response) {
  return `
    <div class="result-stack">
      <div class="detail-grid">
        ${renderDetailItem("Valid", String(Boolean(response.valid)))}
        ${renderDetailItem("Execution Supported", String(Boolean(response.execution_supported)))}
        ${renderDetailItem("Side Effects Performed", String(Boolean(response.side_effects_performed)))}
        ${renderDetailItem("Sample Payload", String(Boolean(response.sample_payload_provided)))}
        ${renderDetailItem("Would Store Observation", String(Boolean(response.would_store_observation)))}
        ${renderDetailItem("Would Publish MQTT", String(Boolean(response.would_publish_mqtt)))}
        ${renderDetailItem("Would Forward HTTP", String(Boolean(response.would_forward_http)))}
        ${renderDetailItem("Would Create Event", String(Boolean(response.would_create_event)))}
        ${renderDetailItem("Would Create Command", String(Boolean(response.would_create_command)))}
        ${renderDetailItem("Would Use DLQ", String(Boolean(response.would_use_dlq)))}
      </div>
      <p class="preview-note"><strong>Planned Path:</strong> ${escapeHtml((response.planned_path || []).join(" -> ") || "None")}</p>
      <p class="preview-note"><strong>Planned Sinks:</strong> ${escapeHtml((response.planned_sinks || []).map(formatPlannedSink).join(" | ") || "None")}</p>
      <p class="preview-note"><strong>Referenced Connectors:</strong> ${escapeHtml((response.referenced_connectors || []).map(formatReferencedConnector).join(" | ") || "None")}</p>
      <div>${renderIssueList(getValidationIssues(response), "No validation issues returned.")}</div>
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
    renderGraphEmpty("stored", "No stored flow selected.", "Select a flow to render its read-only graph.");
    renderGraphIssues("stored", null, "Validation issues from the dashboard summary appear here. Load stored validation for the full structured result.");
    renderGraphEffects("stored", null);
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
    <p>Valid: <strong>${escapeHtml(String(Boolean(data.validation_summary.valid)))}</strong></p>
    <p>Errors: <strong>${formatNumber(data.validation_summary.error_count)}</strong></p>
    <p>Warnings: <strong>${formatNumber(data.validation_summary.warning_count)}</strong></p>
    <p>Issues: <strong>${formatNumber(getValidationIssues(data.validation_summary).length)}</strong></p>
  `;

  document.getElementById("flow-execution-summary").innerHTML = `
    <p>Execution supported: <strong>${escapeHtml(String(Boolean(data.execution_supported)))}</strong></p>
    <p>Status: <strong>${escapeHtml(data.execution_status || "n/a")}</strong></p>
    <p>Side effects performed: <strong>${escapeHtml(String(Boolean(data.side_effects_performed)))}</strong></p>
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
      <td><pre class="mono-block">${escapeHtml(JSON.stringify(redactSecrets(node.config || {}), null, 2))}</pre></td>
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

function invalidateProposedFlowChecks(options = {}) {
  const { preserveGraph = false } = options;
  state.cache.flowProposedValidation = null;
  state.cache.flowProposedDryRun = null;
  renderProposedFlowValidation(null);
  renderProposedFlowDryRun(null);
  renderGraphIssues("proposed", null, "Validate the proposed flow to inspect structured issues before saving.");
  renderGraphEffects("proposed", null);
  if (!preserveGraph) {
    renderProposedGraphState();
  }
}

function renderProposedGraphState(overrideFlow) {
  try {
    const flow = overrideFlow || getCurrentDraftForGraph();
    renderFlowGraph("proposed", flow, state.cache.flowProposedValidation, state.cache.flowProposedDryRun, "Preview-only graph layer from the current draft.");
  } catch (error) {
    renderGraphEmpty("proposed", error.message, "Preview is unavailable until the draft JSON parses and matches the flow shape.");
  }
}

function renderStoredGraphState() {
  const detail = state.selectedFlowId ? state.cache.flowDetail.get(String(state.selectedFlowId)) : null;
  if (!detail) {
    renderGraphEmpty("stored", "No stored flow selected.", "Select a flow to render its read-only graph.");
    return;
  }

  const validation = state.cache.flowStoredValidation.get(String(state.selectedFlowId))
    || detail.validation_summary
    || null;
  const dryRun = state.cache.flowStoredDryRun.get(String(state.selectedFlowId)) || null;
  renderFlowGraph("stored", detail, validation, dryRun, "Stored graph preview from GET /dashboard/flows/{flow_id}.");
}

function getCurrentDraftForGraph() {
  const advanced = cleanString(document.getElementById("flow-advanced-json").value);
  const draft = advanced ? parseJson(advanced, "Advanced JSON override") : (state.flowDraft || buildFlowDraftFromForm());
  validateFlowDraftShape(draft);
  return draft;
}

function renderFlowGraph(context, flowLike, validation, dryRun, statusPrefix) {
  const ids = GRAPH_IDS[context];
  const graphContainer = document.getElementById(ids.graph);
  const nodes = normalizeNodes(flowLike?.nodes);
  const edges = normalizeEdges(flowLike?.edges);

  if (nodes.length === 0) {
    renderGraphEmpty(context, "No graph nodes available.", statusPrefix);
    return;
  }

  const issues = getValidationIssues(validation);
  const issueMaps = buildIssueMaps(issues);
  const effectNodeIds = buildEffectNodeIds(nodes, dryRun);
  const layout = computeGraphLayout(nodes, edges);
  const selectedNodeId = resolveSelectedNodeId(context, nodes);
  const edgeLookup = new Map(edges.map((edge) => [edgeKey(edge), edge]));

  graphContainer.innerHTML = buildGraphSvg(context, nodes, edges, layout, issueMaps, selectedNodeId, effectNodeIds);
  bindGraphNodeEvents(context, nodes, validation, dryRun, issueMaps);

  document.getElementById(ids.graphStatus).innerHTML = renderGraphStatus(statusPrefix, nodes, edges, validation);
  renderNodeDetail(context, nodes.find((node) => node.node_id === GRAPH_SELECTION[context]), issueMaps, dryRun);
  renderGraphIssues(context, validation, context === "stored"
    ? "Validation issues from the dashboard summary appear here. Load stored validation for the full structured result."
    : "Validate the proposed flow to inspect structured issues before saving.");
  renderGraphEffects(context, dryRun);

  Array.from(graphContainer.querySelectorAll("[data-graph-edge-key]")).forEach((element) => {
    const edge = edgeLookup.get(element.getAttribute("data-graph-edge-key"));
    if (edge && issueMaps.edgeIssues.has(edgeKey(edge))) {
      element.classList.add("has-issues");
    }
  });
}

function renderGraphEmpty(context, statusMessage, bodyMessage) {
  const ids = GRAPH_IDS[context];
  document.getElementById(ids.graph).innerHTML = `
    <div class="flow-graph-empty">${escapeHtml(bodyMessage)}</div>
  `;
  document.getElementById(ids.graphStatus).textContent = statusMessage;
  document.getElementById(ids.nodeDetail).innerHTML = '<div class="graph-node-detail">Select a node in the graph to inspect its redacted config.</div>';
}

function renderGraphStatus(prefix, nodes, edges, validation) {
  const issues = getValidationIssues(validation);
  const errors = issues.filter((issue) => issue.severity === "error").length;
  const warnings = issues.filter((issue) => issue.severity === "warning").length;
  const status = validation?.status || (validation?.valid === true ? "valid" : (validation ? "invalid" : "preview-only"));
  return [
    escapeHtml(prefix),
    `${badge(status, validation ? validationBadgeTone(status === "preview-only" ? "warning" : status) : "info")}`,
    `Nodes: <strong>${formatNumber(nodes.length)}</strong>`,
    `Edges: <strong>${formatNumber(edges.length)}</strong>`,
    `Errors: <strong>${formatNumber(errors)}</strong>`,
    `Warnings: <strong>${formatNumber(warnings)}</strong>`,
  ].join(" ");
}

function renderNodeDetail(context, node, issueMaps, dryRun) {
  const ids = GRAPH_IDS[context];
  const container = document.getElementById(ids.nodeDetail);
  if (!node) {
    container.innerHTML = '<div class="graph-node-detail">Select a node in the graph to inspect its redacted config.</div>';
    return;
  }

  const nodeIssues = issueMaps.nodeIssues.get(node.node_id) || [];
  const redactedConfig = JSON.stringify(redactSecrets(node.config || {}), null, 2) || "{}";
  const effectActive = buildEffectNodeIds([node], dryRun).has(node.node_id);
  container.innerHTML = `
    <div class="result-stack">
      <div class="detail-grid">
        ${renderDetailItem("Node ID", node.node_id)}
        ${renderDetailItem("Node Type", node.node_type || "n/a")}
        ${renderDetailItem("Name", node.name || "n/a")}
        ${renderDetailItem("Kind", node.config?.kind || "n/a")}
        ${renderDetailItem("Highlighted By Dry-Run", effectActive ? "true" : "false")}
      </div>
      <div>
        <p class="preview-note"><strong>Redacted Config JSON</strong></p>
        <pre class="mono-block">${escapeHtml(redactedConfig)}</pre>
      </div>
      <div>
        <p class="preview-note"><strong>Node Issues</strong></p>
        ${renderIssueList(nodeIssues, "No node-specific validation issues.")}
      </div>
    </div>
  `;
}

function renderGraphIssues(context, validation, emptyMessage) {
  const ids = GRAPH_IDS[context];
  const issues = getValidationIssues(validation);
  document.getElementById(ids.issues).innerHTML = renderIssueList(issues, emptyMessage);
}

function renderGraphEffects(context, dryRun) {
  const ids = GRAPH_IDS[context];
  const container = document.getElementById(ids.effects);
  if (!dryRun) {
    container.textContent = "Dry-run stays planning-only. No side effects are performed.";
    return;
  }
  container.innerHTML = `
    <div class="flow-effect-grid">
      ${DRY_RUN_EFFECTS.map(([field, label]) => `
        <div class="flow-effect-card ${dryRun[field] ? "active" : "inactive"}">
          <strong>${escapeHtml(label)}</strong>
          <span>${escapeHtml(String(Boolean(dryRun[field])))}</span>
        </div>
      `).join("")}
    </div>
  `;
}

function normalizeNodes(nodes) {
  return Array.isArray(nodes) ? nodes.filter(Boolean) : [];
}

function normalizeEdges(edges) {
  return Array.isArray(edges) ? edges.filter(Boolean) : [];
}

function getValidationIssues(validation) {
  if (!validation || typeof validation !== "object") {
    return [];
  }
  if (Array.isArray(validation.validation_issues)) {
    return validation.validation_issues;
  }
  if (Array.isArray(validation.issues)) {
    return validation.issues;
  }
  return [];
}

function buildIssueMaps(issues) {
  const nodeIssues = new Map();
  const edgeIssues = new Map();

  for (const issue of issues || []) {
    if (issue?.node_id) {
      if (!nodeIssues.has(issue.node_id)) {
        nodeIssues.set(issue.node_id, []);
      }
      nodeIssues.get(issue.node_id).push(issue);
    }
    if (issue?.edge_id) {
      if (!edgeIssues.has(issue.edge_id)) {
        edgeIssues.set(issue.edge_id, []);
      }
      edgeIssues.get(issue.edge_id).push(issue);
    }
  }

  return { nodeIssues, edgeIssues };
}

function resolveSelectedNodeId(context, nodes) {
  const existing = GRAPH_SELECTION[context];
  if (existing && nodes.some((node) => node.node_id === existing)) {
    return existing;
  }
  GRAPH_SELECTION[context] = nodes[0]?.node_id || null;
  return GRAPH_SELECTION[context];
}

function computeGraphLayout(nodes, edges) {
  if (nodes.every(hasNumericPosition)) {
    return normalizeExplicitPositions(nodes);
  }
  if (isLinearGraph(nodes, edges)) {
    return buildLinearLayout(nodes, edges);
  }
  return buildGridLayout(nodes);
}

function hasNumericPosition(node) {
  return node?.position
    && Number.isFinite(Number(node.position.x))
    && Number.isFinite(Number(node.position.y));
}

function normalizeExplicitPositions(nodes) {
  const width = 960;
  const padding = 48;
  const nodeWidth = 210;
  const nodeHeight = 86;
  const xs = nodes.map((node) => Number(node.position.x));
  const ys = nodes.map((node) => Number(node.position.y));
  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);
  const xSpan = Math.max(1, maxX - minX);
  const ySpan = Math.max(1, maxY - minY);
  const availableWidth = width - (padding * 2) - nodeWidth;
  const scaledNodes = nodes.map((node) => ({
    node_id: node.node_id,
    x: padding + (((Number(node.position.x) - minX) / xSpan) * availableWidth),
    y: padding + (((Number(node.position.y) - minY) / ySpan) * Math.max(140, nodes.length > 3 ? 280 : 140)),
  }));
  const height = Math.max(...scaledNodes.map((item) => item.y)) + nodeHeight + padding;
  return { width, height, nodeWidth, nodeHeight, nodes: scaledNodes };
}

function isLinearGraph(nodes, edges) {
  if (nodes.length < 2 || edges.length !== nodes.length - 1) {
    return false;
  }
  const incoming = new Map(nodes.map((node) => [node.node_id, 0]));
  const outgoing = new Map(nodes.map((node) => [node.node_id, 0]));
  for (const edge of edges) {
    if (!incoming.has(edge.target_node_id) || !outgoing.has(edge.source_node_id)) {
      return false;
    }
    incoming.set(edge.target_node_id, incoming.get(edge.target_node_id) + 1);
    outgoing.set(edge.source_node_id, outgoing.get(edge.source_node_id) + 1);
  }
  const roots = nodes.filter((node) => incoming.get(node.node_id) === 0);
  const terminals = nodes.filter((node) => outgoing.get(node.node_id) === 0);
  return roots.length === 1
    && terminals.length === 1
    && nodes.every((node) => incoming.get(node.node_id) <= 1 && outgoing.get(node.node_id) <= 1);
}

function buildLinearLayout(nodes, edges) {
  const ordered = topologicalOrder(nodes, edges);
  const width = 960;
  const padding = 42;
  const nodeWidth = 220;
  const nodeHeight = 88;
  const gap = ordered.length > 1
    ? Math.max(28, Math.min(88, (width - (padding * 2) - (ordered.length * nodeWidth)) / (ordered.length - 1)))
    : 0;
  const y = 120;
  return {
    width,
    height: 300,
    nodeWidth,
    nodeHeight,
    nodes: ordered.map((node, index) => ({
      node_id: node.node_id,
      x: padding + (index * (nodeWidth + gap)),
      y,
    })),
  };
}

function buildGridLayout(nodes) {
  const width = 960;
  const padding = 42;
  const nodeWidth = 220;
  const nodeHeight = 88;
  const columns = Math.min(3, Math.max(2, Math.ceil(Math.sqrt(nodes.length))));
  const rowGap = 48;
  const colGap = Math.max(28, Math.min(72, (width - (padding * 2) - (columns * nodeWidth)) / Math.max(1, columns - 1)));
  const positioned = nodes.map((node, index) => {
    const row = Math.floor(index / columns);
    const column = index % columns;
    return {
      node_id: node.node_id,
      x: padding + (column * (nodeWidth + colGap)),
      y: 42 + (row * (nodeHeight + rowGap)),
    };
  });
  const height = Math.max(...positioned.map((item) => item.y)) + nodeHeight + padding;
  return { width, height, nodeWidth, nodeHeight, nodes: positioned };
}

function topologicalOrder(nodes, edges) {
  const nodesById = new Map(nodes.map((node) => [node.node_id, node]));
  const outgoing = new Map(nodes.map((node) => [node.node_id, []]));
  const indegree = new Map(nodes.map((node) => [node.node_id, 0]));
  for (const edge of edges) {
    if (!outgoing.has(edge.source_node_id) || !indegree.has(edge.target_node_id)) {
      continue;
    }
    outgoing.get(edge.source_node_id).push(edge.target_node_id);
    indegree.set(edge.target_node_id, indegree.get(edge.target_node_id) + 1);
  }
  const queue = nodes.filter((node) => indegree.get(node.node_id) === 0);
  const ordered = [];
  while (queue.length > 0) {
    const node = queue.shift();
    ordered.push(node);
    for (const target of outgoing.get(node.node_id) || []) {
      indegree.set(target, indegree.get(target) - 1);
      if (indegree.get(target) === 0) {
        queue.push(nodesById.get(target));
      }
    }
  }
  return ordered.length === nodes.length ? ordered : nodes;
}

function buildGraphSvg(context, nodes, edges, layout, issueMaps, selectedNodeId, effectNodeIds) {
  const positionById = new Map((layout.nodes || []).map((item) => [item.node_id || nodes.find((node) => node.node_id === item.node_id)?.node_id || item.node_id, item]));
  const markerId = `flow-graph-arrow-${context}`;
  return `
    <svg viewBox="0 0 ${layout.width} ${layout.height}" role="img" aria-label="${escapeHtml(`${context} flow graph`)}}">
      <defs>
        <marker id="${markerId}" markerWidth="10" markerHeight="10" refX="9" refY="5" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L10,5 L0,10 z" fill="rgba(95, 103, 95, 0.7)"></path>
        </marker>
      </defs>
      ${edges.map((edge) => renderGraphEdge(edge, positionById, layout, markerId, issueMaps)).join("")}
      ${nodes.map((node) => renderGraphNode(node, positionById.get(node.node_id), layout, issueMaps, selectedNodeId, effectNodeIds)).join("")}
    </svg>
  `;
}

function renderGraphEdge(edge, positionById, layout, markerId, issueMaps) {
  const source = positionById.get(edge.source_node_id);
  const target = positionById.get(edge.target_node_id);
  if (!source || !target) {
    return "";
  }
  const startX = source.x + layout.nodeWidth;
  const startY = source.y + (layout.nodeHeight / 2);
  const endX = target.x;
  const endY = target.y + (layout.nodeHeight / 2);
  const controlOffset = Math.max(36, Math.abs(endX - startX) / 2);
  const path = `M ${startX} ${startY} C ${startX + controlOffset} ${startY}, ${endX - controlOffset} ${endY}, ${endX} ${endY}`;
  const midX = (startX + endX) / 2;
  const midY = (startY + endY) / 2;
  const issues = issueMaps.edgeIssues.get(edge.edge_id) || issueMaps.edgeIssues.get(edgeKey(edge)) || [];
  return `
    <g>
      <path class="graph-edge${issues.length ? " has-issues" : ""}" data-graph-edge-key="${escapeHtml(edgeKey(edge))}" d="${path}" marker-end="url(#${markerId})"></path>
      <text class="graph-edge-label" x="${midX}" y="${midY - 8}" text-anchor="middle">${escapeHtml(edge.label || `${edge.source_node_id} -> ${edge.target_node_id}`)}</text>
    </g>
  `;
}

function renderGraphNode(node, position, layout, issueMaps, selectedNodeId, effectNodeIds) {
  if (!position) {
    return "";
  }
  const nodeIssues = issueMaps.nodeIssues.get(node.node_id) || [];
  const selected = selectedNodeId === node.node_id;
  const effectActive = effectNodeIds.has(node.node_id);
  const line1 = node.name || node.node_id;
  const line2 = `${node.node_id} | ${node.node_type || "n/a"}`;
  const line3 = node.config?.kind ? `kind: ${node.config.kind}` : "kind: n/a";
  return `
    <g
      class="graph-node${selected ? " selected" : ""}${nodeIssues.length ? " has-issues" : ""}${effectActive ? " effect-active" : ""}"
      data-graph-node-id="${escapeHtml(node.node_id)}"
      tabindex="0"
      role="button"
      aria-label="${escapeHtml(`${node.node_id} ${node.node_type || ""}`)}"
    >
      <title>${escapeHtml(`${node.node_id} | ${node.node_type || "n/a"} | ${node.name || "unnamed"}${node.config?.kind ? ` | ${node.config.kind}` : ""}`)}</title>
      <rect
        class="graph-node-box type-${escapeHtml(node.node_type || "unknown")}"
        x="${position.x}"
        y="${position.y}"
        rx="14"
        ry="14"
        width="${layout.nodeWidth}"
        height="${layout.nodeHeight}"
      ></rect>
      <text class="graph-node-text graph-node-title" x="${position.x + 14}" y="${position.y + 24}">${escapeHtml(truncateText(line1, 26))}</text>
      <text class="graph-node-subtext" x="${position.x + 14}" y="${position.y + 46}">${escapeHtml(truncateText(line2, 30))}</text>
      <text class="graph-node-subtext" x="${position.x + 14}" y="${position.y + 66}">${escapeHtml(truncateText(line3, 30))}</text>
      ${nodeIssues.length ? `
        <circle class="graph-issue-dot" cx="${position.x + layout.nodeWidth - 14}" cy="${position.y + 14}" r="11"></circle>
        <text class="graph-issue-count" x="${position.x + layout.nodeWidth - 14}" y="${position.y + 18}" text-anchor="middle">${escapeHtml(String(Math.min(nodeIssues.length, 99)))}</text>
      ` : ""}
    </g>
  `;
}

function bindGraphNodeEvents(context, nodes, validation, dryRun, issueMaps) {
  const ids = GRAPH_IDS[context];
  document.getElementById(ids.graph).querySelectorAll("[data-graph-node-id]").forEach((element) => {
    const selectNode = () => {
      GRAPH_SELECTION[context] = element.getAttribute("data-graph-node-id");
      const node = nodes.find((candidate) => candidate.node_id === GRAPH_SELECTION[context]) || null;
      renderNodeDetail(context, node, issueMaps, dryRun);
      document.getElementById(ids.graph).querySelectorAll("[data-graph-node-id]").forEach((candidate) => {
        candidate.classList.toggle("selected", candidate.getAttribute("data-graph-node-id") === GRAPH_SELECTION[context]);
      });
    };
    element.addEventListener("click", selectNode);
    element.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectNode();
      }
    });
  });
}

function buildEffectNodeIds(nodes, dryRun) {
  const active = new Set();
  if (!dryRun) {
    return active;
  }
  for (const node of nodes) {
    const kind = cleanString(node?.config?.kind);
    if ((kind === "internal_observation_store" && dryRun.would_store_observation)
      || (kind === "mqtt_publish" && dryRun.would_publish_mqtt)
      || (kind === "http_forward" && dryRun.would_forward_http)
      || (kind === "event_create" && dryRun.would_create_event)
      || (kind === "command_create" && dryRun.would_create_command)
      || ((kind === "dlq" || node?.node_type === "dlq") && dryRun.would_use_dlq)) {
      active.add(node.node_id);
    }
  }
  return active;
}

function edgeKey(edge) {
  return edge.edge_id || `${edge.source_node_id}->${edge.target_node_id}`;
}

function truncateText(value, maxLength) {
  const text = String(value || "");
  return text.length > maxLength ? `${text.slice(0, Math.max(0, maxLength - 3))}...` : text;
}
