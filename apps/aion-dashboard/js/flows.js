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

const SOURCE_KIND_OPTIONS = [
  "mqtt_subscribe",
  "http_input",
  "ttn_uplink",
  "internal_observation",
];

const DECODER_KIND_OPTIONS = [
  "senml_decode",
  "ultralight_decode",
];

const TRANSFORM_KIND_OPTIONS = [
  "canonical_json",
  "json_map",
];

const FILTER_KIND_OPTIONS = [
  "filter_condition",
];

const RULE_KIND_OPTIONS = [
  "threshold_rule",
];

const SINK_KIND_OPTIONS = [
  "internal_observation_store",
  "raw_message_store",
  "mqtt_publish",
  "http_forward",
  "event_create",
  "command_create",
];

const DLQ_KIND_OPTIONS = [
  "dlq",
];

const EDITABLE_CHAIN_NODE_TYPES = new Set(["decoder", "transform", "filter", "rule"]);
const STORED_COPY_ALLOWED_NODE_TYPES = new Set(["source", "decoder", "transform", "filter", "rule", "sink", "dlq"]);

export function initializeFlowBuilder() {
  document.getElementById("flow-proposed-sample-payload").value = DEFAULT_FLOW_SAMPLE_PAYLOAD;
  document.getElementById("flow-stored-sample-payload").value = DEFAULT_FLOW_SAMPLE_PAYLOAD;
  populateFlowConnectorSelects([]);
  GRAPH_SELECTION.proposed = null;
  state.flowDraft = createDefaultFlowDraft();
  renderFlowBuilderFormFromDraft();
  invalidateProposedFlowChecks({ preserveGraph: true });
  renderFlowDraftPreview();
}

export function bindFlowEvents() {
  document.getElementById("refresh-flows-button").addEventListener("click", () => {
    loadFlows(true);
  });

  document.getElementById("refresh-flow-connectors-button").addEventListener("click", () => {
    loadFlowBuilderConnectors(true);
  });

  document.getElementById("flow-builder-form").addEventListener("input", () => {
    syncFlowDraftFromForm({ resetAdvancedOverride: false, invalidateChecks: true });
  });

  document.getElementById("flow-builder-form").addEventListener("change", () => {
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

  document.getElementById("flow-add-transform-button").addEventListener("click", () => {
    addDraftChainNode("transform");
  });

  document.getElementById("flow-add-filter-button").addEventListener("click", () => {
    addDraftChainNode("filter");
  });

  document.getElementById("flow-add-rule-button").addEventListener("click", () => {
    addDraftChainNode("rule");
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

  document.getElementById("flow-copy-to-draft-button").addEventListener("click", () => {
    copyStoredFlowToDraft();
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
  const options = buildConnectorOptionList(connectors, overviewById);
  const html = renderConnectorOptions(options);

  sourceSelect.innerHTML = html;
  sinkSelect.innerHTML = html;
  sourceSelect.value = options.some((item) => item.value === selectedSource) ? selectedSource : "";
  sinkSelect.value = options.some((item) => item.value === selectedSink) ? selectedSink : "";
  syncFlowConnectorFieldState();
  renderDraftChain();
  renderProposedGraphState();
}

function buildConnectorOptionList(connectors, overviewById) {
  return (connectors || []).map((connector) => {
    const summary = overviewById.get(connector.id);
    const label = connector.display_name || connector.connector_key || connector.id;
    const suffix = [connector.connector_type, summary?.payload_format].filter(Boolean).join(" / ");
    return {
      value: connector.id,
      label: suffix ? `${label} (${suffix})` : label,
    };
  });
}

function renderConnectorOptions(options, selectedValue = "") {
  return ['<option value="">None</option>'].concat(options.map((option) => `
    <option value="${escapeHtml(option.value)}"${option.value === selectedValue ? " selected" : ""}>${escapeHtml(option.label)}</option>
  `)).join("");
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
  state.flowDraft = createDefaultFlowDraft();
  renderFlowBuilderFormFromDraft();
  invalidateProposedFlowChecks({ preserveGraph: true });
  renderFlowDraftPreview();
  setStatus("Flow builder reset.");
}

function createDefaultFlowDraft() {
  const draft = {
    flow_key: "mqtt-normalize-store",
    name: "MQTT Normalize Store",
    description: "MQTT uplink to canonical observation planning",
    enabled: false,
    metadata: {
      category: "ingestion",
      notes: "execution not implemented",
    },
    nodes: [
      {
        node_id: "source-1",
        node_type: "source",
        name: "MQTT Source",
        config: { kind: "mqtt_subscribe" },
      },
      {
        node_id: "transform-1",
        node_type: "transform",
        name: "Canonical Map",
        config: { kind: "canonical_json" },
      },
      {
        node_id: "sink-1",
        node_type: "sink",
        name: "Observation Store",
        config: { kind: "internal_observation_store" },
      },
    ],
    edges: [],
  };
  return finalizeDraftGraph(draft);
}

function syncFlowDraftFromForm(options = {}) {
  const { resetAdvancedOverride = false, invalidateChecks = false } = options;
  if (resetAdvancedOverride) {
    document.getElementById("flow-advanced-json").value = "";
  }

  try {
    if (!state.flowDraft) {
      state.flowDraft = createDefaultFlowDraft();
    }

    state.flowDraft = finalizeDraftGraph(applyFormFieldsToDraft(state.flowDraft));
    if (invalidateChecks) {
      invalidateProposedFlowChecks({ preserveGraph: true });
    }
  } catch (_error) {
    if (invalidateChecks) {
      invalidateProposedFlowChecks({ preserveGraph: true });
    }
  }
  renderFlowDraftPreview();
}

function applyFormFieldsToDraft(draft) {
  const form = document.getElementById("flow-builder-form");
  const next = cloneJson(draft);
  const sourceNode = getSourceNode(next);
  const terminalNode = getTerminalNode(next);
  const metadata = readOptionalJsonText(form.elements.metadata.value, "Metadata JSON");

  next.flow_key = requireNonEmpty(form.elements.flow_key.value, "Flow key");
  next.name = requireNonEmpty(form.elements.name.value, "Name");
  next.description = cleanString(form.elements.description.value) || undefined;
  next.enabled = Boolean(form.elements.enabled.checked);
  next.metadata = metadata;

  sourceNode.node_id = requireNonEmpty(form.elements.source_node_id.value, "Source node ID");
  sourceNode.name = cleanString(form.elements.source_name.value) || undefined;
  sourceNode.node_type = "source";
  sourceNode.config = buildNodeConfig("source", requireNonEmpty(form.elements.source_kind.value, "Source kind"), cleanString(form.elements.source_connector_id.value));

  terminalNode.node_id = requireNonEmpty(form.elements.sink_node_id.value, "Sink node ID");
  terminalNode.name = cleanString(form.elements.sink_name.value) || undefined;
  const sinkKind = requireNonEmpty(form.elements.sink_kind.value, "Sink kind");
  terminalNode.node_type = sinkKind === "dlq" ? "dlq" : "sink";
  terminalNode.config = buildNodeConfig(terminalNode.node_type, sinkKind, cleanString(form.elements.sink_connector_id.value));

  return next;
}

function renderFlowBuilderFormFromDraft() {
  const form = document.getElementById("flow-builder-form");
  const draft = state.flowDraft || createDefaultFlowDraft();
  const sourceNode = getSourceNode(draft);
  const terminalNode = getTerminalNode(draft);
  const sourceKind = cleanString(sourceNode?.config?.kind) || SOURCE_KIND_OPTIONS[0];
  const sinkKind = cleanString(terminalNode?.config?.kind) || SINK_KIND_OPTIONS[0];

  form.elements.flow_key.value = draft.flow_key || "";
  form.elements.name.value = draft.name || "";
  form.elements.description.value = draft.description || "";
  form.elements.enabled.checked = Boolean(draft.enabled);
  form.elements.metadata.value = draft.metadata ? JSON.stringify(redactSecrets(draft.metadata), null, 2) : "";

  form.elements.source_node_id.value = sourceNode?.node_id || "";
  form.elements.source_name.value = sourceNode?.name || "";
  form.elements.source_kind.value = SOURCE_KIND_OPTIONS.includes(sourceKind) ? sourceKind : SOURCE_KIND_OPTIONS[0];
  form.elements.source_connector_id.value = FLOW_SOURCE_KINDS_WITH_CONNECTOR.has(sourceKind)
    ? cleanString(sourceNode?.config?.connector_id)
    : "";

  form.elements.sink_node_id.value = terminalNode?.node_id || "";
  form.elements.sink_name.value = terminalNode?.name || "";
  form.elements.sink_kind.value = sinkKind === "dlq" || SINK_KIND_OPTIONS.includes(sinkKind) ? sinkKind : SINK_KIND_OPTIONS[0];
  form.elements.sink_connector_id.value = FLOW_SINK_KINDS_WITH_CONNECTOR.has(sinkKind)
    ? cleanString(terminalNode?.config?.connector_id)
    : "";

  syncFlowConnectorFieldState();
}

function buildNodeConfig(nodeType, kind, connectorId) {
  const config = { kind };
  if (nodeType === "source" && FLOW_SOURCE_KINDS_WITH_CONNECTOR.has(kind) && connectorId) {
    config.connector_id = connectorId;
  }
  if ((nodeType === "sink" || nodeType === "dlq") && FLOW_SINK_KINDS_WITH_CONNECTOR.has(kind) && connectorId) {
    config.connector_id = connectorId;
  }
  return config;
}

function getEffectiveFlowDraft() {
  const advanced = cleanString(document.getElementById("flow-advanced-json").value);
  if (advanced) {
    const parsed = parseJson(advanced, "Advanced JSON override");
    validateFlowDraftShape(parsed);
    state.flowDraft = state.flowDraft || createDefaultFlowDraft();
    renderFlowDraftPreview();
    return parsed;
  }

  if (!state.flowDraft) {
    state.flowDraft = createDefaultFlowDraft();
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
  if (!Array.isArray(draft.nodes) || draft.nodes.length < 2) {
    throw new Error("Flow draft requires at least source and sink nodes.");
  }
  if (!Array.isArray(draft.edges) || draft.edges.length === 0) {
    throw new Error("Flow draft requires edges.");
  }
}

function renderFlowDraftPreview() {
  const preview = document.getElementById("flow-preview-json");
  const builderStatus = document.getElementById("flow-builder-status");

  try {
    const advanced = cleanString(document.getElementById("flow-advanced-json").value);
    const draft = advanced
      ? parseJson(document.getElementById("flow-advanced-json").value, "Advanced JSON override")
      : finalizeDraftGraph(state.flowDraft || createDefaultFlowDraft());

    preview.textContent = JSON.stringify(redactSecrets(draft), null, 2);
    builderStatus.textContent = advanced
      ? "Advanced JSON override is active. Constrained visual editing is disabled until the override is cleared. Preview stays redacted for display."
      : "Guided builder output is active. Constrained visual editing is limited to a linear source -> chain -> sink draft. Preview stays redacted for display.";

    renderDraftChain();
    renderProposedGraphState(draft);
  } catch (error) {
    preview.textContent = error.message;
    builderStatus.textContent = "Fix the builder fields or advanced JSON before validating, dry-running, or creating the flow.";
    renderDraftChain(error.message);
    renderGraphEmpty("proposed", error.message, "Preview is unavailable until the draft JSON parses and matches the flow shape.");
    renderGraphIssues("proposed", null, "Fix the draft JSON before validation issues can be shown.");
    renderGraphEffects("proposed", null);
  }
}

function renderDraftChain(errorMessage = "") {
  const container = document.getElementById("flow-draft-chain");
  const advancedActive = hasAdvancedOverride();
  if (errorMessage) {
    container.innerHTML = `<div class="flow-graph-empty">${escapeHtml(errorMessage)}</div>`;
    return;
  }
  if (advancedActive) {
    container.innerHTML = '<div class="flow-graph-empty">Clear the advanced JSON override to use constrained visual draft editing.</div>';
    return;
  }

  const draft = state.flowDraft || createDefaultFlowDraft();
  const chain = orderedLinearNodes(draft.nodes);
  container.innerHTML = chain.map((node) => {
    const selected = GRAPH_SELECTION.proposed === node.node_id;
    const connectorId = cleanString(node?.config?.connector_id);
    return `
      <div class="flow-chain-card${selected ? " selected" : ""}">
        <div>
          <strong>${escapeHtml(node.name || node.node_id)}</strong>
          <span>${escapeHtml(`${node.node_id} | ${node.node_type || "n/a"} | ${node.config?.kind || "n/a"}`)}</span>
          <span>${escapeHtml(connectorId ? `connector=${connectorId}` : "connector=none")}</span>
        </div>
        <div class="flow-chain-actions">
          <button class="button subtle" type="button" data-select-proposed-node="${escapeHtml(node.node_id)}">Select</button>
        </div>
      </div>
    `;
  }).join("");

  container.querySelectorAll("[data-select-proposed-node]").forEach((button) => {
    button.addEventListener("click", () => {
      GRAPH_SELECTION.proposed = button.getAttribute("data-select-proposed-node");
      renderFlowDraftPreview();
    });
  });
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
  const copyButton = document.getElementById("flow-copy-to-draft-button");
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
    copyButton.disabled = true;
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
  copyButton.disabled = false;
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
    renderFlowGraph("proposed", flow, state.cache.flowProposedValidation, state.cache.flowProposedDryRun, "Constrained preview graph from the current draft.");
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
  renderFlowGraph("stored", detail, validation, dryRun, "Stored graph preview from GET /dashboard/flows/{flow_id}. Read-only until copied into a builder draft.");
}

function getCurrentDraftForGraph() {
  const advanced = cleanString(document.getElementById("flow-advanced-json").value);
  const draft = advanced
    ? parseJson(advanced, "Advanced JSON override")
    : finalizeDraftGraph(state.flowDraft || createDefaultFlowDraft());
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
  bindGraphNodeEvents(context, nodes, dryRun, issueMaps);

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

  if (context === "proposed" && !hasAdvancedOverride()) {
    container.innerHTML = renderEditableDraftNode(node, issueMaps, dryRun);
    bindEditableDraftNodeEvents(node.node_id);
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

function renderEditableDraftNode(node, issueMaps, dryRun) {
  const nodeIssues = issueMaps.nodeIssues.get(node.node_id) || [];
  const effectActive = buildEffectNodeIds([node], dryRun).has(node.node_id);
  const kindOptions = getKindOptionsForNodeType(node.node_type).map((value) => `
    <option value="${escapeHtml(value)}"${value === cleanString(node?.config?.kind) ? " selected" : ""}>${escapeHtml(value)}</option>
  `).join("");
  const showConnector = nodeSupportsConnector(node);
  const connectorOptions = renderConnectorOptions(
    buildConnectorOptionList(
      state.cache.flowConnectors || [],
      new Map((state.cache.flowConnectorOverview?.connectors || []).map((item) => [item.connector_id, item])),
    ),
    cleanString(node?.config?.connector_id),
  );
  const canMoveUp = canMoveDraftNode(node.node_id, -1);
  const canMoveDown = canMoveDraftNode(node.node_id, 1);
  const canRemove = canRemoveDraftNode(node.node_id);
  const redactedConfig = JSON.stringify(redactSecrets(node.config || {}), null, 2) || "{}";

  return `
    <div class="flow-node-editor">
      <div class="detail-grid">
        ${renderDetailItem("Node ID", node.node_id)}
        ${renderDetailItem("Node Type", node.node_type || "n/a")}
        ${renderDetailItem("Highlighted By Dry-Run", effectActive ? "true" : "false")}
      </div>
      <div class="flow-node-editor-grid">
        <label>
          <span>Name</span>
          <input id="flow-draft-node-name" type="text" value="${escapeHtml(node.name || "")}" ${hasAdvancedOverride() ? "disabled" : ""}>
        </label>
        <label>
          <span>Kind</span>
          <select id="flow-draft-node-kind" ${kindOptions ? "" : "disabled"}>
            ${kindOptions}
          </select>
        </label>
        <label>
          <span>Connector</span>
          <select id="flow-draft-node-connector" ${showConnector ? "" : "disabled"}>
            ${connectorOptions}
          </select>
        </label>
      </div>
      <div class="flow-node-editor-actions">
        <button class="button subtle" type="button" id="flow-draft-node-move-up"${canMoveUp ? "" : " disabled"}>Move Up</button>
        <button class="button subtle" type="button" id="flow-draft-node-move-down"${canMoveDown ? "" : " disabled"}>Move Down</button>
        <button class="button subtle danger-button" type="button" id="flow-draft-node-remove"${canRemove ? "" : " disabled"}>Remove Node</button>
      </div>
      <p class="flow-node-editor-note">Editing stays constrained to the current linear draft. Stored flows remain read-only until copied to the builder draft.</p>
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

function bindEditableDraftNodeEvents(nodeId) {
  const nameInput = document.getElementById("flow-draft-node-name");
  const kindSelect = document.getElementById("flow-draft-node-kind");
  const connectorSelect = document.getElementById("flow-draft-node-connector");
  const moveUpButton = document.getElementById("flow-draft-node-move-up");
  const moveDownButton = document.getElementById("flow-draft-node-move-down");
  const removeButton = document.getElementById("flow-draft-node-remove");

  if (nameInput) {
    nameInput.addEventListener("input", () => {
      updateDraftNode(nodeId, (node) => {
        node.name = cleanString(nameInput.value) || undefined;
      });
    });
  }

  if (kindSelect) {
    kindSelect.addEventListener("change", () => {
      updateDraftNode(nodeId, (node) => {
        const kind = requireNonEmpty(kindSelect.value, "Node kind");
        node.config = buildNodeConfig(node.node_type, kind, cleanString(node?.config?.connector_id));
      });
    });
  }

  if (connectorSelect) {
    connectorSelect.addEventListener("change", () => {
      updateDraftNode(nodeId, (node) => {
        const kind = cleanString(node?.config?.kind);
        node.config = buildNodeConfig(node.node_type, kind, cleanString(connectorSelect.value));
      });
    });
  }

  if (moveUpButton) {
    moveUpButton.addEventListener("click", () => {
      moveDraftChainNode(nodeId, -1);
    });
  }

  if (moveDownButton) {
    moveDownButton.addEventListener("click", () => {
      moveDraftChainNode(nodeId, 1);
    });
  }

  if (removeButton) {
    removeButton.addEventListener("click", () => {
      removeDraftChainNode(nodeId);
    });
  }
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
  const positionById = new Map((layout.nodes || []).map((item) => [item.node_id, item]));
  const markerId = `flow-graph-arrow-${context}`;
  return `
    <svg viewBox="0 0 ${layout.width} ${layout.height}" role="img" aria-label="${escapeHtml(`${context} flow graph`)}">
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

function bindGraphNodeEvents(context, nodes, dryRun, issueMaps) {
  const ids = GRAPH_IDS[context];
  document.getElementById(ids.graph).querySelectorAll("[data-graph-node-id]").forEach((element) => {
    const selectNode = () => {
      GRAPH_SELECTION[context] = element.getAttribute("data-graph-node-id");
      const node = nodes.find((candidate) => candidate.node_id === GRAPH_SELECTION[context]) || null;
      renderNodeDetail(context, node, issueMaps, dryRun);
      if (context === "proposed") {
        renderDraftChain();
      }
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

function addDraftChainNode(nodeType) {
  if (hasAdvancedOverride()) {
    setStatus("Clear the advanced JSON override before using constrained visual draft editing.");
    return;
  }

  const draft = finalizeDraftGraph(state.flowDraft || createDefaultFlowDraft());
  const intermediates = getIntermediateNodes(draft);
  const targetIndex = resolveDraftInsertionIndex(draft);
  const node = buildNewDraftNode(nodeType, intermediates.length + 1);
  intermediates.splice(targetIndex, 0, node);
  draft.nodes = [getSourceNode(draft), ...intermediates, getTerminalNode(draft)];
  state.flowDraft = finalizeDraftGraph(draft);
  GRAPH_SELECTION.proposed = node.node_id;
  invalidateProposedFlowChecks({ preserveGraph: true });
  renderFlowBuilderFormFromDraft();
  renderFlowDraftPreview();
  setStatus(`${capitalize(nodeType)} node added to the constrained draft.`);
}

function buildNewDraftNode(nodeType, sequence) {
  const normalizedType = nodeType === "transform" ? "transform" : nodeType;
  const defaultKind = getKindOptionsForNodeType(normalizedType)[0];
  return {
    node_id: `${normalizedType}-${sequence}`,
    node_type: normalizedType,
    name: buildDefaultNodeName(normalizedType, sequence),
    config: { kind: defaultKind },
  };
}

function buildDefaultNodeName(nodeType, sequence) {
  if (nodeType === "filter") {
    return `Filter ${sequence}`;
  }
  if (nodeType === "rule") {
    return `Rule ${sequence}`;
  }
  if (nodeType === "decoder") {
    return `Decoder ${sequence}`;
  }
  return `Transform ${sequence}`;
}

function resolveDraftInsertionIndex(draft) {
  const intermediates = getIntermediateNodes(draft);
  const selectedId = GRAPH_SELECTION.proposed;
  const selectedIndex = intermediates.findIndex((node) => node.node_id === selectedId);
  if (selectedIndex >= 0) {
    return selectedIndex + 1;
  }
  const sourceSelected = getSourceNode(draft)?.node_id === selectedId;
  if (sourceSelected) {
    return 0;
  }
  return intermediates.length;
}

function updateDraftNode(nodeId, updater) {
  if (hasAdvancedOverride()) {
    setStatus("Clear the advanced JSON override before editing the constrained draft.");
    return;
  }

  const draft = finalizeDraftGraph(state.flowDraft || createDefaultFlowDraft());
  const node = draft.nodes.find((candidate) => candidate.node_id === nodeId);
  if (!node) {
    return;
  }

  updater(node);
  if (node.node_type === "source" || node.node_type === "sink" || node.node_type === "dlq") {
    renderFlowBuilderFormFromDraft();
  }
  state.flowDraft = finalizeDraftGraph(draft);
  GRAPH_SELECTION.proposed = node.node_id;
  invalidateProposedFlowChecks({ preserveGraph: true });
  renderFlowBuilderFormFromDraft();
  renderFlowDraftPreview();
}

function moveDraftChainNode(nodeId, direction) {
  if (hasAdvancedOverride()) {
    setStatus("Clear the advanced JSON override before reordering the constrained draft.");
    return;
  }
  if (!canMoveDraftNode(nodeId, direction)) {
    return;
  }

  const draft = finalizeDraftGraph(state.flowDraft || createDefaultFlowDraft());
  const intermediates = getIntermediateNodes(draft);
  const index = intermediates.findIndex((node) => node.node_id === nodeId);
  const targetIndex = index + direction;
  const [node] = intermediates.splice(index, 1);
  intermediates.splice(targetIndex, 0, node);
  draft.nodes = [getSourceNode(draft), ...intermediates, getTerminalNode(draft)];
  state.flowDraft = finalizeDraftGraph(draft);
  GRAPH_SELECTION.proposed = nodeId;
  invalidateProposedFlowChecks({ preserveGraph: true });
  renderFlowDraftPreview();
}

function removeDraftChainNode(nodeId) {
  if (hasAdvancedOverride()) {
    setStatus("Clear the advanced JSON override before removing nodes from the constrained draft.");
    return;
  }
  if (!canRemoveDraftNode(nodeId)) {
    return;
  }

  const draft = finalizeDraftGraph(state.flowDraft || createDefaultFlowDraft());
  draft.nodes = draft.nodes.filter((node) => node.node_id !== nodeId);
  state.flowDraft = finalizeDraftGraph(draft);
  GRAPH_SELECTION.proposed = getSourceNode(state.flowDraft)?.node_id || null;
  invalidateProposedFlowChecks({ preserveGraph: true });
  renderFlowDraftPreview();
  setStatus(`Removed node ${nodeId} from the constrained draft.`);
}

function canMoveDraftNode(nodeId, direction) {
  const draft = state.flowDraft || createDefaultFlowDraft();
  const intermediates = getIntermediateNodes(draft);
  const index = intermediates.findIndex((node) => node.node_id === nodeId);
  if (index < 0) {
    return false;
  }
  const targetIndex = index + direction;
  return targetIndex >= 0 && targetIndex < intermediates.length;
}

function canRemoveDraftNode(nodeId) {
  const draft = state.flowDraft || createDefaultFlowDraft();
  const node = draft.nodes.find((candidate) => candidate.node_id === nodeId);
  return Boolean(node && EDITABLE_CHAIN_NODE_TYPES.has(node.node_type));
}

function nodeSupportsConnector(node) {
  const kind = cleanString(node?.config?.kind);
  return (node?.node_type === "source" && FLOW_SOURCE_KINDS_WITH_CONNECTOR.has(kind))
    || ((node?.node_type === "sink" || node?.node_type === "dlq") && FLOW_SINK_KINDS_WITH_CONNECTOR.has(kind));
}

function getKindOptionsForNodeType(nodeType) {
  if (nodeType === "source") {
    return SOURCE_KIND_OPTIONS;
  }
  if (nodeType === "decoder") {
    return DECODER_KIND_OPTIONS;
  }
  if (nodeType === "transform") {
    return TRANSFORM_KIND_OPTIONS;
  }
  if (nodeType === "filter") {
    return FILTER_KIND_OPTIONS;
  }
  if (nodeType === "rule") {
    return RULE_KIND_OPTIONS;
  }
  if (nodeType === "dlq") {
    return DLQ_KIND_OPTIONS;
  }
  if (nodeType === "sink") {
    return SINK_KIND_OPTIONS;
  }
  return [];
}

function finalizeDraftGraph(draft) {
  const next = cloneJson(draft);
  const orderedNodes = orderedLinearNodes(next.nodes);
  next.nodes = assignLinearPositions(orderedNodes);
  next.edges = buildLinearEdges(next.nodes);
  return next;
}

function orderedLinearNodes(nodes) {
  const normalizedNodes = normalizeNodes(nodes);
  const source = normalizedNodes.find((node) => node.node_type === "source") || normalizedNodes[0];
  const terminal = normalizedNodes.find((node) => node.node_type === "sink" || node.node_type === "dlq") || normalizedNodes[normalizedNodes.length - 1];
  const intermediates = normalizedNodes.filter((node) => node.node_id !== source?.node_id && node.node_id !== terminal?.node_id);
  return [source, ...intermediates, terminal].filter(Boolean);
}

function assignLinearPositions(nodes) {
  return nodes.map((node, index) => ({
    ...node,
    position: {
      x: 60 + (index * 240),
      y: 120,
    },
  }));
}

function buildLinearEdges(nodes) {
  const edges = [];
  for (let index = 0; index < nodes.length - 1; index += 1) {
    const source = nodes[index];
    const target = nodes[index + 1];
    edges.push({
      edge_id: `${source.node_id}-to-${target.node_id}`,
      source_node_id: source.node_id,
      target_node_id: target.node_id,
    });
  }
  return edges;
}

function getSourceNode(draft) {
  return normalizeNodes(draft.nodes).find((node) => node.node_type === "source");
}

function getTerminalNode(draft) {
  const nodes = normalizeNodes(draft.nodes);
  return nodes.find((node) => node.node_type === "sink" || node.node_type === "dlq");
}

function getIntermediateNodes(draft) {
  return orderedLinearNodes(draft.nodes).filter((node) => EDITABLE_CHAIN_NODE_TYPES.has(node.node_type));
}

function hasAdvancedOverride() {
  return Boolean(cleanString(document.getElementById("flow-advanced-json").value));
}

function copyStoredFlowToDraft() {
  try {
    const detail = state.selectedFlowId ? state.cache.flowDetail.get(String(state.selectedFlowId)) : null;
    if (!detail) {
      return;
    }
    const draft = createConstrainedDraftFromStoredFlow(detail);
    document.getElementById("flow-advanced-json").value = "";
    state.flowDraft = draft;
    GRAPH_SELECTION.proposed = getSourceNode(draft)?.node_id || draft.nodes[0]?.node_id || null;
    invalidateProposedFlowChecks({ preserveGraph: true });
    renderFlowBuilderFormFromDraft();
    renderFlowDraftPreview();
    setStatus(`Copied stored flow ${detail.flow.flow_key} into the constrained builder draft.`);
  } catch (error) {
    handleError(error);
  }
}

function createConstrainedDraftFromStoredFlow(detail) {
  const nodes = normalizeNodes(detail?.nodes);
  const edges = normalizeEdges(detail?.edges);
  if (nodes.length < 2 || !isLinearGraph(nodes, edges)) {
    throw new Error("Stored flow cannot be copied into the constrained draft because it is not a single linear chain.");
  }

  const ordered = topologicalOrder(nodes, edges);
  const sourceCount = ordered.filter((node) => node.node_type === "source").length;
  const terminalCount = ordered.filter((node) => node.node_type === "sink" || node.node_type === "dlq").length;
  if (sourceCount !== 1 || terminalCount !== 1) {
    throw new Error("Stored flow cannot be copied into the constrained draft because it does not have exactly one source and one terminal sink or DLQ.");
  }

  for (const node of ordered) {
    if (!STORED_COPY_ALLOWED_NODE_TYPES.has(node.node_type)) {
      throw new Error(`Stored flow node type ${node.node_type} is not supported by the constrained draft editor.`);
    }
    const supportedKinds = getKindOptionsForNodeType(node.node_type);
    const kind = cleanString(node?.config?.kind);
    if (supportedKinds.length > 0 && !supportedKinds.includes(kind)) {
      throw new Error(`Stored flow kind ${kind || "unknown"} for node ${node.node_id} is not supported by the constrained draft editor.`);
    }
  }

  const next = {
    flow_key: `${detail.flow.flow_key}-copy`,
    name: `${detail.flow.name || detail.flow.flow_key} Copy`,
    description: detail.flow.description || undefined,
    enabled: false,
    metadata: cloneJson(detail.flow.metadata || detail.metadata),
    nodes: ordered.map((node) => ({
      node_id: node.node_id,
      node_type: node.node_type,
      name: node.name || undefined,
      config: cloneJson(node.config || {}),
    })),
    edges: [],
  };
  return finalizeDraftGraph(next);
}

function edgeKey(edge) {
  return edge.edge_id || `${edge.source_node_id}->${edge.target_node_id}`;
}

function truncateText(value, maxLength) {
  const text = String(value || "");
  return text.length > maxLength ? `${text.slice(0, Math.max(0, maxLength - 3))}...` : text;
}

function cloneJson(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function capitalize(value) {
  return value ? `${value.charAt(0).toUpperCase()}${value.slice(1)}` : "";
}
