import {
  DEFAULT_API_BASE_URL,
  DEFAULT_TIMESERIES_LIMIT,
  MAX_TIMESERIES_LIMIT,
  SECRET_LIKE_KEYS,
} from "./constants.js";

export function cleanString(value) {
  return String(value || "").trim();
}

export function parseJson(value, label) {
  try {
    return JSON.parse(value);
  } catch (_error) {
    throw new Error(`${label} must be valid JSON.`);
  }
}

export function readOptionalJsonText(value, label) {
  const text = cleanString(value);
  if (!text) {
    return undefined;
  }
  return parseJson(text, label);
}

export function readOptionalJsonTextarea(elementId, label) {
  return readOptionalJsonText(document.getElementById(elementId).value, label);
}

export function requireNonEmpty(value, label) {
  const cleaned = cleanString(value);
  if (!cleaned) {
    throw new Error(`${label} is required.`);
  }
  return cleaned;
}

export function labelFromFieldName(name) {
  return name
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

export function isUuid(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

export function normalizeDateTimeInput(value) {
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

export function normalizeTimeseriesLimit(value) {
  const numeric = Number.parseInt(String(value || DEFAULT_TIMESERIES_LIMIT), 10);
  if (!Number.isFinite(numeric) || numeric < 1) {
    throw new Error("Limit must be a positive integer.");
  }
  return Math.min(numeric, MAX_TIMESERIES_LIMIT);
}

export function formatObservationValue(value) {
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

export function observationNumber(value) {
  if (value && value.type === "number" && typeof value.value === "number" && Number.isFinite(value.value)) {
    return value.value;
  }
  return null;
}

export function formatMaybeNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value.toLocaleString(undefined, { maximumFractionDigits: 6 });
  }
  return String(value ?? "n/a");
}

export function formatTimeseriesRange(from, to) {
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

export function buildTimeseriesChartSvg(points) {
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

export function formatDateTime(value) {
  if (!value) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

export function formatNumber(value) {
  return Number(value || 0).toLocaleString();
}

export function normalizeApiBaseUrl(value) {
  return String(value || DEFAULT_API_BASE_URL).trim().replace(/\/+$/, "");
}

export function badge(label, tone) {
  return `<span class="badge ${tone || ""}">${escapeHtml(String(label))}</span>`;
}

export function connectorBadgeTone(connector) {
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

export function workerPlanTone(status) {
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

export function workerRuntimeTone(status) {
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

export function validationBadgeTone(status) {
  if (status === "valid") {
    return "success";
  }
  if (status === "warning") {
    return "warning";
  }
  return "danger";
}

export function withGeneratedAt(message, generatedAt) {
  return generatedAt ? `${message} Generated at ${formatDateTime(generatedAt)}.` : message;
}

export function setStatus(message) {
  document.getElementById("global-status").textContent = message;
}

export function clearError() {
  const banner = document.getElementById("error-banner");
  banner.textContent = "";
  banner.classList.add("hidden");
}

export function handleError(error) {
  const banner = document.getElementById("error-banner");
  banner.textContent = error.message || "Unexpected error.";
  banner.classList.remove("hidden");
  setStatus("Last request failed.");
}

export function buildEmptyRow(columnCount, message) {
  return `<tr><td colspan="${columnCount}">${escapeHtml(message)}</td></tr>`;
}

export function renderDetailItem(label, value) {
  return `
    <div class="detail-item">
      <strong>${escapeHtml(label)}</strong>
      <span>${escapeHtml(String(value))}</span>
    </div>
  `;
}

export function renderKeyValueBlock(items, allowHtml = false) {
  return items.map(([label, value]) => `
    <p><strong>${escapeHtml(label)}:</strong> ${allowHtml ? (value || "n/a") : escapeHtml(value || "n/a")}</p>
  `).join("");
}

export function safeBrokerUrl(value) {
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

export function redactSecrets(value, parentKey = "") {
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

export function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
