import { state } from "./state.js";
import { redactSecrets } from "./utils.js";

export async function apiGet(path) {
  return apiRequest("GET", path);
}

export async function apiPost(path, body) {
  return apiRequest("POST", path, body);
}

export async function apiPatch(path, body) {
  return apiRequest("PATCH", path, body);
}

export async function apiPut(path, body) {
  return apiRequest("PUT", path, body);
}

export async function apiDelete(path) {
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

  const scopeHint = buildScopeHint(path);

  if (response.status === 401) {
    return new Error(`Missing or invalid token for ${path}.${scopeHint}${detail ? ` ${detail}` : ""}`);
  }

  if (response.status === 403) {
    return new Error(`Token lacks required scope for ${path}.${scopeHint}${detail ? ` ${detail}` : ""}`);
  }

  return new Error(`Request failed for ${path}: ${response.status} ${response.statusText}${detail ? ` - ${detail}` : ""}`);
}

function buildScopeHint(path) {
  if (path === "/flows/execute" || /^\/flows\/[^/]+\/execute$/.test(path)) {
    return " Simulated execute requires `flows:read`; stored flow selection still uses `dashboard:read`, and connector selectors may also need `connectors:read`.";
  }
  if (path === "/flows/validate" || path === "/flows/dry-run" || /^\/flows\/[^/]+\/(validation|dry-run)$/.test(path)) {
    return " Flow validation and dry-run require `flows:read`; stored flow selection still uses `dashboard:read`.";
  }
  if (path === "/dashboard/flows" || /^\/dashboard\/flows\/[^/]+$/.test(path)) {
    return " Dashboard flow reads require `dashboard:read`.";
  }
  return "";
}
