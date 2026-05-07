export const DEFAULT_API_BASE_URL = "http://127.0.0.1:8080";

export const STORAGE_KEYS = {
  apiBaseUrl: "aion.dashboard.apiBaseUrl",
  bearerToken: "aion.dashboard.bearerToken",
};

export const DEFAULT_TIMESERIES_LIMIT = 1000;
export const MAX_TIMESERIES_LIMIT = 10000;
export const TIMESERIES_AGGREGATION_NONE = "none";
export const DEFAULT_FLOW_SAMPLE_PAYLOAD = '{\n  "temperature": 21.4\n}';

export const FLOW_SOURCE_KINDS_WITH_CONNECTOR = new Set([
  "mqtt_subscribe",
  "http_input",
  "ttn_uplink",
]);

export const FLOW_SINK_KINDS_WITH_CONNECTOR = new Set([
  "mqtt_publish",
  "http_forward",
]);

export const CONNECTOR_TYPE_OPTIONS = [
  { value: "mqtt", label: "MQTT" },
  { value: "http", label: "HTTP" },
  { value: "future", label: "Future / unsupported" },
];

export const CONNECTOR_PROFILE_OPTIONS = [
  { value: "generic-mqtt", label: "Generic MQTT" },
  { value: "generic-aion-mqtt", label: "Generic Aion MQTT" },
  { value: "ttn-v3", label: "TTN v3" },
  { value: "custom", label: "Custom / HTTP" },
];

export const SECRET_LIKE_KEYS = new Set([
  "password",
  "secret",
  "token",
  "api_key",
  "access_key",
  "private_key",
  "credential",
]);
