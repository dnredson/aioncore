import { DEFAULT_API_BASE_URL } from "./constants.js";

export const state = {
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

export const sectionTitles = {
  overview: "Overview",
  timeseries: "Time Series",
  connectors: "Connectors",
  flows: "Flows",
};

export const sectionModes = {
  overview: "Read-only",
  timeseries: "Read-only",
  connectors: "Read and admin",
  flows: "Read and admin",
};

export function clearCaches() {
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
