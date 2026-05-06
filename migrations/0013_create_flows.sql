CREATE TABLE IF NOT EXISTS flows (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    flow_key TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    nodes JSONB NOT NULL DEFAULT '[]'::jsonb,
    edges JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT flows_tenant_key_unique UNIQUE (tenant_id, flow_key)
);

CREATE INDEX IF NOT EXISTS idx_flows_tenant_created_at
    ON flows (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_flows_flow_key
    ON flows (flow_key);

CREATE INDEX IF NOT EXISTS idx_flows_enabled
    ON flows (enabled);
