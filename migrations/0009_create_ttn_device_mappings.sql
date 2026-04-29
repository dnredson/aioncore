CREATE TABLE IF NOT EXISTS ttn_device_mappings (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    connector_id UUID NOT NULL REFERENCES ingestion_connectors(id) ON DELETE CASCADE,
    ttn_application_id TEXT,
    ttn_device_id TEXT NOT NULL,
    producer_entity_id UUID NOT NULL REFERENCES entities(id) ON DELETE RESTRICT,
    feature_of_interest_id UUID REFERENCES entities(id) ON DELETE SET NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, connector_id, ttn_application_id, ttn_device_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ttn_device_mappings_unique_no_application
    ON ttn_device_mappings (tenant_id, connector_id, ttn_device_id)
    WHERE ttn_application_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_ttn_device_mappings_tenant
    ON ttn_device_mappings (tenant_id);

CREATE INDEX IF NOT EXISTS idx_ttn_device_mappings_connector
    ON ttn_device_mappings (connector_id);

CREATE INDEX IF NOT EXISTS idx_ttn_device_mappings_device
    ON ttn_device_mappings (ttn_device_id);

CREATE INDEX IF NOT EXISTS idx_ttn_device_mappings_application
    ON ttn_device_mappings (ttn_application_id);

CREATE INDEX IF NOT EXISTS idx_ttn_device_mappings_enabled
    ON ttn_device_mappings (enabled);
