CREATE TABLE IF NOT EXISTS ingestion_connectors (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    connector_key TEXT NOT NULL,
    connector_type TEXT NOT NULL,
    connector_profile TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    display_name TEXT,
    protocol TEXT,
    endpoint TEXT,
    broker_url TEXT,
    client_id TEXT,
    topic_filter TEXT,
    http_path TEXT,
    payload_format TEXT,
    content_type TEXT,
    default_producer_entity_id UUID REFERENCES entities(id) ON DELETE SET NULL,
    default_feature_of_interest_id UUID REFERENCES entities(id) ON DELETE SET NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, connector_key),
    CONSTRAINT ingestion_connectors_connector_type_check CHECK (
        connector_type IN ('http', 'mqtt', 'future')
    ),
    CONSTRAINT ingestion_connectors_connector_profile_check CHECK (
        connector_profile IN ('generic-aion-mqtt', 'generic-mqtt', 'ttn-v3', 'custom')
    )
);

CREATE INDEX IF NOT EXISTS idx_ingestion_connectors_tenant
    ON ingestion_connectors (tenant_id);

CREATE INDEX IF NOT EXISTS idx_ingestion_connectors_connector_key
    ON ingestion_connectors (connector_key);

CREATE INDEX IF NOT EXISTS idx_ingestion_connectors_connector_type
    ON ingestion_connectors (connector_type);

CREATE INDEX IF NOT EXISTS idx_ingestion_connectors_connector_profile
    ON ingestion_connectors (connector_profile);

CREATE INDEX IF NOT EXISTS idx_ingestion_connectors_enabled
    ON ingestion_connectors (enabled);
