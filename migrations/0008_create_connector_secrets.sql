CREATE TABLE IF NOT EXISTS connector_secrets (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    secret_key TEXT NOT NULL,
    secret_type TEXT NOT NULL,
    username TEXT,
    secret_value TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, secret_key),
    CONSTRAINT connector_secrets_secret_type_check CHECK (
        secret_type IN ('mqtt_basic_auth', 'token', 'api_key', 'custom')
    )
);

CREATE INDEX IF NOT EXISTS idx_connector_secrets_tenant
    ON connector_secrets (tenant_id);

CREATE INDEX IF NOT EXISTS idx_connector_secrets_secret_key
    ON connector_secrets (secret_key);

CREATE INDEX IF NOT EXISTS idx_connector_secrets_secret_type
    ON connector_secrets (secret_type);

ALTER TABLE ingestion_connectors
    ADD COLUMN IF NOT EXISTS secret_ref_id UUID REFERENCES connector_secrets(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_ingestion_connectors_secret_ref
    ON ingestion_connectors (secret_ref_id);
