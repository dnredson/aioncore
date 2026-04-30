CREATE TABLE IF NOT EXISTS edge_adapters (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    adapter_key text NOT NULL,
    display_name text,
    adapter_type text NOT NULL,
    status text NOT NULL DEFAULT 'unknown',
    version text,
    host_id text,
    site_id text,
    environment text,
    last_seen_at timestamptz,
    metadata jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT edge_adapters_adapter_key_not_empty CHECK (length(trim(adapter_key)) > 0),
    CONSTRAINT edge_adapters_adapter_type_valid CHECK (
        adapter_type IN ('edge', 'fog', 'cloud', 'lab', 'custom')
    ),
    CONSTRAINT edge_adapters_status_valid CHECK (
        status IN ('online', 'offline', 'degraded', 'unknown')
    ),
    CONSTRAINT edge_adapters_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    ),
    CONSTRAINT edge_adapters_tenant_adapter_key_unique UNIQUE (tenant_id, adapter_key)
);

CREATE INDEX IF NOT EXISTS edge_adapters_adapter_key_idx
    ON edge_adapters (tenant_id, adapter_key);

CREATE INDEX IF NOT EXISTS edge_adapters_status_idx
    ON edge_adapters (tenant_id, status);

CREATE INDEX IF NOT EXISTS edge_adapters_last_seen_idx
    ON edge_adapters (tenant_id, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS edge_adapter_statuses (
    adapter_id uuid PRIMARY KEY REFERENCES edge_adapters(id) ON DELETE CASCADE,
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    status text NOT NULL,
    observed_at timestamptz NOT NULL,
    uptime_seconds bigint,
    active_connectors integer,
    active_plugins integer,
    dlq_depth bigint,
    dlq_oldest_record_at timestamptz,
    last_publish_success_at timestamptz,
    last_publish_failure_at timestamptz,
    last_error text,
    metadata jsonb,
    CONSTRAINT edge_adapter_statuses_status_valid CHECK (
        status IN ('online', 'offline', 'degraded', 'unknown')
    ),
    CONSTRAINT edge_adapter_statuses_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    ),
    CONSTRAINT edge_adapter_statuses_active_connectors_non_negative CHECK (
        active_connectors IS NULL OR active_connectors >= 0
    ),
    CONSTRAINT edge_adapter_statuses_active_plugins_non_negative CHECK (
        active_plugins IS NULL OR active_plugins >= 0
    ),
    CONSTRAINT edge_adapter_statuses_dlq_depth_non_negative CHECK (
        dlq_depth IS NULL OR dlq_depth >= 0
    )
);

CREATE INDEX IF NOT EXISTS edge_adapter_statuses_observed_at_idx
    ON edge_adapter_statuses (tenant_id, observed_at DESC);

