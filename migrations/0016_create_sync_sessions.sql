CREATE TABLE IF NOT EXISTS sync_sessions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    sync_session_id TEXT NOT NULL,
    source_system TEXT,
    source_id TEXT,
    connector_id UUID,
    edge_adapter_id UUID,
    status TEXT NOT NULL,
    connectivity_state TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_batch_id TEXT,
    expected_items BIGINT,
    received_items BIGINT NOT NULL DEFAULT 0,
    accepted_count BIGINT NOT NULL DEFAULT 0,
    duplicate_count BIGINT NOT NULL DEFAULT 0,
    failed_count BIGINT NOT NULL DEFAULT 0,
    observations_created BIGINT NOT NULL DEFAULT 0,
    first_observed_at TIMESTAMPTZ,
    last_observed_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT sync_sessions_status_check CHECK (status IN ('open', 'receiving', 'completed', 'failed', 'abandoned'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_sessions_tenant_external_id
    ON sync_sessions (tenant_id, sync_session_id);
CREATE INDEX IF NOT EXISTS idx_sync_sessions_tenant_status
    ON sync_sessions (tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_sync_sessions_source_system
    ON sync_sessions (source_system);
CREATE INDEX IF NOT EXISTS idx_sync_sessions_source_id
    ON sync_sessions (source_id);
CREATE INDEX IF NOT EXISTS idx_sync_sessions_connector_id
    ON sync_sessions (connector_id);
CREATE INDEX IF NOT EXISTS idx_sync_sessions_last_seen_at
    ON sync_sessions (last_seen_at);
