CREATE TABLE IF NOT EXISTS dlq_records (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    dlq_key TEXT,
    source_system TEXT,
    source_id TEXT,
    connector_id UUID REFERENCES ingestion_connectors(id) ON DELETE SET NULL,
    flow_id UUID REFERENCES flows(id) ON DELETE SET NULL,
    raw_message_id UUID REFERENCES raw_messages(id) ON DELETE SET NULL,
    event_id UUID REFERENCES events(id) ON DELETE SET NULL,
    command_id UUID REFERENCES commands(id) ON DELETE SET NULL,
    idempotency_key TEXT,
    external_flow_id TEXT,
    external_flow_name TEXT,
    external_flowfile_uuid TEXT,
    external_process_group_id TEXT,
    external_processor_id TEXT,
    external_provenance_uri TEXT,
    sync_session_id TEXT,
    payload_format TEXT,
    payload JSONB,
    payload_hash TEXT,
    failure_stage TEXT NOT NULL CHECK (
        failure_stage IN (
            'ingestion',
            'decoding',
            'validation',
            'mapping',
            'rule_evaluation',
            'flow_processing',
            'sink_delivery',
            'command_creation',
            'unknown'
        )
    ),
    failure_reason TEXT NOT NULL,
    failure_detail TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    replay_count INTEGER NOT NULL DEFAULT 0 CHECK (replay_count >= 0),
    status TEXT NOT NULL CHECK (
        status IN (
            'pending',
            'inspecting',
            'resolved',
            'ignored',
            'replay_requested',
            'failed_replay'
        )
    ),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dlq_records_tenant_created_at
    ON dlq_records (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_status
    ON dlq_records (tenant_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_failure_stage
    ON dlq_records (tenant_id, failure_stage, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_source_system
    ON dlq_records (tenant_id, source_system, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_connector
    ON dlq_records (tenant_id, connector_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_flow
    ON dlq_records (tenant_id, flow_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_raw_message
    ON dlq_records (tenant_id, raw_message_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_records_idempotency_key
    ON dlq_records (tenant_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_dlq_records_external_flowfile_uuid
    ON dlq_records (tenant_id, external_flowfile_uuid);
CREATE INDEX IF NOT EXISTS idx_dlq_records_sync_session_id
    ON dlq_records (tenant_id, sync_session_id);
