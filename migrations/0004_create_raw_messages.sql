CREATE TABLE IF NOT EXISTS raw_messages (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    source_type text NOT NULL,
    source_ref text,
    device_key text,
    protocol text NOT NULL,
    payload_format text,
    decoder_hint text,
    content_type text,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload bytea NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    normalization_status text NOT NULL DEFAULT 'pending',
    normalization_error text,
    CONSTRAINT raw_messages_source_type_not_empty CHECK (length(trim(source_type)) > 0),
    CONSTRAINT raw_messages_protocol_not_empty CHECK (length(trim(protocol)) > 0),
    CONSTRAINT raw_messages_headers_is_object CHECK (jsonb_typeof(headers) = 'object'),
    CONSTRAINT raw_messages_payload_not_empty CHECK (octet_length(payload) > 0),
    CONSTRAINT raw_messages_normalization_status_valid CHECK (
        normalization_status IN ('pending', 'normalized', 'failed')
    )
);

CREATE INDEX IF NOT EXISTS raw_messages_tenant_received_at_idx
    ON raw_messages (tenant_id, received_at DESC);

CREATE INDEX IF NOT EXISTS raw_messages_status_idx
    ON raw_messages (tenant_id, normalization_status);

CREATE INDEX IF NOT EXISTS raw_messages_device_idx
    ON raw_messages (tenant_id, device_key);
