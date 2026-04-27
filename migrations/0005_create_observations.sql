CREATE TABLE IF NOT EXISTS observations (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    producer_entity_id uuid NOT NULL REFERENCES entities(id) ON DELETE RESTRICT,
    feature_of_interest_id uuid NOT NULL REFERENCES entities(id) ON DELETE RESTRICT,
    observed_property text NOT NULL,
    value_number double precision,
    value_string text,
    value_bool boolean,
    value_json jsonb,
    unit text,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    protocol text NOT NULL,
    payload_format text NOT NULL,
    raw_message_id uuid REFERENCES raw_messages(id) ON DELETE SET NULL,
    quality jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT observations_pk PRIMARY KEY (observed_at, id),
    CONSTRAINT observations_observed_property_not_empty CHECK (length(trim(observed_property)) > 0),
    CONSTRAINT observations_protocol_not_empty CHECK (length(trim(protocol)) > 0),
    CONSTRAINT observations_payload_format_not_empty CHECK (length(trim(payload_format)) > 0),
    CONSTRAINT observations_quality_is_object CHECK (jsonb_typeof(quality) = 'object'),
    CONSTRAINT observations_metadata_is_object CHECK (jsonb_typeof(metadata) = 'object'),
    CONSTRAINT observations_exactly_one_value CHECK (
        num_nonnulls(value_number, value_string, value_bool, value_json) = 1
    )
);

SELECT create_hypertable('observations', 'observed_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS observations_tenant_feature_time_idx
    ON observations (tenant_id, feature_of_interest_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS observations_tenant_producer_time_idx
    ON observations (tenant_id, producer_entity_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS observations_tenant_property_time_idx
    ON observations (tenant_id, observed_property, observed_at DESC);

CREATE INDEX IF NOT EXISTS observations_raw_message_idx
    ON observations (raw_message_id);
