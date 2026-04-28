ALTER TABLE raw_messages
    ADD COLUMN IF NOT EXISTS producer_entity_id uuid REFERENCES entities(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS feature_of_interest_id uuid REFERENCES entities(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS raw_messages_producer_received_at_idx
    ON raw_messages (tenant_id, producer_entity_id, received_at DESC);

CREATE INDEX IF NOT EXISTS raw_messages_feature_received_at_idx
    ON raw_messages (tenant_id, feature_of_interest_id, received_at DESC);

CREATE INDEX IF NOT EXISTS raw_messages_payload_format_received_at_idx
    ON raw_messages (tenant_id, payload_format, received_at DESC);

CREATE TABLE IF NOT EXISTS payload_profiles (
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    entity_id uuid NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    payload_format text NOT NULL,
    protocol text,
    content_type text,
    attribute_mapping jsonb,
    metadata jsonb,
    CONSTRAINT payload_profiles_pk PRIMARY KEY (tenant_id, entity_id),
    CONSTRAINT payload_profiles_payload_format_not_empty CHECK (length(trim(payload_format)) > 0),
    CONSTRAINT payload_profiles_attribute_mapping_is_object CHECK (
        attribute_mapping IS NULL OR jsonb_typeof(attribute_mapping) = 'object'
    ),
    CONSTRAINT payload_profiles_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS payload_profiles_payload_format_idx
    ON payload_profiles (tenant_id, payload_format);

CREATE TABLE IF NOT EXISTS capabilities (
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    entity_id uuid NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    capability_name text NOT NULL,
    command_type text NOT NULL,
    protocol text,
    metadata jsonb,
    CONSTRAINT capabilities_pk PRIMARY KEY (tenant_id, entity_id, capability_name),
    CONSTRAINT capabilities_capability_name_not_empty CHECK (length(trim(capability_name)) > 0),
    CONSTRAINT capabilities_command_type_not_empty CHECK (length(trim(command_type)) > 0),
    CONSTRAINT capabilities_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS capabilities_command_type_idx
    ON capabilities (tenant_id, command_type);

CREATE TABLE IF NOT EXISTS policies (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    target_entity_id uuid REFERENCES entities(id) ON DELETE CASCADE,
    command_type text,
    requires_approval boolean NOT NULL DEFAULT false,
    auto_execute_allowed boolean NOT NULL DEFAULT false,
    metadata jsonb,
    CONSTRAINT policies_command_type_not_empty CHECK (
        command_type IS NULL OR length(trim(command_type)) > 0
    ),
    CONSTRAINT policies_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS policies_target_command_idx
    ON policies (tenant_id, target_entity_id, command_type);

CREATE TABLE IF NOT EXISTS commands (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    target_entity_id uuid NOT NULL REFERENCES entities(id) ON DELETE RESTRICT,
    command_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    requested_by text,
    reason text,
    claimed_by text,
    claimed_at timestamptz,
    completed_at timestamptz,
    failure_reason text,
    approval_status text,
    policy_decision jsonb,
    retry_count integer NOT NULL DEFAULT 0,
    max_retries integer,
    lease_expires_at timestamptz,
    last_claimed_by text,
    last_failure_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT commands_command_type_not_empty CHECK (length(trim(command_type)) > 0),
    CONSTRAINT commands_status_valid CHECK (
        status IN ('pending', 'claimed', 'executed', 'failed', 'cancelled')
    ),
    CONSTRAINT commands_approval_status_valid CHECK (
        approval_status IS NULL
        OR approval_status IN ('not_required', 'required', 'approved', 'rejected')
    ),
    CONSTRAINT commands_policy_decision_is_object CHECK (
        policy_decision IS NULL OR jsonb_typeof(policy_decision) = 'object'
    ),
    CONSTRAINT commands_retry_count_nonnegative CHECK (retry_count >= 0),
    CONSTRAINT commands_max_retries_nonnegative CHECK (max_retries IS NULL OR max_retries >= 0)
);

CREATE INDEX IF NOT EXISTS commands_target_status_idx
    ON commands (tenant_id, target_entity_id, status);

CREATE INDEX IF NOT EXISTS commands_approval_status_idx
    ON commands (tenant_id, approval_status);

CREATE INDEX IF NOT EXISTS commands_command_type_idx
    ON commands (tenant_id, command_type);

CREATE TABLE IF NOT EXISTS actions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    command_id uuid NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    executor_entity_id uuid REFERENCES entities(id) ON DELETE SET NULL,
    action_type text NOT NULL,
    status text NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    metadata jsonb,
    CONSTRAINT actions_action_type_not_empty CHECK (length(trim(action_type)) > 0),
    CONSTRAINT actions_status_not_empty CHECK (length(trim(status)) > 0),
    CONSTRAINT actions_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS actions_command_idx
    ON actions (tenant_id, command_id);

CREATE TABLE IF NOT EXISTS action_results (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    command_id uuid NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    action_id uuid NOT NULL REFERENCES actions(id) ON DELETE CASCADE,
    status text NOT NULL,
    verified boolean NOT NULL DEFAULT false,
    result_payload jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    metadata jsonb,
    CONSTRAINT action_results_status_not_empty CHECK (length(trim(status)) > 0),
    CONSTRAINT action_results_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS action_results_action_idx
    ON action_results (tenant_id, action_id);

CREATE INDEX IF NOT EXISTS action_results_command_idx
    ON action_results (tenant_id, command_id);

CREATE TABLE IF NOT EXISTS events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    event_type text NOT NULL,
    severity text NOT NULL,
    source_entity_id uuid REFERENCES entities(id) ON DELETE SET NULL,
    target_entity_id uuid REFERENCES entities(id) ON DELETE SET NULL,
    message text,
    occurred_at timestamptz NOT NULL,
    observed_at timestamptz,
    correlation_id text,
    raw_message_id uuid REFERENCES raw_messages(id) ON DELETE SET NULL,
    observation_id uuid,
    command_id uuid REFERENCES commands(id) ON DELETE SET NULL,
    action_id uuid REFERENCES actions(id) ON DELETE SET NULL,
    action_result_id uuid REFERENCES action_results(id) ON DELETE SET NULL,
    metadata jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT events_event_type_not_empty CHECK (length(trim(event_type)) > 0),
    CONSTRAINT events_severity_valid CHECK (
        severity IN ('debug', 'info', 'warning', 'error', 'critical')
    ),
    CONSTRAINT events_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS events_target_idx
    ON events (tenant_id, target_entity_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS events_source_idx
    ON events (tenant_id, source_entity_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS events_type_idx
    ON events (tenant_id, event_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS events_severity_idx
    ON events (tenant_id, severity, occurred_at DESC);

CREATE INDEX IF NOT EXISTS events_command_idx
    ON events (tenant_id, command_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS events_raw_message_idx
    ON events (tenant_id, raw_message_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS events_correlation_idx
    ON events (tenant_id, correlation_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS executor_agents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    agent_key text NOT NULL,
    agent_type text NOT NULL,
    display_name text,
    status text NOT NULL DEFAULT 'offline',
    last_seen_at timestamptz,
    metadata jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT executor_agents_agent_key_not_empty CHECK (length(trim(agent_key)) > 0),
    CONSTRAINT executor_agents_agent_type_not_empty CHECK (length(trim(agent_type)) > 0),
    CONSTRAINT executor_agents_status_valid CHECK (status IN ('online', 'offline', 'degraded')),
    CONSTRAINT executor_agents_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    ),
    CONSTRAINT executor_agents_tenant_agent_key_unique UNIQUE (tenant_id, agent_key)
);

CREATE INDEX IF NOT EXISTS executor_agents_agent_key_idx
    ON executor_agents (tenant_id, agent_key);

CREATE INDEX IF NOT EXISTS executor_agents_status_idx
    ON executor_agents (tenant_id, status);

CREATE TABLE IF NOT EXISTS executor_capabilities (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    agent_id uuid NOT NULL REFERENCES executor_agents(id) ON DELETE CASCADE,
    command_type text NOT NULL,
    protocol text,
    metadata jsonb,
    CONSTRAINT executor_capabilities_command_type_not_empty CHECK (length(trim(command_type)) > 0),
    CONSTRAINT executor_capabilities_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS executor_capabilities_agent_command_idx
    ON executor_capabilities (tenant_id, agent_id, command_type);

CREATE TABLE IF NOT EXISTS executor_scopes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    agent_id uuid NOT NULL REFERENCES executor_agents(id) ON DELETE CASCADE,
    target_entity_id uuid REFERENCES entities(id) ON DELETE CASCADE,
    entity_type text,
    relationship_type text,
    metadata jsonb,
    CONSTRAINT executor_scopes_entity_type_not_empty CHECK (
        entity_type IS NULL OR length(trim(entity_type)) > 0
    ),
    CONSTRAINT executor_scopes_relationship_type_not_empty CHECK (
        relationship_type IS NULL OR length(trim(relationship_type)) > 0
    ),
    CONSTRAINT executor_scopes_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS executor_scopes_agent_idx
    ON executor_scopes (tenant_id, agent_id);

CREATE INDEX IF NOT EXISTS executor_scopes_target_idx
    ON executor_scopes (tenant_id, target_entity_id);

CREATE INDEX IF NOT EXISTS executor_scopes_entity_type_idx
    ON executor_scopes (tenant_id, entity_type);

CREATE INDEX IF NOT EXISTS executor_scopes_relationship_type_idx
    ON executor_scopes (tenant_id, relationship_type);

CREATE TABLE IF NOT EXISTS command_leases (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    command_id uuid NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    executor_id uuid NOT NULL REFERENCES executor_agents(id) ON DELETE CASCADE,
    lease_status text NOT NULL DEFAULT 'active',
    claimed_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    released_at timestamptz,
    completed_at timestamptz,
    metadata jsonb,
    CONSTRAINT command_leases_status_valid CHECK (
        lease_status IN ('active', 'expired', 'released', 'completed', 'failed')
    ),
    CONSTRAINT command_leases_expires_after_claimed CHECK (expires_at > claimed_at),
    CONSTRAINT command_leases_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS command_leases_command_idx
    ON command_leases (tenant_id, command_id, claimed_at DESC);

CREATE INDEX IF NOT EXISTS command_leases_executor_idx
    ON command_leases (tenant_id, executor_id, claimed_at DESC);

CREATE INDEX IF NOT EXISTS command_leases_status_expires_idx
    ON command_leases (tenant_id, lease_status, expires_at);

CREATE TABLE IF NOT EXISTS rules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    name text NOT NULL,
    description text,
    enabled boolean NOT NULL DEFAULT true,
    trigger_type text NOT NULL,
    target_entity_id uuid REFERENCES entities(id) ON DELETE CASCADE,
    observed_property text,
    event_type text,
    condition jsonb NOT NULL,
    action jsonb NOT NULL,
    metadata jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT rules_name_not_empty CHECK (length(trim(name)) > 0),
    CONSTRAINT rules_trigger_type_valid CHECK (
        trigger_type IN ('observation_created', 'event_created', 'manual')
    ),
    CONSTRAINT rules_condition_is_object CHECK (jsonb_typeof(condition) = 'object'),
    CONSTRAINT rules_action_is_object CHECK (jsonb_typeof(action) = 'object'),
    CONSTRAINT rules_metadata_is_object CHECK (
        metadata IS NULL OR jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX IF NOT EXISTS rules_enabled_trigger_idx
    ON rules (tenant_id, enabled, trigger_type);

CREATE INDEX IF NOT EXISTS rules_observed_property_idx
    ON rules (tenant_id, observed_property);

CREATE INDEX IF NOT EXISTS rules_event_type_idx
    ON rules (tenant_id, event_type);
