CREATE TABLE IF NOT EXISTS api_tokens (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    token_name TEXT NOT NULL,
    token_prefix TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    principal_id TEXT,
    scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT api_tokens_tenant_prefix_unique UNIQUE (tenant_id, token_prefix)
);

CREATE INDEX IF NOT EXISTS idx_api_tokens_tenant_created_at
    ON api_tokens (tenant_id, created_at DESC);
