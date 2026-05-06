ALTER TABLE raw_messages
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE INDEX IF NOT EXISTS raw_messages_idempotency_lookup_idx
    ON raw_messages (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS raw_messages_tenant_idempotency_unique_idx
    ON raw_messages (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
