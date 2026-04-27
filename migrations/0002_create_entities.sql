CREATE TABLE IF NOT EXISTS entities (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    entity_key text NOT NULL,
    entity_type text NOT NULL,
    jsonld jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT entities_entity_key_not_empty CHECK (length(trim(entity_key)) > 0),
    CONSTRAINT entities_entity_type_not_empty CHECK (length(trim(entity_type)) > 0),
    CONSTRAINT entities_jsonld_is_object CHECK (jsonb_typeof(jsonld) = 'object'),
    CONSTRAINT entities_jsonld_has_context CHECK (jsonld ? '@context'),
    CONSTRAINT entities_jsonld_has_type CHECK (jsonld ? '@type'),
    CONSTRAINT entities_jsonld_has_id_or_key CHECK ((jsonld ? '@id') OR length(trim(entity_key)) > 0),
    CONSTRAINT entities_tenant_entity_key_unique UNIQUE (tenant_id, entity_key)
);

CREATE INDEX IF NOT EXISTS entities_tenant_type_idx
    ON entities (tenant_id, entity_type);

CREATE INDEX IF NOT EXISTS entities_jsonld_gin_idx
    ON entities USING gin (jsonld);
