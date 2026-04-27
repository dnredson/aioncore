CREATE TABLE IF NOT EXISTS entity_relationships (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    source_entity_id uuid NOT NULL REFERENCES entities(id) ON DELETE RESTRICT,
    relationship_type text NOT NULL,
    target_entity_id uuid NOT NULL REFERENCES entities(id) ON DELETE RESTRICT,
    jsonld jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT entity_relationships_type_not_empty CHECK (length(trim(relationship_type)) > 0),
    CONSTRAINT entity_relationships_no_self_relationship CHECK (source_entity_id <> target_entity_id),
    CONSTRAINT entity_relationships_jsonld_is_object CHECK (jsonb_typeof(jsonld) = 'object')
);

CREATE INDEX IF NOT EXISTS entity_relationships_source_idx
    ON entity_relationships (tenant_id, source_entity_id);

CREATE INDEX IF NOT EXISTS entity_relationships_target_idx
    ON entity_relationships (tenant_id, target_entity_id);

CREATE INDEX IF NOT EXISTS entity_relationships_type_idx
    ON entity_relationships (tenant_id, relationship_type);
