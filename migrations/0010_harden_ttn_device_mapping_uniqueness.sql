DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'ttn_device_mappings'::regclass
          AND contype = 'u'
    LOOP
        EXECUTE format(
            'ALTER TABLE ttn_device_mappings DROP CONSTRAINT IF EXISTS %I',
            constraint_name
        );
    END LOOP;
END $$;

DROP INDEX IF EXISTS idx_ttn_device_mappings_unique_no_application;

CREATE UNIQUE INDEX IF NOT EXISTS idx_ttn_device_mappings_unique_enabled_application
    ON ttn_device_mappings (tenant_id, connector_id, ttn_application_id, ttn_device_id)
    WHERE enabled = TRUE AND ttn_application_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_ttn_device_mappings_unique_enabled_no_application
    ON ttn_device_mappings (tenant_id, connector_id, ttn_device_id)
    WHERE enabled = TRUE AND ttn_application_id IS NULL;
