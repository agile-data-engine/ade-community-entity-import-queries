/* Mapping import from SRC entities to STAGE entities */

SET DEFINE ON;

DEFINE target_entity_schema = 'staging';
/* Original schema from Oracle */
DEFINE original_table_schema = 'ADMIN';
DEFINE entity_source_system = 'ORCL';

SELECT DISTINCT
    t.owner AS "source_entity_schema",
    t.table_name AS "source_entity_name",
    '&target_entity_schema' as "target_entity_schema",
    'STG_' || '&entity_source_system' || '_' || t.table_name AS "target_entity_name",
    'load_stg_' || lower('&entity_source_system') || '_' || lower(t.table_name) || '_from_cloud_storage' AS "target_entity_load_name",
    lower(t.table_name) || '_to_stg_' || lower('&entity_source_system') || '_' || lower(t.table_name) AS "entity_mapping_name",
    lower(t.column_name) AS "target_attribute_name",
    lower(t.column_name) AS "source_attribute_names",
    'LOAD_FILE' AS "target_entity_load_type",
    'ORCL_DEMO' AS "target_entity_load_schedule_name"
FROM all_tab_columns t
WHERE t.owner = '&original_table_schema';