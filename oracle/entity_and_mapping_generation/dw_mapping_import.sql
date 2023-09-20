/* Mapping import from STAGE entities to DW entities */

SET DEFINE ON;

DEFINE source_entity_schema = 'staging';
DEFINE target_entity_schema = 'edw';
DEFINE entity_source_system = 'ORCL';
/* Original schema from Oracle */
DEFINE original_table_schema = 'ADMIN';


WITH attributes as (
    SELECT DISTINCT
        t.table_name AS entity_name,
        t.column_name AS attribute_name,
        MAX(CASE WHEN ac.constraint_type = 'P' AND t.column_name = tc.column_name THEN UPPER(tc.constraint_name) ELSE '' END) AS primary_key_name
    FROM all_tab_columns t
    LEFT JOIN 
        all_cons_columns tc ON t.table_name = tc.table_name
    LEFT JOIN 
        all_constraints ac on tc.constraint_name = ac.constraint_name and tc.owner = ac.owner
    LEFT JOIN 
        all_constraints ac_fk on ac.r_owner = ac_fk.owner and ac.r_constraint_name = ac_fk.constraint_name
    LEFT JOIN 
        all_cons_columns tc_b ON ac_fk.constraint_name = tc_b.constraint_name
    WHERE t.owner = '&original_table_schema'
    GROUP BY 
        t.table_name,
        t.column_name
    ORDER BY entity_name
),
primary_keys as (
    SELECT 
        entity_name,
        lower('"' || LISTAGG(attribute_name, ',') || '"') as primary_key
    FROM attributes
    WHERE primary_key_name IS NOT NULL
    GROUP BY entity_name
),
attribute_list as (
    SELECT 
        entity_name,
        lower('"' || LISTAGG(attribute_name, ',') || '"') as attribute_list
    FROM attributes
    GROUP BY entity_name
),
meta_attributes as (
    SELECT
        'dw_id' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'dw_business_key' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'dw_hash' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'dw_run_id' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'meta_insert_time' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'meta_update_time' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'meta_load_name' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'meta_package_version' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'meta_source_system' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'meta_source_entity' AS attribute_name
    FROM DUAL
    UNION ALL
    SELECT
        'meta_file_name' AS attribute_name
    FROM DUAL
),
entity_meta as (
    SELECT 
        et.entity_name,
        met.attribute_name,
        null as primary_key_name
    FROM meta_attributes met
    CROSS JOIN (SELECT DISTINCT entity_name from attributes) et
),
combined as (
    SELECT *
    FROM attributes
    UNION ALL
    SELECT * 
    FROM entity_meta
)
SELECT DISTINCT
    '&source_entity_schema' AS "source_entity_schema",
    'STG_' ||  t.entity_name AS "source_entity_name",
    '&target_entity_schema' as "target_entity_schema",
    'DW_' || '&entity_source_system' || '_' || t.entity_name AS "target_entity_name",
    'load_dw_' || lower('&entity_source_system') || '_' || lower(t.entity_name) || '_from_stg_' || lower('&entity_source_system') || '_' ||  lower(t.entity_name) AS "target_entity_load_name",
    'stg_' || lower('&entity_source_system') || '_' ||  lower(t.entity_name) || '_to_dw_' || lower('&entity_source_system') || '_' || lower(t.entity_name) AS "entity_mapping_name",
    lower(t.attribute_name) AS "target_attribute_name",
    CASE 
        WHEN pk.entity_name IS NOT NULL AND t.attribute_name = 'dw_id' THEN pk.primary_key
        WHEN pk.entity_name IS NOT NULL AND t.attribute_name = 'dw_business_key' THEN pk.primary_key
        WHEN al.entity_name IS NOT NULL AND t.attribute_name = 'dw_hash' THEN al.attribute_list
        WHEN t.attribute_name = 'dw_run_id' THEN null
        WHEN t.attribute_name = 'meta_insert_time' THEN null
        WHEN t.attribute_name = 'meta_update_time' THEN null
        WHEN t.attribute_name = 'meta_load_name' THEN null
        WHEN t.attribute_name = 'meta_package_version' THEN null
        WHEN t.attribute_name = 'meta_source_system' THEN 'stg_source_system'
        WHEN t.attribute_name = 'meta_source_entity' THEN 'stg_source_entity'
        WHEN t.attribute_name = 'meta_file_name' THEN 'stg_file_name'
        ELSE lower(t.attribute_name)
        END
    AS "source_attribute_names",
    'TRANSFORM_PERSIST' AS "target_entity_load_type",
    CASE 
        WHEN t.attribute_name = 'dw_id' THEN 'HASH_KEY'
        WHEN t.attribute_name = 'dw_business_key' THEN 'BUSINESS_KEY'
        WHEN t.attribute_name = 'dw_hash' THEN 'COMPARISON_HASH'
        WHEN t.attribute_name = 'dw_run_id' THEN 'RUN_ID'
        WHEN t.attribute_name = 'meta_insert_time' THEN 'CURRENT_TS'
        WHEN t.attribute_name = 'meta_update_time' THEN 'CURRENT_TS'
        WHEN t.attribute_name = 'meta_load_name' THEN 'LOAD_NAME'
        WHEN t.attribute_name = 'meta_package_version' THEN 'PACKAGE_VERSION'
        WHEN t.attribute_name = 'meta_source_system' THEN null
        WHEN t.attribute_name = 'meta_source_entity' THEN null
        WHEN t.attribute_name = 'meta_file_name' THEN null
        END
     as "transformation_type"
FROM combined t
left join primary_keys pk on t.entity_name = pk.entity_name
left join attribute_list al on t.entity_name = al.entity_name;