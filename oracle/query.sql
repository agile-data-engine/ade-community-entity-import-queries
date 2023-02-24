/* See possible values from the documentation: https://ade.document360.io/docs/entity-import#columns */

/* Using ADE data types directly when using Oracle */
DEFINE database_dbms_product = 'AGILE_DATA_ENGINE';
DEFINE package_name = 'PACKAGE_NAME';
DEFINE entity_physical_type = 'METADATA_ONLY';
/* Use if you want to define different schema than source schema */
--DEFINE entity_schema = 'SRC';
DEFINE entity_type = 'SOURCE';
/* Optional. Useful to have when entity type is SOURCE */ 
--DEFINE entity_source_system = 'SYSTEM';
/* Original schema from Oracle */
DEFINE original_table_schema = 'ADMIN';

SELECT 
    '&database_dbms_product' AS "datatype_dbms_product",
    /* Optional. Useful when entity type is SOURCE */
    --'&entity_source_system' as "entity_dv_source",
    '&package_name' AS "package_name",
    '&entity_physical_type' AS "entity_physical_type",
    /* Can be set as static or the same as in source. When references used, use source schema. */
    --'&entity_schema' AS "entity_schema",
    t.owner AS "entity_schema",
    t.table_name AS "entity_name",
    '&entity_type' AS "entity_type",
    t.column_name AS "attribute_name",
    UPPER(
        CASE
            WHEN t.data_type = 'char' THEN 'char'
            WHEN t.data_type LIKE 'VARCHAR%' THEN 'varchar'
            when t.data_type LIKE 'NUMBER' and DATA_SCALE = 0 then 'INTEGER8'
            when t.data_type LIKE 'TIMESTAMP(6) WITH TIME ZONE' then 'timestamptz'
            when t.data_type LIKE 'TIMESTAMP(6)' then 'timestamp'
            WHEN t.data_type LIKE 'NUMBER' and DATA_SCALE > 0 THEN 'DECIMAL'
            WHEN t.data_type LIKE 'NUMBER' and DATA_SCALE is null THEN 'INTEGER8'
            /* Oracle DATE datatype can contain time information, so it is safer to change that to timestamp. 
               In case you are certain date can be used, then this is not necessary change */
            WHEN t.data_type LIKE 'DATE' THEN 'timestamp'
            WHEN t.data_type LIKE 'INTERVAL%' then 'varchar'
            ELSE t.data_type
        END
    ) AS "attribute_datatype",
    CASE 
        WHEN t.data_type = 'CHAR' THEN t.data_length
        WHEN t.data_type LIKE 'VARCHAR%' THEN t.data_length
        WHEN t.data_type LIKE 'INTERVAL%' THEN 100
        ELSE NULL
        END 
    AS "attribute_length",
    CASE 
        WHEN t.data_type = 'NUMBER' AND data_scale > 1 
            THEN t.data_precision 
        ELSE NULL 
        END 
    AS "attribute_precision",
    CASE 
        WHEN t.data_type = 'NUMBER' AND data_scale > 1 
        THEN t.data_scale 
        ELSE NULL 
        END 
    AS "attribute_scale",
    CASE WHEN t.nullable = 'Y' THEN 1 ELSE 0 END "attribute_nullable",
    t.column_id as "attribute_position",
    MAX(CASE WHEN ac.constraint_type = 'P' AND t.column_name = tc.column_name THEN UPPER(tc.constraint_name) ELSE '' END) AS "primary_key_name",
    MAX(CASE WHEN ac.constraint_type = 'P' AND t.column_name = tc.column_name THEN UPPER(tc.position) ELSE '' END) AS "primary_key_position",
    MAX(CASE WHEN ac.constraint_type = 'R' AND t.column_name = tc.column_name THEN UPPER(tc.constraint_name) ELSE '' END) AS "foreign_key_name",
    MAX(CASE WHEN ac.constraint_type = 'R' AND t.column_name = tc.column_name THEN UPPER(ac_fk.owner) ELSE '' END) as "foreign_key_parent_entity_schema",
    MAX(CASE WHEN ac.constraint_type = 'R' AND t.column_name = tc.column_name THEN UPPER(ac_fk.table_name) ELSE '' END) as "foreign_key_parent_entity_name",
    MAX(CASE WHEN ac.constraint_type = 'R' AND t.column_name = tc.column_name THEN UPPER(tc_b.column_name) ELSE '' END) as "foreign_key_parent_entity_attribute_name",
    MAX(CASE WHEN ac.constraint_type = 'R' AND t.column_name = tc.column_name THEN UPPER(tc_b.position) ELSE '' END) as "foreign_key_position"
FROM all_tab_columns t
LEFT JOIN 
    all_cons_columns tc ON t.table_name = tc.table_name
LEFT JOIN 
    all_constraints ac on tc.constraint_name = ac.constraint_name and tc.owner = ac.owner
LEFT JOIN 
    all_constraints ac_fk on ac.r_owner = ac_fk.owner and ac.r_constraint_name = ac_fk.constraint_name
LEFT JOIN 
    all_cons_columns tc_b ON ac_fk.constraint_name = tc_b.constraint_name
WHERE t.owner = 'ADMIN'
GROUP BY 
    t.table_name,
    t.owner,
    t.column_name,
    t.data_type,
    t.data_length,
    t.data_precision,
    t.data_scale,
    CASE WHEN t.nullable = 'Y' THEN 1 ELSE 0 END,
    t.column_id
ORDER BY 
    "entity_name", 
    "attribute_position";