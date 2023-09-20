/* Entity import for DW-entities */

SET DEFINE ON;

/* See possible values from the documentation: https://docs.agiledataengine.com/docs/import-entities#Importentities-Columns */

/* Using ADE data types directly when using Oracle */
DEFINE database_dbms_product = 'AGILE_DATA_ENGINE';
DEFINE package_name = 'DW_ORACLE_DEMO_SF';
DEFINE entity_physical_type = 'TABLE';
DEFINE entity_type = 'DW';
/* Original schema from Oracle */
DEFINE original_table_schema = 'ADMIN';
/* Target entity schema */
DEFINE entity_schema = 'edw';
/* Source system in ADE, defined in CONFIG_SYSTEMS https://docs.agiledataengine.com/docs/config_systems */
DEFINE entity_source_system = 'ORCL';


WITH attributes as (
    SELECT DISTINCT
        'DW_' || '&entity_source_system' || '_' || t.table_name AS entity_name,
        t.column_name AS attribute_name,
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
        ) AS attribute_datatype,
        CASE 
            WHEN t.data_type = 'CHAR' THEN t.data_length
            WHEN t.data_type LIKE 'VARCHAR%' THEN t.data_length
            WHEN t.data_type LIKE 'INTERVAL%' THEN 100
            ELSE NULL
            END 
        AS attribute_length,
        CASE 
            WHEN t.data_type = 'NUMBER' AND data_scale > 1 
                THEN t.data_precision 
            ELSE NULL 
            END 
        AS attribute_precision,
        CASE 
            WHEN t.data_type = 'NUMBER' AND data_scale > 1 
            THEN t.data_scale 
            ELSE NULL 
            END 
        AS attribute_scale,
        CASE WHEN t.nullable = 'Y' THEN 1 ELSE 0 END attribute_nullable,
        null as technical_attribute_type,
        null as attribute_description,
        t.column_id as attribute_position
    FROM all_tab_columns t
    WHERE t.owner = '&original_table_schema'
    ORDER BY entity_name, attribute_position
),
meta_attributes as (
    /* Default metadata attributes defined for DW entity type.
       See documentation: https://docs.agiledataengine.com/docs/config_entity_defaults
       And examples: https://docs.agiledataengine.com/docs/persistent-staging#Persistentstaging-Configuringdefaultsettingsforapersistentstagingarea */
    SELECT
        'dw_id' AS attribute_name,
        'CHAR' as attribute_datatype,
        32 as attribute_length,
        null as attribute_precision,
        null as attribute_scale,
        0 as attribute_nullable,
        'DV_HASHKEY' as technical_attribute_type,
        'Hash key' as attribute_description,
        -11 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'dw_business_key' AS "attribute_name",
        'VARCHAR' as "attribute_datatype",
        200 as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'BUSINESS_KEY' as technical_attribute_type,
        'Business key' as attribute_description,
        -10 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'dw_hash' AS "attribute_name",
        'CHAR' as "attribute_datatype",
        32 as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'DV_DATAHASH' as technical_attribute_type,
        'Comparison hash' as attribute_description,
        -9 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'dw_run_id' AS "attribute_name",
        'INTEGER8' as "attribute_datatype",
        null as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'RUN_ID' as technical_attribute_type,
        'ADE run id' as attribute_description,
        -8 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'meta_insert_time' AS "attribute_name",
        'TIMESTAMP' as "attribute_datatype",
        null as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'DV_LOAD_TIME' as technical_attribute_type,
        'Insert timestamp' as attribute_description,
        -7 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'meta_update_time' AS "attribute_name",
        'TIMESTAMP' as "attribute_datatype",
        null as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'DV_UPDATE_TIME' as technical_attribute_type,
        'Update timestamp' as attribute_description,
        -6 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'meta_update_time' AS "attribute_name",
        'VARCHAR' as "attribute_datatype",
        100 as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'META_LOAD_NAME' as technical_attribute_type,
        'Load name' as attribute_description,
        -5 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'meta_package_version' AS "attribute_name",
        'INTEGER8' as "attribute_datatype",
        null as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'META_PACKAGE_VERSION' as technical_attribute_type,
        'ADE Package version' as attribute_description,
        -4 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'meta_source_system' AS "attribute_name",
        'VARCHAR' as "attribute_datatype",
        50 as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'META_SOURCE_SYSTEM' as technical_attribute_type,
        'Source system name' as attribute_description,
        -3 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'meta_source_entity' AS "attribute_name",
        'VARCHAR' as "attribute_datatype",
        100 as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'META_SOURCE_ENTITY' as technical_attribute_type,
        'Source entity name' as attribute_description,
        -2 as attribute_position
    FROM DUAL
    UNION ALL
    SELECT
        'meta_file_name' AS "attribute_name",
        'VARCHAR' as "attribute_datatype",
        100 as "attribute_length",
        null as "attribute_precision",
        null as attribute_scale,
        0 as attribute_nullable,
        'META_FILE_NAME' as technical_attribute_type,
        'Source file name' as attribute_description,
        -1 as attribute_position
    FROM DUAL
),
entity_meta as (
    SELECT 
        et.entity_name,
        met.*
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
SELECT 
    '&database_dbms_product' AS "datatype_dbms_product",
    '&package_name' AS "package_name",
    '&entity_physical_type' AS "entity_physical_type",
    '&entity_schema' AS "entity_schema",
    '&entity_type' AS "entity_type",
    at.entity_name AS "entity_name",
    at.attribute_name AS "attribute_name",
    at.attribute_datatype AS "attribute_datatype",
    at.attribute_length AS "attribute_length",
    at.attribute_precision AS "attribute_precision",
    at.attribute_scale AS "attribute_scale",
    ROW_NUMBER() OVER(PARTITION BY at.entity_name ORDER BY at.attribute_position) as "attribute_position",
    at.attribute_nullable as "attribute_nullable",
    at.technical_attribute_type as "technical_attribute_type",
    at.attribute_description as "attribute_description"
FROM DUAL
CROSS JOIN combined at
ORDER BY at.entity_name, at.attribute_position;