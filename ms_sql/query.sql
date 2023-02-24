/* See possible values from the documentation: https://ade.document360.io/docs/entity-import#columns */

DECLARE @database_dbms_product VARCHAR(20) = 'MS_SQL';
DECLARE @package_name VARCHAR(20) = 'PACKAGE_NAME';
DECLARE @entity_physical_type VARCHAR(20) = 'METADATA_ONLY';
DECLARE @entity_schema VARCHAR(10) = 'SRC';
DECLARE @entity_type VARCHAR(20) = 'SOURCE';
/* Optional. Useful to have when entity type is SOURCE. Source system must already exist in ADE. */ 
--DECLARE @entity_source_system VARCHAR(20) = 'SYSTEM';
/* Original schema from SQL DB */
DECLARE @original_table_schema VARCHAR(10) = 'Sales';
  
SELECT
    DISTINCT
    @database_dbms_product AS "datatype_dbms_product",
    /* Optional. Useful to have when entity type is SOURCE */
    --@entity_source_system as "entity_dv_source",
    @package_name AS "package_name", 
    @entity_physical_type AS "entity_physical_type",
    /* You can also use schema from source database by supplying    
       t.table_schema AS "entity_schema", */
    @entity_schema AS "entity_schema",
    UPPER(t.table_name) AS "entity_name",
    @entity_type AS "entity_type", 
    c.column_name AS "attribute_name", 
    UPPER(
        CASE
            WHEN c.data_type = 'bit' THEN 'BOOLEAN'
            WHEN c.data_type IN ('money', 'numeric', 'smallmoney') THEN 'DECIMAL'
            WHEN c.data_type = 'float' THEN 'DOUBLE'
            WHEN c.data_type = 'tinyint' THEN 'SMALLINT'
            WHEN c.data_type IN ('int4', 'smallint', 'smallserial') THEN 'INT'
            WHEN c.data_type IN ('bigint', 'bigserial', 'int', 'int8', 'integer', 'serial') THEN 'BIGINT'
            WHEN c.data_type IN ('datetime', 'datetime2') THEN 'TIMESTAMP'
            WHEN c.data_type IN ('text', 'uniqueidentifier', 'uuid', 'varbinary', 'xml', 'nvarchar', 'hierarchyid') THEN 'VARCHAR'
            WHEN c.data_type IN ('nchar', 'char') THEN 'CHAR'
            ELSE c.data_type
        END
      ) AS "attribute_datatype", 
    CASE
        WHEN c.data_type = 'uniqueidentifier' THEN '36'
        WHEN c.data_type = 'geography' THEN ''
        WHEN CONVERT(varchar(30), c.character_maximum_length) = '-1' THEN '4000' 
        ELSE COALESCE(CONVERT(varchar(30), c.character_maximum_length), '') END AS "attribute_length",
    CASE WHEN c.data_type IN ('smallint', 'tinyint', 'float', 'smallserial', 'bigint', 'bigserial', 'int', 'int4', 'int8', 'integer', 'serial') 
        THEN '' ELSE COALESCE(CONVERT(varchar(30), c.numeric_precision), '') END AS "attribute_precision",
    CASE WHEN c.data_type IN ('smallint', 'tinyint', 'float', 'smallserial', 'bigint', 'bigserial', 'int', 'int4', 'int8', 'integer', 'serial') 
        THEN '' ELSE COALESCE(CONVERT(varchar(30), c.numeric_scale), '') END AS "attribute_scale",
    CASE WHEN c.is_nullable = 'YES' THEN 1 ELSE 0 END "attribute_nullable",
    c.ordinal_position AS "attribute_position",
    /* Optional columns */
    CASE WHEN tc.constraint_type = 'PRIMARY KEY' AND c.column_name = kcu.column_name THEN UPPER(tc.constraint_name) ELSE '' END AS "primary_key_name",
    CASE WHEN tc.constraint_type = 'PRIMARY KEY' AND c.column_name = kcu.column_name THEN CAST(kcu.ordinal_position AS VARCHAR(3)) ELSE '' END AS "primary_key_position",
    MAX(CASE WHEN tc_fk.constraint_type = 'FOREIGN KEY' AND c.column_name = kcu_fk.column_name THEN UPPER(tc_fk.constraint_name) ELSE '' END) AS "foreign_key_name",
    MAX(CASE WHEN tc_fk.constraint_type = 'FOREIGN KEY' AND c.column_name = kcu_fk.column_name THEN tc_fk_pk.table_schema ELSE '' END) AS "foreign_key_parent_entity_schema",
    MAX(CASE WHEN tc_fk.constraint_type = 'FOREIGN KEY' AND c.column_name = kcu_fk.column_name THEN UPPER(tc_fk_pk.table_name) ELSE '' END) AS "foreign_key_parent_entity_name",
    MAX(CASE WHEN tc_fk.constraint_type = 'FOREIGN KEY' AND c.column_name = kcu_fk.column_name AND kcu_fk.ordinal_position = kcu_fk_pk.ordinal_position THEN kcu_fk_pk.column_name ELSE '' END) AS "foreign_key_parent_entity_attribute_name",
    MAX(CASE WHEN tc_fk.constraint_type = 'FOREIGN KEY' AND c.column_name = kcu_fk.column_name THEN CAST(kcu_fk.ordinal_position AS VARCHAR(3)) ELSE '' END) AS "foreign_key_position"
FROM
    information_schema.tables t
JOIN
    information_schema.columns c ON (t.table_catalog = c.table_catalog AND t.table_schema = c.table_schema AND t.table_name = c.table_name)
LEFT JOIN
    information_schema.table_constraints AS tc ON (t.table_catalog = tc.table_catalog AND t.table_schema = tc.table_schema AND t.table_name = tc.table_name AND tc.constraint_type = 'PRIMARY KEY')
LEFT JOIN
    information_schema.key_column_usage AS kcu ON (tc.table_catalog = kcu.table_catalog AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name AND tc.constraint_catalog = kcu.constraint_catalog AND tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = kcu.constraint_name AND c.column_name = kcu.column_name)
LEFT JOIN
    information_schema.table_constraints tc_fk ON (t.table_catalog = tc_fk.table_catalog AND t.table_schema = tc_fk.table_schema AND t.table_name = tc_fk.table_name AND tc_fk.constraint_type = 'FOREIGN KEY')
LEFT JOIN
    information_schema.key_column_usage kcu_fk ON (tc_fk.table_catalog = kcu_fk.table_catalog AND tc_fk.table_schema = kcu_fk.table_schema AND tc_fk.table_name = kcu_fk.table_name AND tc_fk.constraint_catalog = kcu_fk.constraint_catalog AND tc_fk.constraint_schema = kcu_fk.constraint_schema AND tc_fk.constraint_name = kcu_fk.constraint_name AND c.column_name = kcu_fk.column_name)
LEFT JOIN
    information_schema.referential_constraints rc_fk ON (tc_fk.constraint_catalog = rc_fk.constraint_catalog AND tc_fk.constraint_schema = rc_fk.constraint_schema AND tc_fk.constraint_name = rc_fk.constraint_name)
LEFT JOIN
    information_schema.table_constraints AS tc_fk_pk ON (rc_fk.unique_constraint_catalog = tc_fk_pk.constraint_catalog AND rc_fk.unique_constraint_schema = tc_fk_pk.constraint_schema AND rc_fk.unique_constraint_name = tc_fk_pk.constraint_name)
LEFT JOIN
    information_schema.key_column_usage AS kcu_fk_pk ON (tc_fk_pk.table_catalog = kcu_fk_pk.table_catalog AND tc_fk_pk.table_schema = kcu_fk_pk.table_schema AND tc_fk_pk.table_name = kcu_fk_pk.table_name AND tc_fk_pk.constraint_name = kcu_fk_pk.constraint_name AND kcu_fk.ordinal_position = kcu_fk_pk.ordinal_position)
WHERE t.table_schema = @original_table_schema
GROUP BY
    t.table_schema,
    UPPER(t.table_name),
    c.column_name,
    c.data_type,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale,
    CASE WHEN c.is_nullable = 'YES' THEN 1 ELSE 0 END,
    c.ordinal_position,
    CASE WHEN tc.constraint_type = 'PRIMARY KEY' AND c.column_name = kcu.column_name THEN UPPER(tc.constraint_name) ELSE '' END, 
    CASE WHEN tc.constraint_type = 'PRIMARY KEY' AND c.column_name = kcu.column_name THEN CAST(kcu.ordinal_position AS VARCHAR(3)) ELSE '' END
ORDER BY
    "datatype_dbms_product",
    "package_name",
    "entity_schema",
    "entity_name",
    "attribute_position";