/* See possible values from the documentation: https://docs.agiledataengine.com/docs/import-entities#Importentities-Columns */

DECLARE database_dbms_product STRING DEFAULT 'BIGQUERY';
DECLARE package_name STRING DEFAULT 'PACKAGE_NAME';
DECLARE entity_physical_type STRING DEFAULT 'METADATA_ONLY';
DECLARE entity_schema STRING DEFAULT 'SRC';
DECLARE entity_type STRING DEFAULT 'SOURCE';
/* Optional. Useful to have when entity type is SOURCE */ 
--DECLARE entity_source_system STRING DEFAULT 'SYSTEM';

select
    (database_dbms_product) as datatype_dbms_product,
    /* Optional. Useful when entity type is SOURCE */
    --(entity_source_system) as entity_dv_source,
    (package_name) as package_name,
    (entity_physical_type) as entity_physical_type,
    (entity_schema) as entity_schema,
    c.table_name as entity_name,
    (entity_type) as entity_type,
    c.column_name as attribute_name,
    upper(
        case
            when c.data_type = 'ARRAY<STRING>' then 'ARRAY'
            when split(c.data_type, '(')[SAFE_OFFSET(0)] = 'STRING' then 'STRING'
            when split(c.data_type, '(')[SAFE_OFFSET(0)] = 'NUMERIC' then 'NUMERIC'
            else c.data_type
        end
      ) as attribute_datatype,
    case
        when c.data_type like 'STRING(%'
            then 
                split(split(c.data_type, '(')[SAFE_OFFSET(1)], ')')[SAFE_OFFSET(0)]
        /* If length not given, setting default length, because it is required in ADE */
        when c.data_type = 'STRING' then '255'
        else null end
    as attribute_length,
    case
        when c.data_type like 'NUMERIC(%'
            then split(split(split(c.data_type, '(')[SAFE_OFFSET(1)], ')')[SAFE_OFFSET(0)], ',')[SAFE_OFFSET(0)]
        /* If precision not given, setting default precision, because it is required in ADE */
        when c.data_type = 'NUMERIC' then '20'
       else null end
    as attribute_precision,
    case
        when c.data_type like 'NUMERIC(%'
            then split(split(split(c.data_type, '(')[SAFE_OFFSET(1)], ')')[SAFE_OFFSET(0)], ',')[SAFE_OFFSET(1)]
        /* If scale not given, setting default scale, because it is required in ADE */
        when c.data_type = 'NUMERIC' then '5'
       else null end
    as attribute_scale,
    case when c.is_nullable = 'YES' then 1 else 0 end nullable,
    c.ordinal_position AS position
/* Information schema is dataset/schema specific. This example is for raw-dataset */
from raw.INFORMATION_SCHEMA.COLUMNS c 
order by entity_name, ordinal_position;