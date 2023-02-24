/* See possible values from the documentation: https://ade.document360.io/docs/entity-import#columns */

set database_dbms_product = 'SNOWFLAKE';
set package_name = 'PACKAGE_NAME';
set entity_physical_type = 'METADATA_ONLY';
set entity_schema = 'SRC';
set entity_type = 'SOURCE';
/* Optional. Useful to have when entity type is SOURCE */ 
--set entity_source_system = 'SYSTEM';
/* Original schema from Snowflake */
set original_table_schema = 'RAW';

select
    $database_dbms_product as "datatype_dbms_product",
    /* Optional. Useful when entity type is SOURCE */
    --$entity_source_system as "entity_dv_source",
    $package_name as "package_name",
    $entity_physical_type as "entity_physical_type",
    $entity_schema as "entity_schema",
    c.table_name as "entity_name",
    $entity_type as "entity_type",
    c.column_name as "attribute_name",
    upper(
        case
            when c.data_type = 'ARRAY' then 'array'
            when c.data_type = 'VARIANT' then 'variant'
            when c.data_type = 'TEXT' then 'varchar'
            when c.data_type like 'TIMESTAMP_NTZ' then 'timestamp'
            when c.data_type like 'TIMESTAMP_TZ' then 'timestamptz'
            when c.data_type = 'NUMBER' and c.numeric_precision = 38 then 'bigint'
            when c.data_type = 'NUMBER' and c.numeric_precision < 38 then 'number'
            else c.data_type
        end
      ) as "attribute_datatype",
    case
        when c.data_type like 'TEXT%' then c.character_maximum_length
        else null end
    as "attribute_length",
    case
       when c.data_type = 'NUMBER' and c.numeric_precision = 38 then null
       when c.data_type = 'NUMBER' and c.numeric_precision < 38 then c.numeric_precision
       else null end
    as "attribute_precision",
    case
       when c.data_type = 'NUMBER' and c.numeric_precision = 38 then null
       when c.data_type = 'NUMBER' and c.numeric_precision < 38 then c.numeric_scale
       else null end
    as "attribute_scale",
    case when c.is_nullable = 'YES' then 1 else 0 end "nullable",
    c.ordinal_position AS "position"
from information_schema.columns c 
where table_schema ilike $original_table_schema
order by "entity_name", ordinal_position;