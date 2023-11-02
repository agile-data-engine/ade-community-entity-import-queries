/* See possible values from the documentation: https://docs.agiledataengine.com/docs/import-entities#Importentities-Columns */

set database_dbms_product = 'SNOWFLAKE';
set package_name = 'PACKAGE_NAME';
set entity_physical_type = 'METADATA_ONLY';
set entity_schema = 'SRC';
set entity_type = 'SOURCE';
set entity_name = 'my_entity_name';
/* Optional. Useful to have when entity type is SOURCE */ 
set entity_source_system = 'my_source_system';

/* Create CSV file format which matches the files in your cloud storage */
create or replace file format csv_format
  type = csv
  parse_header = true
  field_delimiter = ';';

/* Create JSON file format which matches the files in your cloud storage */
create or replace file format json_format
  type = json
  strip_outer_array = true;

/* Create PARQUET file format which matches the files in your cloud storage */
create or replace file format parquet_format
  type = parquet;


select 
    $database_dbms_product as "datatype_dbms_product",
    /* Optional. Useful when entity type is SOURCE */
    $entity_source_system as "entity_dv_source",
    $package_name as "package_name",
    $entity_physical_type as "entity_physical_type",
    $entity_schema as "entity_schema",
    $entity_name as "entity_name",
    $entity_type as "entity_type",
    /* Replacing possible '.' with '_' */
    replace(replace(column_name::string, '"', ''), '.', '_') as "attribute_name",
    case 
        when type like ('NUMBER(%, 0)') then 'BIGINT'
        when type like ('NUMBER(%, %)') then 'NUMBER'
        when type like ('TEXT') then 'VARCHAR'
        when type like 'TIMESTAMP_NTZ' then 'TIMESTAMP'
        else type end
        as "attribute_datatype",
    case
        when type like 'TEXT%' then 255
        else null end
    as "attribute_length",
    case
       when type like ('NUMBER(%, 0)') then null
       when type like ('NUMBER(%, %)') then split(split(type, '(')[1], ',')[0]::integer
       else null end
    as "attribute_precision",
    case
       when type like ('NUMBER(%, 0)') then null
       when type like ('NUMBER(%, %)') then split(split(type, ')')[0], ',')[1]::integer
       else null end
    as "attribute_scale",
    case when nullable = 'TRUE' then 1 else 0 end "nullable", 
    order_id + 1 AS "position"
  from table(
    /* See Snowflake documentation for more information: 
       https://docs.snowflake.com/en/sql-reference/functions/infer_schema */
    infer_schema(
      location=>'@<sf_stage_name>/<file_location>'
      , file_format=>'<file format csv_format/json_format/parquet_format>'
      )
    );