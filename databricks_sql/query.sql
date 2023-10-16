/* See possible values from the documentation: https://docs.agiledataengine.com/docs/import-entities#Importentities-Columns */

/* Query uses query parameters: https://docs.databricks.com/en/sql/user/queries/query-parameters.html
   Example values:
        database_dbms_product = DATABRICKS
        entity_source_system = ERP
            - Entity source system must exist in CONFIG_SYSTEMS -package beforehand
        package_name = STG_DATABRICKS_DEMO
        entity_physical_type = METADATA_ONLY
        entity_schema = src
        entity_type = SOURCE
        original_schema = RAW
            - Example of existing schema in Databricks
*/
select
    {{database_dbms_product}} as datatype_dbms_product,
    /* Optional. Useful when entity type is SOURCE */
    {{entity_source_system}} as entity_dv_source,
    {{package_name}} as package_name,
    {{entity_physical_type}} as entity_physical_type,
    {{entity_schema}} as entity_schema,
    c.table_name as entity_name,
    {{entity_type}} as entity_type,
    c.column_name as attribute_name,
    upper(
        case
            when c.full_data_type like 'varchar%' then 'string'
            when c.data_type ilike 'decimal' then c.data_type
            when c.full_data_type ilike 'array<string>' then 'array'
            /* ADE default CONFIG_DATATYPES does not have logical datatype corresponding 
               array<int> datatype. It can either be treated as string or you need to add 
               new logical datatype to match with array<int> */
            when c.full_data_type ilike 'array<int>' then 'array'
            else c.full_data_type
        end
      ) as attribute_datatype,
    case
        when c.full_data_type like 'varchar%' then split(split(c.full_data_type, '[(]')[1], '[)]')[0]
        /* On default, CONFIG_DATATYPES requires string to have length defined. 255 is added as initial value */
        when c.full_data_type = 'string' then '255'
        /* On default, CONFIG_DATATYPES requires binary to have length defined. 255 is added as initial value */
        when c.full_data_type = 'binary' then '255'
        else null end
    as attribute_length,
    case
       /* Numeric precision from full datatype, since numeric_precision does not seem to work */
       when c.data_type ilike 'decimal' 
        then split(split(split(c.full_data_type, '[(]')[1], '[)]')[0], '[,]')[0]
       else null end
    as attribute_precision,
    case
       when c.full_data_type like 'decimal%' 
       /* Numeric precision from full datatype, since numeric_scale does not seem to work */
        then split(split(split(c.full_data_type, '[(]')[1], '[)]')[0], '[,]')[1]
       else null end
    as attribute_scale,
    case when c.is_nullable = 'YES' then 1 else 0 end nullable,
    /* Positions start from 0 and in ADE they start from 1 */
    c.ordinal_position + 1 AS position
from information_schema.columns c 
where table_schema ilike {{original_schema}}
order by entity_name, ordinal_position;