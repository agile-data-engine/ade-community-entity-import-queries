-- Parameters
create volatile table temp_params (
	package_name VARCHAR(100),
	entity_schema VARCHAR(100),
	entity_logical_name VARCHAR(100),
	entity_type VARCHAR(50),
	entity_physical_type VARCHAR(50),
	src_database_name VARCHAR(100),
	src_table_name VARCHAR(100)
)
on commit preserve rows;


---------------------------------
-- EDIT PARAMETER VALUES HERE: --
---------------------------------
insert into temp_params values (
	'STG_PACKAGE_NAME', -- ADE PACKAGE NAME
	'src', -- ADE ENTITY SCHEMA
	'SRCTABLENAME', -- ADE ENTITY LOGICAL NAME
	'SOURCE', -- ADE ENTITY TYPE
	'METADATA_ONLY', -- ADE ENTITY PHYSICAL TYPE

	-- Note that the used DBC.ColumnsV view does not have similar information about column data types for views.
	-- See Teradata documentation about DBC.ColumnsqV if you need to ingest data from views.
	'SRCDBNAME', -- SOURCE DATABASE NAME
	'SRCTABLENAME' -- SOURCE TABLE NAME
);
---------------------------------


-- Entity import
select
	(select package_name from temp_params) as package_name,
	'AGILE_DATA_ENGINE' as datatype_dbms_product,
	(select entity_schema from temp_params) as entity_schema,
	(select entity_logical_name from temp_params) as entity_logical_name,
	(select entity_type from temp_params) as entity_type,
	(select entity_physical_type from temp_params) as entity_physical_type,
	lower(ColumnName) as attribute_name,
	1 as attribute_nullable,
	row_number() over (order by ColumnId) as attribute_position,
	coalesce(CommentString, '') as attribute_description,
	case 
		-- Compare to ADE supported data types, note that many types listed here are not supported by default in ADE.
		-- Modify as needed, configure new data types in ADE or load as VARCHAR and convert in DW.
		when ColumnType = 'A1' then 'ARRAY'
		when ColumnType = 'AN' then 'MULTI-DIMENSIONAL ARRAY'
		when ColumnType = 'AT' then 'TIME'
		when ColumnType = 'BF' then 'BYTE'
		when ColumnType = 'BO' then 'BLOB'
		when ColumnType = 'BV' then 'VARBYTE'
		when ColumnType = 'CF' then 'CHAR'
		when ColumnType = 'CO' then 'CLOB'
		when ColumnType = 'CV' then 'VARCHAR'
		when ColumnType = 'D' then 'DECIMAL'
		when ColumnType = 'DA' then 'DATE'
		when ColumnType = 'DH' then 'INTERVAL DAY TO HOUR'
		when ColumnType = 'DM' then 'INTERVAL DAY TO MINUTE'
		when ColumnType = 'DS' then 'INTERVAL DAY TO SECOND'
		when ColumnType = 'DY' then 'INTERVAL DAY'
		when ColumnType = 'F' then 'DOUBLE' --'FLOAT'
		when ColumnType = 'HM' then 'INTERVAL HOUR TO MINUTE'
		when ColumnType = 'HS' then 'INTERVAL HOUR TO SECOND'
		when ColumnType = 'HR' then 'INTERVAL HOUR'
		when ColumnType = 'I' then 'INTEGER4' --'INTEGER'
		when ColumnType = 'I1' then 'INTEGER1' --'BYTEINT'
		when ColumnType = 'I2' then 'INTEGER2' --'SMALLINT'
		when ColumnType = 'I8' then 'INTEGER8' --'BIGINT'
		when ColumnType = 'JN' then 'JSON'
		when ColumnType = 'MI' then 'INTERVAL MINUTE'
		when ColumnType = 'MO' then 'INTERVAL MONTH'
		when ColumnType = 'MS' then 'INTERVAL MINUTE TO SECOND'
		when ColumnType = 'N' then 'DECIMAL' --'NUMBER'
		when ColumnType = 'PD' then 'PERIOD(DATE)'
		when ColumnType = 'PM' then 'PERIOD(TIMESTAMP WITH TIME ZONE)'
		when ColumnType = 'PS' then 'PERIOD(TIMESTAMP)'
		when ColumnType = 'PT' then 'PERIOD(TIME)'
		when ColumnType = 'PZ' then 'PERIOD(TIME WITH TIME ZONE)'
		when ColumnType = 'SC' then 'INTERVAL SECOND'
		when ColumnType = 'SZ' then 'TIMESTAMP_TZ' --'TIMESTAMP WITH TIME ZONE'
		when ColumnType = 'TS' then 'TIMESTAMP'
		when ColumnType = 'TZ' then 'TIME WITH TIME ZONE'
		when ColumnType = 'UT' then 'UDT Type'
		when ColumnType = 'XM' then 'XML'
		when ColumnType = 'YM' then 'INTERVAL YEAR TO MONTH'
		when ColumnType = 'YR' then 'INTERVAL YEAR'
		else ColumnType
	end as attribute_datatype,
	case
		when ColumnType in ('BF', 'BV', 'CF', 'CO', 'CV') then cast(ColumnLength as varchar(10))
		else ''
	end as attribute_length,
	case
		when ColumnType in ('D', 'N') then cast(DecimalTotalDigits as varchar(10))
		else ''
	end as attribute_precision,
	case
		when ColumnType in ('D', 'N') then cast(DecimalFractionalDigits as varchar(10))
		else ''
	end as attribute_scale
from 
	-- The DBC database contains critical system tables that define the user databases in the Teradata Database.
	-- https://docs.teradata.com/
	DBC.ColumnsV
where
	DatabaseName = (select src_database_name from temp_params)
	and TableName = (select src_table_name from temp_params)
order by
	9;


drop table temp_params;