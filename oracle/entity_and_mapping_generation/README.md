## Example queries for creating entities and mappings

These examples generate the following:
1. SOURCE-entities: [source_import.sql](source_import.sql)
2. STAGE-entities: [stage_with_meta.sql](stage_with_meta.sql)
3. Mappings between SOURCE and STAGE -entities: [mapping_import.sql](mapping_import.sql)
4. DW-entities: [dw_import.sql](dw_import.sql)
5. Generates DW-loads based on Oracle primary keys. Creates mappings from STAGE-entieties to DW-entities: [dw_mapping_import.sql](dw_mapping_import.sql)

Import resulting CSV:s to ADE in the specified order.