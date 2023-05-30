# ADE Community entity import queries
This repository contains example solutions for querying different source databases and importing the resulting CSVs of those queries to Agile Data Engine. 

Contents:
- Queries for different database products
  - Microsoft SQL Server / Azure SQL Database
  - Snowflake
  - Oracle

## Purpose
This solution helps to import entity metadata, so you can have correct schema created by ADE. After importing entity metadata, you can load your source files correctly to target database.

For more information, please refer to the official documentation: https://docs.agiledataengine.com/docs/import-entities


## Usage
1) Execute query in source database
2) Export the result set in CSV format
3) Import the CSV file to Agile Data Engine
4) Check the results from resulted package(s)

## Disclaimer

**The repository is provided as community solution and it may require modifications to fit your use case. Note that this solution is not part of the Agile Data Engine product. Please use at your own caution.**
