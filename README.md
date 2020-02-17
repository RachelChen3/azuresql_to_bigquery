# Azure SQL to BigQuery

## Task: Immigrate data from Azure SQL into Bigquery tables

## Steps:
1. Follow the (tutorial)[https://docs.microsoft.com/en-us/azure/sql-database/sql-database-connect-query-python?tabs=macos] to install drivers on Mac for connecting Azure SQL. 

Note: SQL Server is a little difference with Azure SQL. For SQL Server,check this (tutorial)[https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Mac-OSX].

2. Down google credential and install the library for bigquery. (tutorial)[https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries]

3. Run 

```
python azuresqlconn.py --datasetname='datasetname' \
--tablename='table name' \
--server='server name/ip' \
--username='user name' \
--password='passwords' \
--bqcredential='path to your downloaded google credential json file' \
--datasetname_BQ='database name you wanna created in bigquery table' \
--sql_tables='selected table names in sql selection query' \
--sql_columnid='column name which you wanna sorted by this column in sql selection query'
```
