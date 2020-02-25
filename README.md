# Azure SQL to BigQuery

## Task: Immigrate data from Azure SQL into Bigquery tables

## Steps:
1. Follow the [tutorial](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-connect-query-python?tabs=macos) to install drivers on Mac for connecting Azure SQL. 

Note: SQL Server is a little difference with Azure SQL. For SQL Server,check this [tutorial](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Mac-OSX).

2. Down google credential and install the library for bigquery. [tutorial](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries)

3. Run 

```
python azuresqlconn.py --datasetname_sql=dataset name in Azure SQL \
--server=servename or server IP 
--username=username to login the Azure SQL 
--password=password for loging  
--bqcredential=path to google credential json for allowing accessing your BigQuery 
--sql_table=sql table name
--sql_columnid=column id name 
```
