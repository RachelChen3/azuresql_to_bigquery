import pyodbc
import google
from google.cloud import bigquery
import os
import argparse
import re

def get_ranges(Number,unit):
    '''
    :param Number: Records amount
    :param unit: chunksize the data
    :return: list of the data range. for example, if Number = 309, will return [(0,100),(100,200),(200,300),(300,309)]
    '''
    step = int(Number / unit)
    range_list = [(idx*unit,(idx+1)*unit) for idx in list(range(step))]
    range_list += [(step*unit,Number)]
    return range_list

def sqlnameintobq(sqlname):
    return ''.join([el.title() for el in list(filter(None,re.findall(r'(\w*)',sqlname)))])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parameters for connecting Azure SQL
    parser.add_argument('--datasetname_sql',type=str,
                        help="your dataset name in Azure SQL",required=True)

    parser.add_argument('--server',type=str,
                        help="your server name or ip",required=True)
    parser.add_argument('--driver',default='{ODBC Driver 17 for SQL Server}',type=str,
                        help="ODBC driver, default is ODBC driver 17 ",required=False)
    parser.add_argument('--username',type=str,
                        help="username to login Azure SQL",required=True)
    parser.add_argument('--password',default='yourargs.password',type=str,
                        help="password to login Azure SQL",required=True)
    parser.add_argument('--bqcredential',type=str,
                        help="google credential json for access bigqeury",required=True)

    # parameters for bigquery tables
    parser.add_argument('--datasetname_bq',type=str, default=None,
                        help="your dataset name in bigquery table, default is the same datasetname in Azure SQL")
    parser.add_argument('--table_bq',type=str, default=None,
                        help="your table name in bigquery, default is the same table name from your Azure SQL")
    parser.add_argument('--rowsunit',default=100,type=int,
                        help="chunksize for the data, default is 100",required=False)

    # parameters for sql selection query
    parser.add_argument('--sql_table',type=str,
                        help="table name in sql select query",required=True)
    parser.add_argument('--sql_columnid',type=str,
                        help="column name which sort by this column in selection sql query",required=True)

    # delet dataset?
    parser.add_argument("--do_delete", action="store_true", help="deleting the whole dataset.")

    args = parser.parse_args()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.bqcredential

    print("the connection string is:" + 'DRIVER='+str(args.driver).strip()+';SERVER='+str(args.server).strip()+';PORT=1433;DATABASE='+str(args.datasetname_sql).strip()+';UID='+str(args.username).strip()+';PWD='+str(args.password).strip())

    cnxn = pyodbc.connect('DRIVER='+str(args.driver).strip()+';SERVER='+str(args.server).strip()+';PORT=1433;DATABASE='+str(args.datasetname_sql).strip()+';UID='+str(args.username).strip()+';PWD='+str(args.password).strip())
    cursor = cnxn.cursor()

    cursor.execute("select count(*) from {}".format(args.sql_table))
    records_amount = cursor.fetchone()
    records_amount = records_amount[0]

    range_list = get_ranges(records_amount,100)

for range_pair in range_list:
    cursor.execute("SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (ORDER BY {}) AS RowNum FROM {}) "
                   "AS MyDerivedTable WHERE MyDerivedTable.RowNum BETWEEN {} AND {}".format(
        args.sql_columnid,args.sql_table,int(range_pair[0]),int(range_pair[1])))

    row = cursor.fetchone()
    if int(range_pair[0]) == 0: # access the first 100(unit) records
        colnames = [ele[0] for ele in list(row.cursor_description)]
        datatypes = [str(ele[1]) for ele in list(row.cursor_description)]
        nullmodel =  [str(ele[6]) for ele in list(row.cursor_description)]

        # mapping field names into bigquery format
        datatype_mappings = {"<class 'int'>":"INT64","<class 'datetime.datetime'>":"DATETIME","<class 'str'>":"STRING",
                             "<class 'bool'>":"BOOL","<class 'bytearray'>":"BYTES","<class 'float'>":"FLOAT64"}
        null_mapings = {"False":"Required","True":"Nullable"}

        datatype_bq = [datatype_mappings[ele] for ele in datatypes]
        model_bq = [null_mapings[ele] for ele in nullmodel]

        # delete tabels
        client = bigquery.Client()
        if args.datasetname_bq == None:
            args.datasetname_bq = sqlnameintobq(args.datasetname_sql)
        if args.table_bq == None:
            args.table_bq  = sqlnameintobq(args.sql_table)

        dataset_id = "{}.{}".format(client.project,args.datasetname_bq)
        table_id = '{}.{}'.format(dataset_id,args.table_bq)
        client.delete_table(table_id, not_found_ok=True)  # Make an API request.
        print("Deleted table '{}'.".format(args.table_bq))

        # create dataset
        try:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "europe-west2"
            dataset = client.create_dataset(dataset)  # Make an API request.
            print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
        except google.api_core.exceptions.Conflict:
            print("Dataset {}.{} already existed!".format(client.project, dataset.dataset_id))
            pass

        # create table
        schema = [bigquery.SchemaField(colnames[idx],datatype_bq[idx],mode =model_bq[idx]) for idx,val in enumerate(datatype_bq)]
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))


    rowlist = []
    while row:
        rowlist.append(tuple(row))
        row = cursor.fetchone()

    # print(rowlist)
    errors = client.insert_rows(table, rowlist)  # Make an API request.
    if errors == []:
        print("New rows have been added.")

    if args.do_delete:
        # delete the database
        client.delete_dataset(
            dataset_id, delete_contents=True, not_found_ok=True
        )  # Make an API request.
        print("Deleted dataset '{}'.".format(dataset_id))


