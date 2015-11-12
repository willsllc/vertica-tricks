# first execute the generate_schema.sql query on your MSSQL server, and save the results to a file
# then use this python script to convert to a Vertica-compliant SQL file

import csv, argparse
from itertools import groupby
	
parser = argparse.ArgumentParser(description='Create Vertica SQL to generate tables from a CSV file.')
parser.add_argument('filename',help='Path to a .csv file that contains one row per column')
args = parser.parse_args()

# expecting a CSV with the following columns:
#  0: schema_name
#  1: table_name
#  2: column_name
#  3: is_null
#  4: is_pk
#  5: mssql_data_type
#  6: vertica_data_type
#  7: data_length

with open(args.filename, 'rb') as f:
    reader = csv.reader(f)
    schema = list(reader)

# generate create table statements for every table
sql = '';
for key, group in groupby(schema, lambda x: x[0] + '.' + x[1]):
    sql += 'create table ' + key + ' ('
    for col in group:
        if(col[6] != 'NULL'):
            sql += '\r\n\t"' + col[2] + '" ' + col[6]
            if(col[6] == 'varchar'):
                sql += '(' + col[7] + ')'
            if(col[4] == '1'):
                sql += ' primary key '
            if(col[3] == '0'):
                sql += ' not null '
            sql += ','
    sql = sql[:-1] #trim trailing comma
    sql += '\r\n); \r\n\r\n'

# go to standard output
print sql