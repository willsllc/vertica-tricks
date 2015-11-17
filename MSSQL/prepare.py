# first execute the generate_schema.sql query on your MSSQL server, and save the results to a file
# then use this python script to convert to a Vertica-compliant SQL file

import csv, argparse, json
from itertools import groupby
	
parser = argparse.ArgumentParser(description='Create Vertica SQL to generate tables from a CSV file.')
parser.add_argument('filename',help='Path to a .csv file that contains one row per column')
parser.add_argument('schema',help='Path to save the CREATE TABLE statements')
parser.add_argument('tables',help='Path to save the table metadata JSON file')
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


def is_column_valid(col):
    """Given a column definition, checks if it is Vertica-compliant."""
    if(col[6] == 'NULL'):
        return False
    if(col[6] == 'varchar'):
        if(col[7].isdigit()):
            l = int(col[7])
            if(l > 0 and l < 6500):
                return True
            return False
        return False
    return True
	
with open(args.filename, 'rb') as f:
    reader = csv.reader(f)
    filename = list(reader)

# iterate through all the tables
schema = ''     # will hold a big SQL string with CREATE TABLE statements for each table
tables = []     # will hold a big JSON array with metadata about each table
for key, group in groupby(filename, lambda x: x[0] + '.' + x[1]):
    valid_cols = [];
    sql_schema = 'create table ' + key + ' ('
    sql_export = 'select '
    sql_import = 'copy ' + key + ' columns('
    for col in group:
        if(is_column_valid(col)):           #only for valid columns...
            valid_cols.append(col[2])       #append to the array
            sql_import += col[2] + ','      #append to the COPY statement
            #append to the SELECT query
            sql_export += 'case when ' + col[2] + ' is null then \'NULL\' else quotename(replace(replace(replace(cast(' + col[2] + ' as varchar),char(13),\'<br>\'),char(10),\'<br/>\'),char(34),\'`\'),char(34)) end as ' + col[2] + ','     
            #append to the CREATE query 
            sql_schema +=  '\r\n\t"' + col[2] + '" ' + col[6]   
            if(col[6] == 'varchar'):
                sql_schema += '(' + col[7] + ')'
            if(col[4] == '1'):
                sql_schema += ' primary key '
            if(col[3] == '0'):
                sql_schema += ' not null '
            sql_schema += ','
    # trim trailing commas
    sql_schema = sql_schema[:-1] 
    sql_export = sql_export[:-1]
    sql_import = sql_import[:-1]
    # close out the statements
    sql_schema += '\r\n); \r\n\r\n'
    sql_export += ' from ' + key + ';'
    sql_import += ') from @file delimited \',\' enclosed by "" null \'NULL\' on error abort direct;'
    # append
    schema += sql_schema
    tables.append({'name':key,'columns':valid_cols,'schema':sql_schema,'export':sql_export,'import':sql_import})

# write the sql to schema output file
with open(args.schema, 'w+') as f:
    f.write(schema)

# write the sql to schema output file
with open(args.tables, 'w+') as f:
    f.write(json.dumps(tables,indent=2))

