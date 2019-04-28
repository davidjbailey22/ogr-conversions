"""
data conversions for ETL's - csv to postgis
By D. Bailey
"""

import os.path
import psycopg2
import osgeo.ogr
import pandas as pd
pd.set_option('display.max_columns', 2000)
pd.set_option('display.max_rows', 9000)
import numpy as np

### csv to postgis ###

# csv_file: input csv file
# table: name of database table including schema
# columns: list of columns

def csv2post(csv_file, table, columns):

    # create pandas data frame from csv file
    df_csv = pd.read_csv(csv_file)[columns]
    print df_csv

    ## load csv into table ##

    # connection variables
    db = "dbname='db'"
    user = "user='user'"
    host = "host='host'"
    passw = "password=pw"

    # connect to postgis
    connection = psycopg2.connect(db + user + host + passw)
    cursor = connection.cursor()
    schema_name = table.split(".")[0]
    db_name = db.split("=")[1]
    table_name = table.split(".")[1]

    from sqlalchemy import create_engine
    # create db engine
    engine = create_engine('postgresql://username:password@host:5432/database', pool_pre_ping=True)

    # option for drop TABLE
    cursor.execute("DELETE FROM %s;" % table)

    #load
    df_csv.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
    connection.commit()
