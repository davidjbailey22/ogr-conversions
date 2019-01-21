"""
geo data conversions for ETL's - shapefile, gdb, postgis, csv, geojson
By D. Bailey
"""

import osgeo.ogr
import psycopg2

# postgis to shp

def postgis2shp(post_table):

    # connection variables
    db = "dbname='db'"
    user = "user='user'"
    host = "host='host'"
    passw = "password=password"

    # connect to postgis
    connection = psycopg2.connect(db + user + host + passw)
    cursor = connection.cursor()

    # select query
    select_query = "SELECT * from {};".format(post_table)
    cursor.execute(select_query)
    values = cursor.fetchall()

    print values

    # commit to Database
    connection.commit()


#postgis2shp()
