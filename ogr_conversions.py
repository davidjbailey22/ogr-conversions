"""
geo data conversions for ETL's - shapefile, gdb, postgis, csv, geojson
By D. Bailey
"""

import osgeo.ogr
import psycopg2

## load shapefile into postgis table

def shp2postgis(shp_lyr, post_table):

    # ogr shapefile set up
    shp = r"{}".format(shp_lyr)
    driver = osgeo.ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(shp, 0)
    layer = dataSource.GetLayer()
    shp_layer = layer.GetLayerDefn()
    field_names_list = []

    # function to iterate through fields
    def load():

        feature = layer.GetNextFeature()
        geometry = feature.GetGeometryRef()
        new_geom = geometry.GetGeometryName()
        field_names_list.append(new_geom + " GEOMETRY")
        for i in range(shp_layer.GetFieldCount()):
            field = shp_layer.GetFieldDefn(i).GetName()
            fieldTypeCode = shp_layer.GetFieldDefn(i).GetType()
            fieldType = shp_layer.GetFieldDefn(i).GetFieldTypeName(fieldTypeCode)

            if fieldType == "String":
                field_names_list.append(field + " TEXT")
            elif fieldType == "Real":
                field_names_list.append(field + " REAL")

        field_names = ', '.join(field_names_list)

        print field_names

        # postgis connection variables
        db = "db"
        user = "user"
        host = "host"
        passw = "passw"

        # connect to postgis
        connection = psycopg2.connect(db + user + host + passw)
        cursor = connection.cursor()

        # create query
        create_query = "CREATE TABLE {} ({});".format(post_table, field_names)
        cursor.execute(create_query)

        # commit to Database
        connection.commit()

    load()

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
