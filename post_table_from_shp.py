"""
create postgis table schema from shapefile schema
By D. Bailey
"""

import osgeo.ogr
import psycopg2

def createfromshp(shp_lyr, post_table):

    # ogr shapefile set up
    shp = r"{}".format(shp_lyr)
    driver = osgeo.ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(shp, 0)
    layer = dataSource.GetLayer()
    shp_layer = layer.GetLayerDefn()
    field_names_list = []

    # function to iterate through fields
    def create():

        feature = layer.GetNextFeature()
        geometry = feature.GetGeometryRef()
        new_geom = geometry.GetGeometryName()
        spatialRef = geometry.GetSpatialReference()
        datum = spatialRef.GetAttrValue("DATUM")
        proj = spatialRef.GetAttrValue("PROJECTION")
        print "Spatial info: {} , {}, {} ".format(spatialRef, datum, new_geom)

        # For points
        if new_geom == "POINT":
            if datum == "North_American_Datum_1983":
                field_names_list.append(new_geom + " GEOMETRY (point,4269)")
            elif datum == "WGS_1984" and proj != "Mercator_Auxiliary_Sphere":
                field_names_list.append(new_geom + " GEOMETRY (point,4326)")
            elif datum == "WGS_1984" and proj == "Mercator_Auxiliary_Sphere":
                field_names_list.append(new_geom + " GEOMETRY (point,3857)")
            else:
                print "no known datum"

        # For polygon
        if new_geom == "POLYGON":
            if datum == "North_American_Datum_1983":
                field_names_list.append(new_geom + " GEOMETRY (polygon,4269)")
            elif datum == "WGS_1984" and proj != "Mercator_Auxiliary_Sphere":
                field_names_list.append(new_geom + " GEOMETRY (polygon,4326)")
            elif datum == "WGS_1984" and proj == "Mercator_Auxiliary_Sphere":
                field_names_list.append(new_geom + " GEOMETRY (polygon,3857)")
            else:
                print "no known datum"

        # For lines
        if new_geom == "LINESTRING":
            if datum == "North_American_Datum_1983":
                field_names_list.append(new_geom + " GEOMETRY (linestring,4269)")
            elif datum == "WGS_1984" and proj != "Mercator_Auxiliary_Sphere":
                field_names_list.append(new_geom + " GEOMETRY (linestring,4326)")
            elif datum == "WGS_1984" and proj == "Mercator_Auxiliary_Sphere":
                field_names_list.append(new_geom + " GEOMETRY (linestring,3857)")
            else:
                print "no known datum"

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
        pw = ""
        db = "dbname='name of database'"
        user = "user='name of user'"
        host = "host='host'"
        passw = "password='{}'".format(pw)

        # connect to postgis
        connection = psycopg2.connect(db + user + host + passw)
        cursor = connection.cursor()

        # create sql query with field names
        create_query = "CREATE TABLE {} ({});".format(post_table, field_names)
        cursor.execute(create_query)

        # commit to Database
        connection.commit()

    create()