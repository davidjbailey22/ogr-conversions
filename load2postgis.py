""""
load shapefile features into a postgis table
by D. Bailey
"""

import os.path
import psycopg2
import osgeo.ogr
import osr
from psycopg2 import sql


def load2postgis(post_db, post_user, post_host, post_pw, shp_path, shapefile, fields, post_table):

    # connect to postgis
    connection = psycopg2.connect(post_db + post_user + post_host + post_pw)
    cursor = connection.cursor()

    # drop tables first
    cursor.execute("DELETE FROM {}".format(post_table))

    # open shapefile
    file = os.path.join(shp_path, shapefile)
    shp = osgeo.ogr.Open(file)

    # get Shapefile fields and geometry
    layer1 = shp.GetLayer(0)
    for s in layer1:

        # use only two critical columns
        # get values for shapefile fields and perform basic data processing
        f1 = s.GetFieldAsString(fields[0]).replace("'", "''")
        f2 = s.GetFieldAsString(fields[1]).replace("'", "''")

        # get Geometry and Spatial Ref info from shapefile and export as a well known text
        geom = s.GetGeometryRef()
        spatialRef = geom.GetSpatialReference()
        datum = spatialRef.GetAttrValue("DATUM")
        proj = spatialRef.GetAttrValue("PROJECTION")
        wkt = geom.ExportToWkt()

        # load wkbPolygon or wkbMultiPolygon shapefiles based on GeometryType
        if geom.GetGeometryType() == 3 or geom.GetGeometryType() == 6: #osgeo.ogr.wkbPolygon and osgeo.ogr.wkbMultiPolygon
            # force a multipolygon to polygon
            geom = osgeo.ogr.ForceToPolygon(geom)
            wkt = geom.ExportToWkt()
            if datum == "North_American_Datum_1983":
                srid = "4269" #set to NAD83
                cursor.execute("INSERT INTO {} ({}, polygon) VALUES ('{}', '{}', ST_PolygonFromText('{}', {}))".format(post_table,",".join(fields),f1, f2, wkt, srid))

            elif datum == "WGS_1984" and proj != "Mercator_Auxiliary_Sphere":
                srid = "4326" #set to NAD83
                cursor.execute("INSERT INTO {} ({}, polygon) VALUES ('{}', '{}', ST_PolygonFromText('{}', {}))".format(post_table,",".join(fields),f1, f2, wkt,srid))

            elif datum == "WGS_1984" and proj == "Mercator_Auxiliary_Sphere":
                srid = "3857" #set to WGS_1984 web mercator
                cursor.execute("INSERT INTO {} ({}, polygon) VALUES ('{}', '{}', ST_PolygonFromText('{}', {}))".format(post_table,",".join(fields),f1, f2, wkt,srid))

        # load wkbPoint shapefiles based on GeometryType
        elif geom.GetGeometryType() ==  1: #osgeo.ogr.wkbPoint:

            # filter out POINT (too_big too_big) geom error
            if wkt == "POINT (too_big too_big)":
                print "record removed from load due to POINT (too_big too_big) error"

            else:
                # get SRID based on datum and projection of shapefile
                # insert records into PostGIS
                if datum == "North_American_Datum_1983":
                    srid = "4269"
                    cursor.execute("INSERT INTO {} ({}, point) VALUES ('{}', '{}', St_PointFromText('{}', {}))".format(post_table,",".join(fields), f1,f2, wkt, srid))
                elif datum == "WGS_1984" and proj != "Mercator_Auxiliary_Sphere":
                    srid = "4326"
                    cursor.execute("INSERT INTO {} ({}, point) VALUES ('{}', '{}', St_PointFromText('{}', {}))".format(post_table,",".join(fields), f1,f2, wkt, srid))
                elif datum == "WGS_1984" and proj == "Mercator_Auxiliary_Sphere":
                    srid = "3857"  # set to WGS_1984 web mercator
                    cursor.execute("INSERT INTO {} ({}, point) VALUES ('{}', '{}', St_PointFromText('{}', {}))".format(post_table,",".join(fields),f1, f2, wkt,srid))
                else:
                    srid = "1116"
                    cursor.execute("INSERT INTO {} ({}, point) VALUES ('{}', '{}', St_PointFromText('{}', {}))".format(post_table, ",".join(fields), f1, f2, wkt,srid))

        # load wkbLineString shapefiles based on GeometryType
        elif geom.GetGeometryType() == 2: #osgeo.ogr.wkbLineString

            # get SRID based on datum and projection of shapefile
            # insert records into PostGIS
            if datum == "North_American_Datum_1983":
                srid = "4269" #set to NAD83
                cursor.execute("INSERT INTO {} ({}, linestring) VALUES ('{}', '{}', ST_LineFromText('{}', {}))".format(post_table,",".join(fields),f1, f2, wkt, srid))

            elif datum == "WGS_1984" and proj != "Mercator_Auxiliary_Sphere":
                srid = "4326" #set to NAD83
                cursor.execute("INSERT INTO {} ({}, linestring) VALUES ('{}', '{}', ST_LineFromText('{}', {}))".format(post_table,",".join(fields), f1,f2, wkt, srid))
            elif datum == "WGS_1984" and proj == "Mercator_Auxiliary_Sphere":
                srid = "3857"  # set to WGS_1984 web mercator
                cursor.execute("INSERT INTO {} ({}, linestring) VALUES ('{}', '{}', ST_LineFromText('{}', {}))".format(post_table,",".join(fields),f1, f2, wkt,srid))

        else:
            # shapefile's geom type is neither wkbPolygon, wkbPoint, or wkbLineString
            print "Undefined Geom (neither wkbPolygon, wkbPoint, or wkbLineString)"

    # commit to postgis
    connection.commit()
