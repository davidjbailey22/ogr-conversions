**Overview**

geo data conversions used to plug into existing geospatial ETL workflows

**Example**

first define database connections variables:

db = "database name"
user = "database user name"
host = "host"
passw = "password for user name"

load2postgis("/GIS_Data/shp/city_boundaries.shp", "prod_data.cities")
