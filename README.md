<h3>Overview<h3/>

geo data conversions used to plug into existing geospatial ETL workflows

<h3>Example<h3/>

first define database connections variables:

db = "db"
user = "user"
host = "host"
passw = "passw"

**shp2postgis(**"/GIS_Data/shp/city_boundaries.shp", "prod_data.cities"**)**
