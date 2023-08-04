import geopandas as gpd
import duckdb
municipalities = gpd.read_file("data/Com01012023_WGS84.shp")
trento = municipalities[municipalities.COMUNE == "Trento"]
geo_trento = gpd.GeoDataFrame(trento,geometry="geometry",crs="epsg:4326")
minx = geo_trento.geometry.bounds.minx.values[0]
maxx = geo_trento.geometry.bounds.minx.values[0]
miny = geo_trento.geometry.bounds.minx.values[0]
maxy = geo_trento.geometry.bounds.minx.values[0]
sql = []

sql_places = """ 
INSTALL spatial;
INSTALL httpfs;
LOAD spatial;
LOAD httpfs;
SET s3_region='us-west-2';
COPY (
   SELECT
       	id,
       	updatetime,
       	version,
       	confidence,
       	json_extract_string(names, '$.common[0].value') as name,
       	json_extract_string(categories, '$.main') as main_category,
       	json_extract_string(categories, '$.alternate[0]') as second_category,
	json_extract_string(websites, '$[0]') as website,
	json_extract_string(socials, '$[0]') as social,
	json_extract_string(emails, '$[0]') as email,
	json_extract_string(phones, '$[0]') as phone,
	json_extract_string(phones, '$[1]') as second_phone,
	json_extract_string(brand, '$.names.common[0].value') as brand,
	json_extract_string(addresses, '$[0].freeform') as complete_address,
	json_extract_string(addresses, '$[0].locality') as locality,
	json_extract_string(addresses, '$[0].postCode') as postcode,
	json_extract_string(addresses, '$[0].region') as region,
	json_extract_string(addresses, '$[0].country') as country,
	json_extract_string(sources, '$[0].property') as source_property,
	json_extract_string(sources, '$[0].dataset') as source_dataset,
       	ST_GeomFromWkb(geometry) AS geometry,
       FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=places/type=*/*', filename=true, hive_partitioning=1)
WHERE
  	bbox.minx > %s 
  	and bbox.maxx < %s
  	and bbox.miny > %s
  	and bbox.maxy < %s
) TO 'places_trento.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
""" % (minx,maxx,miny,maxy)
sql.append(sql_places)

sql_buildings = """ 
INSTALL spatial;
INSTALL httpfs;
LOAD spatial;
LOAD httpfs;
SET s3_region='us-west-2';
COPY (
   SELECT
       	id,
       	updatetime,
       	version,
 	json_extract_string(sources, '$.property[0]') as source_property,
	json_extract_string(sources, '$.dataset[0]') as source_dataset,
       	json_extract_string(names, '$.common[0].value') as name,
       	height,
       	numFloors,
       	ST_GeomFromWkb(geometry) AS geometry,
       FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=buildings/type=*/*', filename=true, hive_partitioning=1)
WHERE
  	bbox.minx > %s 
  	and bbox.maxx < %s
  	and bbox.miny > %s
  	and bbox.maxy < %s
) TO 'buildings_trento.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
""" % (minx,maxx,miny,maxy)
sql.append(sql_buildings)

sql_transportation_connector = """
INSTALL spatial;
INSTALL httpfs;
LOAD spatial;
LOAD httpfs;
SET s3_region='us-west-2';
COPY (
   SELECT
       	id,
       	ST_GeomFromWkb(geometry) AS geometry
       FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=transportation/type=connector/*', filename=true, hive_partitioning=1)
   WHERE
  	bbox.minx > 11.041575 
  	and bbox.maxx < 11.191811
  	and bbox.miny > 45.979679
  	and bbox.maxy < 46.144072
) TO 'transportation_connector_trento.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
"""
sql.append(sql_transportation_connector)

sql_roads = """
INSTALL spatial;
INSTALL httpfs;
LOAD spatial;
LOAD httpfs;
SET s3_region='us-west-2';
COPY (
   SELECT
       	id,
       	updatetime,
       	version,
	subType,
	cast (connectors as json) as connectors,
	json_extract_string(road, '$.roadNames.common[0].value') as name,
	json_extract_string(road, '$.class') as classification,
	json_extract_string(road, '$.surface') as surface,
	json_extract_string(road, '$.lanes[0].direction') as lane1_direction,
	json_extract_string(road, '$.lanes[1].direction') as lane2_direction,
       	ST_GeomFromWkb(geometry) AS geometry,
       FROM read_parquet('s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=transportation/type=segment/*', filename=true, hive_partitioning=1)
WHERE
  	bbox.minx > 11.041575 
  	and bbox.maxx < 11.191811
  	and bbox.miny > 45.979679
  	and bbox.maxy < 46.144072
) TO 'roads_trento.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON');
"""
sql.append(sql_roads)

# s = sql_places
# duckdb.sql(s)

for s in sql:
    duckdb.sql(s)

