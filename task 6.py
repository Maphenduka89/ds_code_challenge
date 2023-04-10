"""
Task 5
"""
import h3
import pandas
import utils
import requests
from geopy.geocoders import Nominatim

# Initialize the geolocator
geolocator = Nominatim(user_agent="my_application")

# Find the location of the town you're interested in
location = geolocator.geocode("BELLVILLE SOUTH, South Africa")

# Get the latitude and longitude of the location
lat = location.latitude
lon = location.longitude

# Print the centroid
print("Centroid: ({}, {})".format(lat, lon))

hex_id = h3.geo_to_h3(lat, lon, 8)


sql_expression_service_hex = """
                                SELECT 
                                    *
                                FROM s3object s
                             """

sr_hex_df = utils.retrieve_csv_file_from_s3(bucket='cct-ds-code-challenge-input-data',
                                            key='sr_hex.csv.gz',
                                            sql_expression=sql_expression_service_hex)

# "In which suburbs should the Water and Sanitation directorate concentrate their infrastructure improvement efforts?".
