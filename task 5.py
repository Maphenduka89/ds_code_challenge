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

# Step 5: Filter the DataFrame to select only those service requests within 1 minute of the suburb centroid
subsample = sr_hex_df[(sr_hex_df['h3_level8_index'] == hex_id)]

print(subsample.shape)

wind_speed_df = pandas.read_excel(requests.get('https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods').content, header=2)

wind_speed_df["Date & Time"] = pandas.to_datetime(wind_speed_df["Date & Time"], errors='coerce')
wind_speed_df = wind_speed_df[wind_speed_df["Date & Time"].notnull()]

print(wind_speed_df.shape)

sr_hex_df['creation_timestamp'] = pandas.to_datetime(sr_hex_df['creation_timestamp'])
sr_hex_df['creation_timestamp'] = sr_hex_df['creation_timestamp'].dt.round('6H')
print(sr_hex_df.head())
augmented_data = pandas.merge_asof(sr_hex_df.sort_values('creation_timestamp'), wind_speed_df,
                                   left_on='creation_timestamp',
                                   right_on='Date & Time',
                                   direction='nearest',
                                   tolerance=pandas.Timedelta('1H'))

print(augmented_data.shape)

# Replace the latitude and longitude with a H3 index with resolution 9
augmented_data['h3_index'] = augmented_data.apply(lambda row: h3.geo_to_h3(row['latitude'], row['longitude'], 9), axis=1)
augmented_data.drop(['latitude', 'longitude'], axis=1, inplace=True)
