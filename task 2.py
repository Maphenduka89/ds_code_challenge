"""
Task 2
"""

import time
import pandas
import logging
import utils
import h3


logger = logging.getLogger()

# log level
logger.setLevel(logging.INFO)

# Define error threshold
JOIN_ERROR_THRESHOLD = 0.05  # 5% of total records
# Threshold value is chosen to balance the tradeoff between the risk of missing critical data and the risk of wasting
# computational resources on problematic data. The optimal threshold will depend on the specific use case and data.

start_time = time.time()

sql_expression_service = """
                           SELECT 
                               s.notification_number,
                               s.reference_number,
                               s.creation_timestamp,
                               s.completion_timestamp,
                               s.directorate,
                               s.department,
                               s.branch,
                               s."section",
                               s.code_group,
                               s.code,
                               s.cause_code_group,
                               s.cause_code,
                               s.official_suburb,
                               s.latitude,
                               s.longitude
                           FROM s3object s
                          """

sql_expression_resolution_8 = """
                               SELECT 
                                   s.properties.index,
                                   s.properties.centroid_lat,
                                   s.properties.centroid_lon
                               FROM s3object[*].features[*] s
                              """
resolution_8_df = utils.retrieve_json_file_from_s3(bucket='cct-ds-code-challenge-input-data',
                                                   key='city-hex-polygons-8.geojson',
                                                   sql_expression=sql_expression_resolution_8)

logger.info(f"Resolution 8 dataframe is {resolution_8_df.shape}")

sr_df = utils.retrieve_csv_file_from_s3(bucket='cct-ds-code-challenge-input-data',
                                        key='sr.csv.gz',
                                        sql_expression=sql_expression_service)

# Join service requests with hexagons
logger.info("Joining service requests with hexagons...")
sr_df[['latitude', 'longitude']] = sr_df[['latitude', 'longitude']].apply(pandas.to_numeric, errors='coerce')
sr_df['latitude'] = sr_df['latitude'].astype(float)
sr_df['longitude'] = sr_df['longitude'].astype(float)

sr_df['h3_level8_index'] = sr_df.apply(lambda row: h3.geo_to_h3(lat = row["latitude"], lng = row["longitude"], resolution = 8), axis = 1)

resolution_8_df['centroid_lat'] = resolution_8_df['centroid_lat'].astype(float)
resolution_8_df['centroid_lon'] = resolution_8_df['centroid_lon'].astype(float)

joined_df = pandas.merge(sr_df,
                         resolution_8_df,
                         how='left',
                         left_on=['h3_level8_index'],
                         right_on='index',
                         indicator=True)

# Count the number of failed joins
num_failed_joins = len(joined_df[joined_df['index_right'].isnull()])

# Calculate the join failure rate
join_failure_rate = num_failed_joins / len(sr_df)

# Log the join failure rate and the number of failed joins
logger.info(f"Join failure rate: {join_failure_rate:.2%}")
logger.info(f"Number of failed joins: {num_failed_joins}")

# Check if the join failure rate exceeds the error threshold
if join_failure_rate > JOIN_ERROR_THRESHOLD:
    logger.error(f"Join failure rate exceeds threshold of {JOIN_ERROR_THRESHOLD:.2%}. Aborting.")

end_time = time.time()
logger.info(f"Time taken: {end_time - start_time:.2f} seconds")
