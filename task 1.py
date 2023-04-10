"""
Task 1
"""

import pandas
import logging
import utils

logger = logging.getLogger()

# log level
logger.setLevel(logging.INFO)


sql_expression_filter_resolution_8 = """
                                       SELECT 
                                           s.type, 
                                           s.properties.index,
                                           s.properties.centroid_lat,
                                           s.properties.centroid_lon,
                                           s.geometry
                                       FROM s3object[*].features[*] s 
                                       WHERE s.properties.resolution = 8
                                    """


sql_expression_resolution_8 = """
                               SELECT 
                                   s.type, 
                                   s.properties.index,
                                   s.properties.centroid_lat,
                                   s.properties.centroid_lon,
                                   s.geometry
                               FROM s3object[*].features[*] s
                              """

resolution_8_filtered_df = utils.retrieve_json_file_from_s3(bucket='cct-ds-code-challenge-input-data',
                                                            key='city-hex-polygons-8-10.geojson',
                                                            sql_expression=sql_expression_filter_resolution_8)

logger.info(f"Resolution 8 filtered dataframe is {resolution_8_filtered_df.shape}")

resolution_8_df = utils.retrieve_json_file_from_s3(bucket='cct-ds-code-challenge-input-data',
                                                   key='city-hex-polygons-8.geojson',
                                                   sql_expression=sql_expression_resolution_8)

logger.info(f"Resolution 8 dataframe is {resolution_8_df.shape}")
logger.info((f"Are the two pandas dataframe equal {pandas.DataFrame.equals(resolution_8_filtered_df, resolution_8_df)}"))


