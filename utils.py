"""
Utilities
"""

import ast
import boto3
from botocore.exceptions import ClientError
import logging
import pandas
import requests
import time
from io import StringIO

logger = logging.getLogger('coct')


def boto_s3_client():
    """
    Boto client
    """
    response = requests.get("https://cct-ds-code-challenge-input-data.s3.af-south-1.amazonaws.com/ds_code_challenge_creds.json").json()

    return boto3.client('s3',
                        aws_access_key_id=response['s3']['access_key'],
                        aws_secret_access_key=response['s3']['secret_key'],
                        region_name='af-south-1')


def get_s3_file_size(bucket: str, key: str) -> int:
    """
    Gets the file size of S3 object by a HEAD request
    :param bucket: S3 bucket
    :param key: S3 object path
    :return: File size in bytes. Defaults to 0 if any error.
    """

    s3_client = boto_s3_client()
    file_size = 0
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        if response:
            file_size = int(response.get('ResponseMetadata').get('HTTPHeaders').get('content-length'))
    except ClientError:
        logger.exception(f'Client error reading S3 file {bucket} : {key}')
    return file_size


def retrieve_csv_file_from_s3(bucket: str,
                              key: str,
                              sql_expression: str):

    """
    Retrieves S3 file content via S3 Select ScanRange and store it in a local file.
    :param bucket: S3 bucket
    :param key: S3 key
    :return:
    """

    s3_client = boto_s3_client()
    result_stream = []

    # fetch file size via S3 HEAD
    file_size = get_s3_file_size(bucket=bucket, key=key)
    if not file_size:
        logger.error(f'File size invalid {file_size}')
        raise Exception("Sorry, file may be invalid")

    try:
        response = s3_client.select_object_content(
            Bucket=bucket,
            Key=key,
            ExpressionType='SQL',
            Expression=sql_expression,
            InputSerialization={
                'CSV': {
                    'FileHeaderInfo': 'USE',
                    'RecordDelimiter': '\n',
                    "FieldDelimiter": ",",
                    # 'AllowQuotedRecordDelimiter': True
                },
                "CompressionType": "GZIP"
            },
            OutputSerialization={
                'JSON': {
                    'RecordDelimiter': ',',
                },
            },
        )
        for event in response['Payload']:
            if 'Records' in event:
                try:
                    result_stream.append(event['Records']['Payload'].decode('utf-8'))
                    result_stream = ast.literal_eval(''.join(result_stream))
                except:
                    pass
    except ClientError:
        logger.exception(f'Client error reading S3 file {bucket} : {key}')
    except Exception:
        logger.exception(f'Error reading S3 file {bucket} : {key}')
    # Split each string into a list of values and remove quotes
    return pandas.DataFrame.from_dict(result_stream)


def retrieve_json_file_from_s3(bucket: str,
                               key: str,
                               sql_expression: str):
    """
    Retrieves S3 file content via S3 Select ScanRange and store it in a local file.
    :param bucket: S3 bucket
    :param key: S3 key
    :param sql_expression: SQL expression
    :return:
    """

    # Start timer
    start_time = time.time()

    s3_client = boto_s3_client()
    expression = sql_expression
    result_stream = []

    # fetch file size via S3 HEAD
    file_size = get_s3_file_size(bucket=bucket, key=key)
    if not file_size:
        logger.error(f'File size invalid {file_size}')
        raise Exception("Sorry, file may be invalid")

    logger.info(f'File size about {file_size} bytes')

    try:
        response = s3_client.select_object_content(
            Bucket=bucket,
            Key=key,
            ExpressionType='SQL',
            Expression=expression,
            InputSerialization={
                'JSON': {
                    'Type': 'DOCUMENT'
                }
            },
            OutputSerialization={
                'JSON': {
                    'RecordDelimiter': ',',
                },
            }
        )

        """
        select_object_content() response is an event stream which can be looped to concatenate the overall result set
        """
        for event in response['Payload']:
            if 'Records' in event:
                result_stream.append(event['Records']['Payload'].decode('utf-8'))
                try:
                    result_stream = ast.literal_eval(''.join(result_stream))
                except Exception as e:
                    logging.error(f'Error processing results: {str(e)}')
                    pass
    except ClientError:
        logger.exception(f'Client error reading S3 file {bucket} : {key}')
    except Exception:
        logger.exception(f'Error reading S3 file {bucket} : {key}')

        # Stop timer and calculate execution time
        end_time = time.time()
        execution_time = end_time - start_time

        # Log success message with execution time
        logging.info(f'S3 SELECT query succeeded in {execution_time:.2f} seconds')

    return pandas.DataFrame.from_dict(result_stream)

