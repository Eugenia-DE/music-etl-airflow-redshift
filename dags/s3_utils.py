# s3_utils.py

import boto3
import logging

logger = logging.getLogger(__name__)

def get_s3_client():
    """Returns a Boto3 S3 client."""
    return boto3.client('s3')

def list_s3_objects(bucket_name, prefix=''):
    """Lists objects in an S3 bucket with a given prefix."""
    s3_client = get_s3_client()
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return response.get('Contents', [])
    except Exception as e:
        logger.error(f"Error listing S3 objects in bucket {bucket_name} with prefix {prefix}: {e}")
        return []

def read_s3_object_content(bucket_name, key):
    """Reads the content of an S3 object as a string."""
    s3_client = get_s3_client()
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return response['Body'].read().decode('utf-8')
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"S3 object not found: s3://{bucket_name}/{key}")
        return None
    except Exception as e:
        logger.error(f"Error reading S3 object s3://{bucket_name}/{key}: {e}")
        raise

def write_s3_object_content(bucket_name, key, content):
    """Writes content to an S3 object."""
    s3_client = get_s3_client()
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=content)
        logger.info(f"Successfully wrote content to s3://{bucket_name}/{key}")
    except Exception as e:
        logger.error(f"Error writing to S3 object s3://{bucket_name}/{key}: {e}")
        raise