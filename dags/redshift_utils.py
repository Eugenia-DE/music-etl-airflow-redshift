# redshift_utils.py

import logging
# Change this import:
# from airflow.providers.amazon.aws.hooks.redshift import RedshiftHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook # Use RedshiftSQLHook instead
from config import REDSHIFT_CONN_ID, REDSHIFT_COPY_IAM_ROLE

logger = logging.getLogger(__name__)

def get_redshift_hook():
    """Returns a RedshiftSQLHook instance."""
    # Change this instantiation:
    # return RedshiftHook(REDSHIFT_CONN_ID)
    return RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID) # Use RedshiftSQLHook

def execute_redshift_sql(sql_query):
    """Executes a SQL query on Redshift."""
    redshift_hook = get_redshift_hook()
    try:
        logger.info(f"Executing SQL query:\n{sql_query}")
        redshift_hook.run(sql_query)
        logger.info("SQL query executed successfully.")
    except Exception as e:
        logger.error(f"Error executing Redshift SQL query: {e}")
        raise

def copy_to_redshift(s3_key, table_name, file_format='PARQUET', options=''):
    """
    Copies data from S3 to a Redshift table using the COPY command.
    Assumes s3_key is the full path (bucket_name/key_prefix).
    """
    redshift_hook = get_redshift_hook()
    # Construct the full S3 path including bucket
    full_s3_path = f"s3://{s3_key}"

    copy_sql = f"""
        COPY {table_name}
        FROM '{full_s3_path}'
        IAM_ROLE '{REDSHIFT_COPY_IAM_ROLE}'
        FORMAT AS {file_format}
        {options};
    """
    try:
        logger.info(f"Executing Redshift COPY command for table {table_name} from {full_s3_path}...")
        redshift_hook.run(copy_sql) # RedshiftSQLHook also has a .run() method for arbitrary SQL
        logger.info(f"Data successfully copied to {table_name}.")
    except Exception as e:
        logger.error(f"Error copying data to Redshift table {table_name} from {full_s3_path}: {e}")
        raise

def fetch_data_from_redshift(sql_query):
    """
    Fetches data from Redshift using a SQL query and returns it as a Pandas DataFrame.
    """
    redshift_hook = get_redshift_hook()
    try:
        logger.info(f"Fetching data from Redshift with query:\n{sql_query}")
        # RedshiftSQLHook's get_pandas_df method fetches data directly into a DataFrame
        df = redshift_hook.get_pandas_df(sql_query)
        logger.info(f"Successfully fetched {len(df)} rows from Redshift.")
        return df
    except Exception as e:
        logger.error(f"Error fetching data from Redshift: {e}")
        raise