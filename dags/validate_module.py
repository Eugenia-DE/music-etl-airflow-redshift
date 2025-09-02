# validate_module.py - Updated for receiving single batch via mapping

import logging
import pandas as pd
from datetime import datetime

# Assuming s3_utils.py and config.py are in the same or accessible path
from s3_utils import read_s3_object_content, write_s3_object_content
from config import S3_PROCESSED_BUCKET

logger = logging.getLogger(__name__)

def validate_data(**kwargs):
    """
    Reads a single extracted streaming data batch from S3, performs validation and cleaning,
    then writes the validated data back to the S3 processed bucket.
    This function is designed to be mapped over multiple input keys.
    """
    ti = kwargs['ti']
    # This will now be a single S3 key for the current mapped batch instance
    extracted_events_s3_key = kwargs['extracted_events_s3_key'] 

    if not extracted_events_s3_key: 
        logger.info("No extracted streaming event data key provided for validation for this batch. Skipping.")
        ti.xcom_push(key='validated_data_s3_key', value=None) # Push None if this mapped instance skips
        return None

    logger.info(f"Loading extracted data for validation from s3://{S3_PROCESSED_BUCKET}/{extracted_events_s3_key}...")
    
    # Read the CSV content from the provided S3 key
    csv_content = read_s3_object_content(S3_PROCESSED_BUCKET, extracted_events_s3_key)
    df = pd.read_csv(pd.io.common.StringIO(csv_content))

    logger.info(f"Original extracted records for this batch: {len(df)}")

    # --- Validation and Cleaning Steps ---
    critical_columns = ['track_id', 'user_id', 'listen_time']
    df_cleaned = df.dropna(subset=critical_columns).copy() 
    
    if len(df) - len(df_cleaned) > 0:
        logger.warning(f"Dropped {len(df) - len(df_cleaned)} rows due to missing critical values in this batch (from {extracted_events_s3_key}).")
        
    df_cleaned['listen_time'] = pd.to_datetime(df_cleaned['listen_time'], errors='coerce')
    df_cleaned.dropna(subset=['listen_time'], inplace=True) 
    if len(df) - len(df_cleaned) > 0: 
        logger.warning(f"Dropped more rows after invalid 'listen_time' conversion: {len(df) - len(df_cleaned)} (from {extracted_events_s3_key}).")

    df_cleaned['track_id'] = df_cleaned['track_id'].astype(str)
    df_cleaned['user_id'] = df_cleaned['user_id'].astype(str)
    
    original_len = len(df_cleaned)
    df_cleaned.drop_duplicates(inplace=True)
    if original_len - len(df_cleaned) > 0:
        logger.warning(f"Dropped {original_len - len(df_cleaned)} duplicate streaming records in this batch (from {extracted_events_s3_key}).")

    logger.info(f"Validated records for this batch: {len(df_cleaned)}")

    # --- Write Validated Data to S3 Processed Bucket ---
    # Append a unique identifier to the filename to distinguish validated batches
    # We can reuse part of the original extracted S3 key for unique identification
    original_filename_part = extracted_events_s3_key.split('/')[-1] # e.g., batch_0_streaming_events_...csv
    output_s3_key = f"processed/validated_streams/{datetime.now().strftime('%Y/%m/%d')}/validated_{original_filename_part}"

    csv_buffer = pd.io.common.StringIO()
    df_cleaned.to_csv(csv_buffer, index=False)
    write_s3_object_content(S3_PROCESSED_BUCKET, output_s3_key, csv_buffer.getvalue())

    logger.info(f"Validated data for batch saved to s3://{S3_PROCESSED_BUCKET}/{output_s3_key}")

    # Push the S3 key of the validated data for this specific batch
    ti.xcom_push(key='validated_data_s3_key', value=output_s3_key)
    logger.info("Data validation for batch complete.")