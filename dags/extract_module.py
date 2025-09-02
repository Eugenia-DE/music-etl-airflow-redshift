# extract_module.py - REVISED for dynamic task mapping of streaming events

import logging
import pandas as pd
from datetime import datetime
from dateutil.parser import parse
import re

# Assuming s3_utils.py and config.py are in the same or accessible path
from s3_utils import read_s3_object_content, list_s3_objects, write_s3_object_content
from config import S3_RAW_BUCKET, S3_PROCESSED_BUCKET, S3_STREAM_MARKER_PATH, S3_METADATA_PATH

logger = logging.getLogger(__name__)

def get_last_processed_timestamp(bucket_name, key):
    """Reads the last processed timestamp from an S3 marker file."""
    try:
        content = read_s3_object_content(bucket_name, key)
        return parse(content.strip())
    except Exception:
        # This log message will appear if the file is genuinely missing or empty
        logger.info(f"Marker file s3://{bucket_name}/{key} not found or empty. Starting from epoch.")
        return datetime.fromtimestamp(0) # Epoch time

def extract_data(**kwargs):
    """
    Extracts new streaming event data incrementally from S3 (batch by batch).
    Also reads and processes static dimension data (songs, users) from S3 raw
    and saves them as Parquet in the processed bucket.
    """
    ti = kwargs['ti']
    current_time = datetime.now()

    # --- 1. Process Streaming Events Incrementally (and prepare for mapping) ---
    last_processed_timestamp = get_last_processed_timestamp(S3_RAW_BUCKET, S3_STREAM_MARKER_PATH)
    logger.info(f"Last processed stream timestamp: {last_processed_timestamp}")

    raw_event_prefix = "raw/events/"
    all_raw_event_objects = list_s3_objects(S3_RAW_BUCKET, raw_event_prefix)

    new_event_files = []
    # Initialize new_max_timestamp with the current last_processed_timestamp.
    # If no new files are found, this ensures the marker doesn't regress.
    new_max_timestamp = last_processed_timestamp 
    
    # Regex to extract timestamp from filename (e.g., streaming_events_2023-01-01_10-00-00.csv)
    timestamp_pattern = re.compile(r'streaming_events_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.csv')

    for obj_key in all_raw_event_objects:
        match = timestamp_pattern.search(obj_key)
        if match:
            file_timestamp_str = match.group(1).replace('_', ' ') # Convert to datetime string
            file_timestamp = parse(file_timestamp_str) # Convert to datetime object
            
            # Use 'file_timestamp > last_processed_timestamp' to avoid reprocessing the exact last file
            if file_timestamp > last_processed_timestamp:
                new_event_files.append(obj_key)
                if file_timestamp > new_max_timestamp:
                    new_max_timestamp = file_timestamp
    
    # Sort files to ensure deterministic processing order if multiple batches appear
    new_event_files.sort()

    extracted_events_output_keys = []
    if not new_event_files:
        logger.info("No new streaming event files found since last run. Skipping extraction of events for mapping.")
    else:
        logger.info(f"Found {len(new_event_files)} new streaming event files to process as separate batches.")
        for i, s3_key in enumerate(new_event_files):
            logger.info(f"Processing new event file: s3://{S3_RAW_BUCKET}/{s3_key}")
            csv_content = read_s3_object_content(S3_RAW_BUCKET, s3_key)
            
            # Define a unique output key for each extracted CSV batch in the processed bucket
            original_filename = s3_key.split('/')[-1]
            extracted_events_output_key = f"processed/extracted_events/{current_time.strftime('%Y/%m/%d')}/batch_{i}_{original_filename}"
            
            # Write raw CSV content directly to the processed bucket as an individual file
            write_s3_object_content(S3_PROCESSED_BUCKET, extracted_events_output_key, csv_content) 
            logger.info(f"Single batch of streaming events saved to s3://{S3_PROCESSED_BUCKET}/{extracted_events_output_key}")
            extracted_events_output_keys.append(extracted_events_output_key)
        
        # Push a list of S3 keys for dynamic task mapping
        ti.xcom_push(key='extracted_events_s3_keys_for_mapping', value=extracted_events_output_keys)
        # Push the maximum timestamp found among new files (for updating the marker later)
        ti.xcom_push(key='new_max_timestamp', value=new_max_timestamp.isoformat())
    
    # If no new event files, ensure the XCom for mapping is an empty list so no mapped tasks are created
    if not extracted_events_output_keys:
        ti.xcom_push(key='extracted_events_s3_keys_for_mapping', value=[])


    # --- 2. Process Dimension Data (Songs and Users - these are UNMAPPED, processed once per DAG run) ---
    # These outputs will be pulled by the mapped transform tasks.
    processed_dimensions_output_path = "processed/dimensions/"

    # Process Songs Data
    songs_raw_s3_key = f"{S3_METADATA_PATH}songs/songs.csv"
    songs_processed_s3_key = f"{processed_dimensions_output_path}songs/songs.parquet"
    
    try:
        logger.info(f"Reading raw songs data from s3://{S3_RAW_BUCKET}/{songs_raw_s3_key}...")
        songs_csv_content = read_s3_object_content(S3_RAW_BUCKET, songs_raw_s3_key)
        df_songs = pd.read_csv(pd.io.common.StringIO(songs_csv_content))
        
        songs_pq_buffer = pd.io.common.BytesIO()
        df_songs.to_parquet(songs_pq_buffer, index=False)
        write_s3_object_content(S3_PROCESSED_BUCKET, songs_processed_s3_key, songs_pq_buffer.getvalue())
        logger.info(f"Processed songs data saved to s3://{S3_PROCESSED_BUCKET}/{songs_processed_s3_key}")
        ti.xcom_push(key='processed_songs_s3_key', value=songs_processed_s3_key)
    except Exception as e:
        logger.error(f"Error processing songs data: {e}. Ensure s3://{S3_RAW_BUCKET}/{songs_raw_s3_key} exists.", exc_info=True)
        ti.xcom_push(key='processed_songs_s3_key', value=None) # Push None if failed


    # Process Users Data
    users_raw_s3_key = f"{S3_METADATA_PATH}users/users.csv"
    users_processed_s3_key = f"{processed_dimensions_output_path}users/users.parquet"
    
    try:
        logger.info(f"Reading raw users data from s3://{S3_RAW_BUCKET}/{users_raw_s3_key}...")
        users_csv_content = read_s3_object_content(S3_RAW_BUCKET, users_raw_s3_key) 
        df_users = pd.read_csv(pd.io.common.StringIO(users_csv_content))
        
        users_pq_buffer = pd.io.common.BytesIO()
        df_users.to_parquet(users_pq_buffer, index=False)
        write_s3_object_content(S3_PROCESSED_BUCKET, users_processed_s3_key, users_pq_buffer.getvalue())
        logger.info(f"Processed users data saved to s3://{S3_PROCESSED_BUCKET}/{users_processed_s3_key}")
        ti.xcom_push(key='processed_users_s3_key', value=users_processed_s3_key)
    except Exception as e:
        logger.error(f"Error processing users data: {e}. Ensure s3://{S3_RAW_BUCKET}/{users_raw_s3_key} exists.", exc_info=True)
        ti.xcom_push(key='processed_users_s3_key', value=None) # Push None if failed

    logger.info("Extraction and initial processing of all raw data complete.")