# transform_module.py - REVISED for dynamic task mapping and fixed dim lookup

import logging
import pandas as pd
from datetime import datetime

# Assuming s3_utils.py and config.py are in the same or accessible path
from s3_utils import read_s3_object_content, write_s3_object_content
from config import S3_PROCESSED_BUCKET, S3_KPI_BUCKET

logger = logging.getLogger(__name__)

def transform_and_compute_kpis(**kwargs):
    """
    Transforms a single batch of validated streaming data by joining with dimension data from S3,
    and computes comprehensive genre-level and hourly KPIs.
    Uploads KPIs as Parquet files to S3.
    This function is designed to be mapped over multiple input keys.
    """
    ti = kwargs['ti']
    # This will be a single S3 key for the current mapped batch instance
    validated_data_s3_key = kwargs['validated_data_s3_key']

    # These are pulled from the UNMAPPED 'extract_data' task.
    # Airflow ensures that even when a task is mapped, it can pull XComs from an unmapped upstream task.
    processed_songs_s3_key = ti.xcom_pull(task_ids='extract_data', key='processed_songs_s3_key')
    processed_users_s3_key = ti.xcom_pull(task_ids='extract_data', key='processed_users_s3_key')

    if not validated_data_s3_key:
        logger.info("No validated streaming event data for this batch to transform. Skipping transformation and KPI computation.")
        ti.xcom_push(key='genre_kpis_s3_key', value=None) # Push None if this mapped instance skips
        ti.xcom_push(key='hourly_kpis_s3_key', value=None) # Push None if this mapped instance skips
        return None

    # --- Load Validated Streaming Data from S3 Processed bucket ---
    logger.info(f"Loading validated streaming data for transformation from s3://{S3_PROCESSED_BUCKET}/{validated_data_s3_key}...")
    csv_content = read_s3_object_content(S3_PROCESSED_BUCKET, validated_data_s3_key)
    df_streams = pd.read_csv(pd.io.common.StringIO(csv_content))

    # Data Cleaning and Preparation for streams (still good to have here for robust transformation)
    df_streams['listen_time'] = pd.to_datetime(df_streams['listen_time'])
    df_streams.drop_duplicates(inplace=True) # Safeguard against any unexpected duplicates
    logger.info(f"Streaming data after dropping duplicates for this batch: {len(df_streams)} records.")

    # --- Fetch Dimension Data from S3 Processed (Parquet format) ---
    if not processed_songs_s3_key or not processed_users_s3_key:
        logger.error("Processed dimension S3 keys not found from 'extract_data'. Cannot perform dimension lookup.")
        # Raise an error to fail the task if critical dependencies are missing
        raise ValueError("Processed songs or users data missing from extract_data output.")
    
    logger.info(f"Fetching processed dim_songs from s3://{S3_PROCESSED_BUCKET}/{processed_songs_s3_key}...")
    songs_pq_content = read_s3_object_content(S3_PROCESSED_BUCKET, processed_songs_s3_key)
    df_dim_songs = pd.read_parquet(pd.io.common.BytesIO(songs_pq_content))
    logger.info(f"Fetched {len(df_dim_songs)} records from S3 for dim_songs.")

    logger.info(f"Fetching processed dim_users from s3://{S3_PROCESSED_BUCKET}/{processed_users_s3_key}...")
    users_pq_content = read_s3_object_content(S3_PROCESSED_BUCKET, processed_users_s3_key)
    df_dim_users = pd.read_parquet(pd.io.common.BytesIO(users_pq_content))
    logger.info(f"Fetched {len(df_dim_users)} records from S3 for dim_users.")

    # --- Enrich Streaming Data by Joining with Dimensions ---
    enriched_df = pd.merge(df_streams, df_dim_songs, on='track_id', how='left')
    enriched_df['artists'].fillna('Unknown', inplace=True)
    enriched_df['track_name'].fillna('Unknown', inplace=True)
    enriched_df['track_genre'].fillna('Unknown', inplace=True)
    enriched_df['duration_ms'].fillna(0, inplace=True)

    enriched_df = pd.merge(enriched_df, df_dim_users, on='user_id', how='left')
    enriched_df['user_name'].fillna('Unknown User', inplace=True)
    enriched_df['user_country'].fillna('Unknown Country', inplace=True)
    enriched_df['user_age'].fillna(-1, inplace=True)

    logger.info(f"Enriched streaming data records for this batch after joins: {len(enriched_df)}")

    # --- KPI 1: Genre-Level KPIs ---
    logger.info("Computing Genre-Level KPIs for this batch...")
    genre_kpis = enriched_df.groupby('track_genre').agg(
        total_streams=('track_id', 'count'),
        unique_users=('user_id', 'nunique'),
        unique_songs=('track_id', 'nunique'),
        avg_track_duration_ms=('duration_ms', 'mean')
    ).reset_index()
    
    genre_kpis['popularity_index'] = genre_kpis['total_streams'] 
    logger.info(f"Genre KPIs rows for this batch: {len(genre_kpis)}")

    # --- KPI 2: Hourly KPIs ---
    logger.info("Computing Hourly KPIs for this batch...")
    enriched_df['listen_hour'] = enriched_df['listen_time'].dt.hour
    hourly_kpis = enriched_df.groupby('listen_hour').agg(
        total_streams=('track_id', 'count'),
        unique_users=('user_id', 'nunique'),
        unique_songs_hourly=('track_id', 'nunique')
    ).reset_index()
    logger.info(f"Hourly KPIs rows for this batch: {len(hourly_kpis)}")

    # Define S3 paths for KPIs (unique per batch)
    # The batch identifier will ensure unique KPI files for each mapped task instance.
    # We extract the batch identifier from the validated_data_s3_key to maintain consistency.
    original_filename_part = validated_data_s3_key.split('/')[-1] # e.g., validated_batch_0_streaming_events_...csv
    # Remove the 'validated_' prefix and '.csv' suffix for cleaner KPI file naming
    batch_identifier = original_filename_part.replace('validated_', '').replace('.csv', '.parquet') 
    processing_date_path = datetime.now().strftime('%Y/%m/%d')
    
    genre_kpis_s3_key = f"kpis/genre_kpis/{processing_date_path}/genre_kpis_{batch_identifier}"
    hourly_kpis_s3_key = f"kpis/hourly_kpis/{processing_date_path}/hourly_kpis_{batch_identifier}"

    # Upload to S3 as Parquet
    genre_pq_buffer = pd.io.common.BytesIO()
    genre_kpis.to_parquet(genre_pq_buffer, index=False)
    write_s3_object_content(S3_KPI_BUCKET, genre_kpis_s3_key, genre_pq_buffer.getvalue())
    logger.info(f"Genre KPIs for batch uploaded to s3://{S3_KPI_BUCKET}/{genre_kpis_s3_key}")

    hourly_pq_buffer = pd.io.common.BytesIO()
    hourly_kpis.to_parquet(hourly_pq_buffer, index=False)
    write_s3_object_content(S3_KPI_BUCKET, hourly_kpis_s3_key, hourly_pq_buffer.getvalue())
    logger.info(f"Hourly KPIs for batch uploaded to s3://{S3_KPI_BUCKET}/{hourly_kpis_s3_key}")

    # Push S3 keys for this specific batch's KPIs
    ti.xcom_push(key='genre_kpis_s3_key', value=genre_kpis_s3_key)
    ti.xcom_push(key='hourly_kpis_s3_key', value=hourly_kpis_s3_key)
    
    logger.info("Transformation and KPI computation for batch complete.")