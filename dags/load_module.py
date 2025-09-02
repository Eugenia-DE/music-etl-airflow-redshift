# load_module.py - REVISED for collecting mapped KPI outputs

import logging
import pandas as pd
from datetime import datetime

# Assuming redshift_utils.py, s3_utils.py and config.py are in the same or accessible path
from redshift_utils import execute_redshift_sql, copy_to_redshift
from s3_utils import write_s3_object_content, read_s3_object_content
from config import S3_RAW_BUCKET, S3_KPI_BUCKET, S3_STREAM_MARKER_PATH

logger = logging.getLogger(__name__)

def load_data(**kwargs):
    """
    Collects transformed KPI data from all mapped upstream tasks, combines it,
    and loads into Redshift staging and fact tables.
    Also updates the stream marker in S3.
    """
    ti = kwargs['ti']
    # Pull ALL XComs from the mapped 'transform_and_compute_kpis' task.
    # Airflow's xcom_pull behavior for mapped tasks is to return a list of all successful outputs.
    all_genre_kpis_s3_keys = ti.xcom_pull(task_ids='transform_and_compute_kpis', key='genre_kpis_s3_key')
    all_hourly_kpis_s3_keys = ti.xcom_pull(task_ids='transform_and_compute_kpis', key='hourly_kpis_s3_key')
    
    # The new_max_timestamp is from the UNMAPPED 'extract_data' task, so pull it directly.
    new_max_timestamp_str = ti.xcom_pull(task_ids='extract_data', key='new_max_timestamp')

    # Filter out None values that might come from mapped tasks that skipped (e.g., if a batch was empty)
    valid_genre_kpis_s3_keys = [k for k in all_genre_kpis_s3_keys if k is not None]
    valid_hourly_kpis_s3_keys = [k for k in all_hourly_kpis_s3_keys if k is not None]

    if not valid_genre_kpis_s3_keys and not valid_hourly_kpis_s3_keys:
        logger.info("No valid KPI data found from any batch to load to Redshift. Skipping data loading.")
        if new_max_timestamp_str: # Still update marker if extraction found new files, even if no KPIs computed (e.g. empty batch)
            update_stream_marker(S3_RAW_BUCKET, S3_STREAM_MARKER_PATH, new_max_timestamp_str)
        return None

    # --- 0. Combine all KPI dataframes from different batches into single DataFrames ---
    combined_genre_kpis_df = pd.DataFrame()
    for s3_key in valid_genre_kpis_s3_keys:
        logger.info(f"Reading genre KPIs from s3://{S3_KPI_BUCKET}/{s3_key} for combination...")
        pq_content = read_s3_object_content(S3_KPI_BUCKET, s3_key)
        df_batch = pd.read_parquet(pd.io.common.BytesIO(pq_content))
        combined_genre_kpis_df = pd.concat([combined_genre_kpis_df, df_batch], ignore_index=True)
    
    # IMPORTANT: If multiple batches can have the same genre, we need to aggregate the KPIs
    # Summing unique_users and unique_songs across batches can overcount if a user/song appears in multiple batches.
    # For a simple aggregate, sum is often chosen, but for true "unique" count across *all* batches,
    # you'd need the original detailed streaming data or more complex aggregation logic.
    # For now, we'll sum, as it's a common simplification for incremental data.
    if not combined_genre_kpis_df.empty:
        combined_genre_kpis_df = combined_genre_kpis_df.groupby('track_genre').agg(
            total_streams=('total_streams', 'sum'),
            unique_users=('unique_users', 'sum'), 
            unique_songs=('unique_songs', 'sum'), 
            avg_track_duration_ms=('avg_track_duration_ms', 'mean'), # Average of averages
            popularity_index=('popularity_index', 'sum')
        ).reset_index().rename(columns={'track_genre': 'genre'}) # Rename for Redshift table consistency
    else:
        logger.info("No genre KPIs to combine.")


    combined_hourly_kpis_df = pd.DataFrame()
    for s3_key in valid_hourly_kpis_s3_keys:
        logger.info(f"Reading hourly KPIs from s3://{S3_KPI_BUCKET}/{s3_key} for combination...")
        pq_content = read_s3_object_content(S3_KPI_BUCKET, s3_key)
        df_batch = pd.read_parquet(pd.io.common.BytesIO(pq_content))
        combined_hourly_kpis_df = pd.concat([combined_hourly_kpis_df, df_batch], ignore_index=True)

    # Perform aggregation for hourly KPIs
    if not combined_hourly_kpis_df.empty:
        combined_hourly_kpis_df = combined_hourly_kpis_df.groupby('listen_hour').agg(
            total_streams=('total_streams', 'sum'),
            unique_users=('unique_users', 'sum'),
            unique_songs_hourly=('unique_songs_hourly', 'sum')
        ).reset_index()
    else:
        logger.info("No hourly KPIs to combine.")


    # Define a single S3 path for the combined KPIs to load to Redshift
    combined_kpis_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    combined_genre_kpis_load_s3_key = None
    combined_hourly_kpis_load_s3_key = None

    # Write combined KPIs back to S3 for Redshift COPY
    if not combined_genre_kpis_df.empty:
        combined_genre_kpis_load_s3_key = f"kpis/combined_for_redshift/genre_kpis_combined_{combined_kpis_timestamp}.parquet"
        genre_pq_buffer = pd.io.common.BytesIO()
        combined_genre_kpis_df.to_parquet(genre_pq_buffer, index=False)
        write_s3_object_content(S3_KPI_BUCKET, combined_genre_kpis_load_s3_key, genre_pq_buffer.getvalue())
        logger.info(f"Combined Genre KPIs uploaded to s3://{S3_KPI_BUCKET}/{combined_genre_kpis_load_s3_key}")

    if not combined_hourly_kpis_df.empty:
        combined_hourly_kpis_load_s3_key = f"kpis/combined_for_redshift/hourly_kpis_combined_{combined_kpis_timestamp}.parquet"
        hourly_pq_buffer = pd.io.common.BytesIO()
        combined_hourly_kpis_df.to_parquet(hourly_pq_buffer, index=False)
        write_s3_object_content(S3_KPI_BUCKET, combined_hourly_kpis_load_s3_key, hourly_pq_buffer.getvalue())
        logger.info(f"Combined Hourly KPIs uploaded to s3://{S3_KPI_BUCKET}/{combined_hourly_kpis_load_s3_key}")


    # --- Redshift Loading Steps ---

    # 1. Create Staging Tables for KPIs (if not exists)
    create_stg_genre_kpis_sql = """
        CREATE TABLE IF NOT EXISTS stg_genre_kpis (
            genre VARCHAR(256),
            total_streams BIGINT,
            unique_users BIGINT,
            unique_songs BIGINT,
            avg_track_duration_ms DOUBLE PRECISION,
            popularity_index BIGINT
        );
    """
    create_stg_hourly_kpis_sql = """
        CREATE TABLE IF NOT EXISTS stg_hourly_kpis (
            listen_hour INTEGER,
            total_streams BIGINT,
            unique_users BIGINT,
            unique_songs_hourly BIGINT
        );
    """
    execute_redshift_sql(create_stg_genre_kpis_sql)
    execute_redshift_sql(create_stg_hourly_kpis_sql)
    logger.info("KPI staging tables checked/created.")


    # 2. Create Fact Tables for KPIs (if not exists)
    create_fact_genre_kpis_sql = """
        CREATE TABLE IF NOT EXISTS fact_genre_kpis (
            genre VARCHAR(256) PRIMARY KEY,
            total_streams BIGINT,
            unique_users BIGINT,
            unique_songs BIGINT,
            avg_track_duration_ms DOUBLE PRECISION,
            popularity_index BIGINT,
            last_updated TIMESTAMP DEFAULT GETDATE()
        )
        DISTKEY(genre)
        SORTKEY(last_updated);
    """
    create_fact_hourly_kpis_sql = """
        CREATE TABLE IF NOT EXISTS fact_hourly_kpis (
            listen_hour INTEGER PRIMARY KEY,
            total_streams BIGINT,
            unique_users BIGINT,
            unique_songs_hourly BIGINT,
            last_updated TIMESTAMP DEFAULT GETDATE()
        )
        DISTSTYLE ALL
        SORTKEY(listen_hour);
    """
    execute_redshift_sql(create_fact_genre_kpis_sql)
    execute_redshift_sql(create_fact_hourly_kpis_sql)
    logger.info("KPI fact tables checked/created.")


    # 3. Truncate Staging Tables before loading new data
    execute_redshift_sql("TRUNCATE TABLE stg_genre_kpis;")
    execute_redshift_sql("TRUNCATE TABLE stg_hourly_kpis;")
    logger.info("Staging tables truncated.")

    # 4. Load KPIs to Staging Tables from S3 (combined files)
    if combined_genre_kpis_load_s3_key:
        logger.info(f"Loading combined genre KPIs from s3://{S3_KPI_BUCKET}/{combined_genre_kpis_load_s3_key} to stg_genre_kpis...")
        copy_to_redshift(f"{S3_KPI_BUCKET}/{combined_genre_kpis_load_s3_key}", "stg_genre_kpis")
        logger.info("Combined Genre KPIs loaded to staging.")

    if combined_hourly_kpis_load_s3_key:
        logger.info(f"Loading combined hourly KPIs from s3://{S3_KPI_BUCKET}/{combined_hourly_kpis_load_s3_key} to stg_hourly_kpis...")
        copy_to_redshift(f"{S3_KPI_BUCKET}/{combined_hourly_kpis_load_s3_key}", "stg_hourly_kpis")
        logger.info("Combined Hourly KPIs loaded to staging.")

    # 5. Upsert from Staging to Fact Tables
    # Genre KPIs Upsert
    if combined_genre_kpis_load_s3_key: # Only run if there was data to load
        upsert_genre_kpis_sql = f"""
            BEGIN;
            DELETE FROM fact_genre_kpis
            USING stg_genre_kpis
            WHERE fact_genre_kpis.genre = stg_genre_kpis.genre;

            INSERT INTO fact_genre_kpis (genre, total_streams, unique_users, unique_songs, avg_track_duration_ms, popularity_index, last_updated)
            SELECT genre, total_streams, unique_users, unique_songs, avg_track_duration_ms, popularity_index, GETDATE()
            FROM stg_genre_kpis;
            COMMIT;
        """
        execute_redshift_sql(upsert_genre_kpis_sql)
        logger.info("Genre KPIs upserted to fact_genre_kpis.")
    else:
        logger.info("No combined genre KPIs to upsert.")


    # Hourly KPIs Upsert
    if combined_hourly_kpis_load_s3_key: # Only run if there was data to load
        upsert_hourly_kpis_sql = f"""
            BEGIN;
            DELETE FROM fact_hourly_kpis
            USING stg_hourly_kpis
            WHERE fact_hourly_kpis.listen_hour = stg_hourly_kpis.listen_hour;

            INSERT INTO fact_hourly_kpis (listen_hour, total_streams, unique_users, unique_songs_hourly, last_updated)
            SELECT listen_hour, total_streams, unique_users, unique_songs_hourly, GETDATE()
            FROM stg_hourly_kpis;
            COMMIT;
        """
        execute_redshift_sql(upsert_hourly_kpis_sql)
        logger.info("Hourly KPIs upserted to fact_hourly_kpis.")
    else:
        logger.info("No combined hourly KPIs to upsert.")


    # --- 6. Update Stream Marker ---
    if new_max_timestamp_str: # Only update if new files were processed by extract_data
        update_stream_marker(S3_RAW_BUCKET, S3_STREAM_MARKER_PATH, new_max_timestamp_str)
    else:
        logger.info("No new maximum timestamp to update the stream marker with.")
    
    logger.info("Data loading to Redshift complete.")


def update_stream_marker(bucket_name, key, timestamp_str):
    """
    Updates the S3 marker file with the latest processed timestamp.
    """
    try:
        write_s3_object_content(bucket_name, key, timestamp_str)
        logger.info(f"Stream marker updated to {timestamp_str} at s3://{bucket_name}/{key}")
    except Exception as e:
        logger.error(f"Error updating stream marker at s3://{bucket_name}/{key}: {e}")
        raise