# music_streaming_pipeline_mwaa_v7_dag.py

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import modules containing our ETL logic
# Ensure these modules are accessible to Airflow (e.g., in a plugins folder or the same dags folder)
from extract_module import extract_data
from validate_module import validate_data
from transform_module import transform_and_compute_kpis
from load_module import load_data

@dag(
    dag_id='music_streaming_etl_pipeline_v7', 
    start_date=days_ago(1),
    schedule_interval='@hourly', 
    catchup=False, 
    tags=['music', 'etl', 'streaming', 'kpis', 's3-data-lake', 'dynamic-mapping'],
    doc_md="""
    ### Music Streaming ETL Pipeline v7 (S3 Data Lake Focused - Dynamic Batch Processing)

    This DAG processes music streaming data in a batch-by-batch fashion using dynamic task mapping.

    1.  **Extract:**
        * Incremental extraction of new streaming events from S3 Raw, saving each new event file as a separate batch in S3 Processed.
        * Reads and processes (to Parquet) static dimension data (songs, users) from S3 Raw, saving them to S3 Processed.
        * Pushes a list of S3 keys for new streaming event batches for mapping.
    2.  **Validate (Mapped):** For each streaming event batch, cleans and validates the data, saving to S3 Processed.
    3.  **Transform & Compute KPIs (Mapped):** For each validated batch, enriches data using processed dimensions (from extract_data) and calculates KPIs, saving them to S3 KPI bucket.
    4.  **Load Data (Unmapped):** Collects all computed KPIs from *all* mapped transformation tasks, combines them, loads into Redshift fact tables, and updates the processing marker.
    """
)
def music_streaming_etl_pipeline_v7():

    # Task to extract new streaming data AND process dimension data (Unmapped)
    # This task will now push a list of S3 keys for new streaming event batches to XCom
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        # provide_context is implicitly True for @dag decorated tasks in Airflow 2.2+,
        # but explicitly adding it doesn't hurt for clarity or older versions.
        provide_context=True, 
    )

    # Task to validate the extracted streaming data (Dynamically Mapped)
    # It maps over the list of S3 keys produced by extract_data_task
    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        # The 'op_kwargs' are passed to each mapped instance.
        # 'extracted_events_s3_keys_for_mapping' must be the XCom key from 'extract_data_task'
        # Airflow automatically maps over this list when .expand() is used.
        op_kwargs={
            'extracted_events_s3_key': extract_data_task.output # Airflow passes individual elements of the list here
        },
        provide_context=True,
    )

    # Task to transform data and compute KPIs (Dynamically Mapped)
    # It maps over the outputs of the mapped validate_data_task instances
    transform_and_compute_kpis_task = PythonOperator(
        task_id='transform_and_compute_kpis',
        python_callable=transform_and_compute_kpis,
        # 'validated_data_s3_key' now comes from the mapped 'validate_data_task'
        op_kwargs={
            'validated_data_s3_key': validate_data_task.output # Airflow passes individual elements of the list here
        },
        provide_context=True,
    )

    # Task to load the transformed data (KPIs) into Redshift (Unmapped)
    # This task will run only once after all mapped transform_and_compute_kpis tasks complete.
    # It collects all XComs from all instances of 'transform_and_compute_kpis_task'.
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        # Set trigger rule to 'all_done' to ensure it runs even if some mapped tasks were skipped
        # (e.g., if a batch was empty after validation)
        trigger_rule='all_done', 
        provide_context=True,
    )

    # Define the task dependencies
    # extract_data_task runs first. Its output (a list) is expanded across validate_data_task.
    # validate_data_task's output (a list of individual outputs) is expanded across transform_and_compute_kpis_task.
    # load_data_task depends on ALL instances of transform_and_compute_kpis_task.
    extract_data_task >> validate_data_task >> transform_and_compute_kpis_task
    transform_and_compute_kpis_task >> load_data_task

# Instantiate the DAG
music_streaming_etl_pipeline_v7()