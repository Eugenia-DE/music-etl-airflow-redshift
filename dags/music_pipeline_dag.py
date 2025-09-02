from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from data_pipeline_logic import extract_data_task, validate_data_task, transform_and_compute_kpis_task, load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='music_streaming_pipeline_mwaa_v4', 
    start_date=datetime(2023, 1, 1), # A fixed date in the past, good for testing
    schedule_interval=None,         # Set to None for manual triggering during testing
    default_args=default_args,
    catchup=False,
    tags=['music_pipeline', 'etl'],
    doc_md="""
    # Music Streaming Data Pipeline DAG

    This DAG orchestrates the full ETL process for music streaming data,
    including extraction, validation, transformation, KPI computation,
    and loading to Redshift.
    """
) as dag:
    
    # 1. Extract Data Task
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_task,
        provide_context=True,
    )

    # 2. Validate Data Task
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data_task,
        provide_context=True,
    )

    # 3. Transform Data and Compute KPIs Task
    transform_kpis_task = PythonOperator(
        task_id='transform_and_compute_kpis',
        python_callable=transform_and_compute_kpis_task,
        provide_context=True,
    )

    # 4. Load Data to Redshift Task (uses the new load_data function)
    load_to_redshift_task = PythonOperator(
        task_id='load_data_to_redshift',
        python_callable=load_data, # Calls the load_data function in data_pipeline_logic
        provide_context=True,
    )

    # Define the complete task flow
    extract_task >> validate_task >> transform_kpis_task >> load_to_redshift_task