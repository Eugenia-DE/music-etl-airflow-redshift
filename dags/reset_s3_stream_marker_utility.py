from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from marker_utilities import reset_stream_marker_to_epoch 


with DAG(
    dag_id='reset_s3_stream_marker_to_epoch_utility',
    start_date=datetime(2023, 1, 1), 
    schedule_interval=None, 
    catchup=False, 
    tags=['utility', 's3', 'marker', 'backfill', 'full-load'],
    doc_md="""
    ### Reset S3 Stream Marker to Epoch Utility DAG

    This action will force the main data pipeline to reprocess ALL historical data
    it finds in the raw S3 bucket from the very beginning.

    - Initial full data load.
    - Full historical backfill.
    - When existing data needs to be reprocessed entirely.
    """
) as dag:
    reset_marker_task = PythonOperator(
        task_id='reset_marker_to_epoch',
        python_callable=reset_stream_marker_to_epoch,
    )