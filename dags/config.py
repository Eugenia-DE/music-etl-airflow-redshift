# AWS S3 Bucket Names
S3_RAW_BUCKET = "music-streaming-data-bucks"
S3_PROCESSED_BUCKET = "music-streaming-processed-bucks"
S3_KPI_BUCKET = "music-streaming-kpis"

# S3 Paths within the S3_RAW_BUCKET
S3_STREAMING_PATH = "raw/streaming/" 
S3_METADATA_PATH = "raw/metadata/"   

# S3 Path for the incremental stream marker and temporary extracted data (within S3_RAW_BUCKET)
S3_STREAM_MARKER_PATH = "marker/last_processed_stream_timestamp.txt"
S3_TEMP_EXTRACTED_PATH = "temp/extracted_new_streams.csv"

# Redshift Connection Details 
REDSHIFT_CONN_ID = "redshift_conn_id"

# Redshift IAM Role ARN for S3 COPY operations
# associated with Redshift Serverless workgroup.
REDSHIFT_COPY_IAM_ROLE = "arn:aws:iam::[your account ID]:role/RedshiftServerlessCopyRole"