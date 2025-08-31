# End-to-End ETL Data Pipeline with Apache Airflow and Amazon Redshift for a Music Streaming Service

## End-to-end ETL data pipeline with Apache Airflow to ingest metadata from CSV (RDS simulation) and streaming data from Amazon S3, transform and validate it, and load into Amazon Redshift to compute KPIs for music streaming analytics and business intelligence.


This project demonstrates an end-to-end ETL data pipeline for a music streaming service, orchestrated with Apache Airflow (MWAA). The pipeline:

Ingests user and song metadata from CSV (RDS simulation) and streaming event data from Amazon S3

Validates and transforms raw data into analytics-ready tables

Loads data into Amazon Redshift using an Upsert strategy for efficiency

Computes KPIs such as genre-level metrics (listen counts, popularity index, average track duration) and hourly insights (unique listeners, top artists, track diversity)

The pipeline supports business intelligence, and user behavior analytics, reflecting a practical modern data engineering with Airflow, S3, and Redshift.