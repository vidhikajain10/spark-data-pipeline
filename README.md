# Distributed ETL Pipeline using Apache Spark & Airflow

## Overview
This project implements a distributed batch ETL pipeline using Apache Spark for large-scale data processing and Apache Airflow for workflow orchestration.

## Features
- Distributed data processing using PySpark
- Data transformation & aggregation
- Partitioned Parquet storage
- Workflow scheduling using Airflow DAG
- Basic data validation

## Tech Stack
- Apache Spark (PySpark)
- Apache Airflow
- Python
- Parquet Storage Format

## How to Run
1. Install dependencies:
   pip install -r requirements.txt

2. Place dataset in data/raw/

3. Start Airflow:
   airflow standalone

4. Trigger DAG from Airflow UI.
