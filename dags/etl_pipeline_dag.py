from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add project path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark_jobs.extract import extract_data
from spark_jobs.transform import transform_data
from spark_jobs.load import load_data
from utils.validation import validate_data

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
}

def run_pipeline():
    input_path = "data/raw/sales.csv"
    output_path = "data/processed/"

    df = extract_data(input_path)
    validate_data(df)

    transformed_df = transform_data(df)
    validate_data(transformed_df)

    load_data(transformed_df, output_path)

with DAG(
    dag_id="spark_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Spark ETL Pipeline using Airflow"
) as dag:

    etl_task = PythonOperator(
        task_id="run_spark_etl",
        python_callable=run_pipeline
    )

    etl_task
