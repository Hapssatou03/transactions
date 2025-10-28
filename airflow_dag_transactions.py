"""
Airflow DAG: transactions_pipeline
- Task 1: (placeholder) upload raw file to S3
- Task 2: run local transform script (simulate cleaning step)
- Task 3: (placeholder) upload cleaned to S3
Adjust operators to use S3 Hooks/Connections in your environment.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import os

RAW_PATH = os.environ.get("RAW_PATH", "/opt/airflow/dags/data/transactions_raw.csv")
CLEAN_PATH = os.environ.get("CLEAN_PATH", "/opt/airflow/dags/data/transactions_clean.csv")

def upload_raw_to_s3():
    # TODO: replace with S3Hook or boto3 using an Airflow connection
    print("Upload raw to S3 - placeholder")

def run_transform():
    cmd = [
        "python", "/opt/airflow/dags/etl_transform.py",
        "--input", RAW_PATH,
        "--output", CLEAN_PATH
    ]
    subprocess.check_call(cmd)

def upload_clean_to_s3():
    # TODO: replace with S3Hook or boto3 using an Airflow connection
    print("Upload cleaned to S3 - placeholder")

default_args = {
    "owner": "hapssatou",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="transactions_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["transactions","batch","aws","s3","athena"]
) as dag:

    t1 = PythonOperator(
        task_id="upload_raw_to_s3",
        python_callable=upload_raw_to_s3
    )

    t2 = PythonOperator(
        task_id="transform_clean",
        python_callable=run_transform
    )

    t3 = PythonOperator(
        task_id="upload_clean_to_s3",
        python_callable=upload_clean_to_s3
    )

    t1 >> t2 >> t3
