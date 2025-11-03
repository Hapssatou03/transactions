# dags/airflow_dag_transactions.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET = os.getenv("S3_BUCKET", "hapssatou-datalake")

# Garde /opt/airflow/dags/data si tes fichiers sont versionnés dans le repo
RAW_LOCAL = os.getenv("RAW_LOCAL", "/opt/airflow/dags/data/transactions_raw_20251028.csv")
CLEAN_LOCAL = os.getenv("CLEAN_LOCAL", "/opt/airflow/dags/data/transactions_clean_local.csv")

def upload_raw_to_s3(**context):
    dt = context["logical_date"]      # pendulum.DateTime
    ds_path = dt.format("YYYY/MM/DD")
    ds_nodash = dt.format("YYYYMMDD")
    key = f"transactions/raw/{ds_path}/transactions_{ds_nodash}.csv"

    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_file(filename=RAW_LOCAL, key=key, bucket_name=BUCKET, replace=True)
    print(f"[RAW] Uploaded to s3://{BUCKET}/{key}")

def run_transform(**context):
    import subprocess, os
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(dag_dir)        # /opt/airflow
    script = os.path.join(project_root, "scripts", "etl_transform.py")
    subprocess.check_call(["python", script, "--input", RAW_LOCAL, "--output", CLEAN_LOCAL])
    print("[TRANSFORM] Clean CSV written")

def upload_clean_to_s3(**context):
    dt = context["logical_date"]
    ds_path = dt.format("YYYY/MM/DD")
    ds_nodash = dt.format("YYYYMMDD")
    key = f"transactions/clean/{ds_path}/transactions_clean_{ds_nodash}.csv"

    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_file(filename=CLEAN_LOCAL, key=key, bucket_name=BUCKET, replace=True)
    print(f"[CLEAN] Uploaded to s3://{BUCKET}/{key}")

default_args = {"owner": "hapssatou", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="transactions_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["transactions", "aws", "s3", "athena"],
) as dag:
    t1 = PythonOperator(task_id="upload_raw_to_s3", python_callable=upload_raw_to_s3)
    t2 = PythonOperator(task_id="transform_clean",  python_callable=run_transform)
    t3 = PythonOperator(task_id="upload_clean_to_s3", python_callable=upload_clean_to_s3)
    t1 >> t2 >> t3