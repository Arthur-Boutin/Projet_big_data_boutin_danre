from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from lib.dvf_fetcher import fetch_dvf_data

from lib.raw_to_fmt_dvf import convert_dvf_to_parquet
from lib.lbc_fetcher import fetch_lbc_data
from lib.raw_to_fmt_lbc import convert_lbc_to_parquet

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'immobilier_big_data_pipeline',
    default_args=default_args,
    description='Pipeline DVF 2025',
    schedule='0 * * * *', # Run every hour
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_dvf_2025',
        python_callable=fetch_dvf_data
    )

    transform_task = PythonOperator(
        task_id='transform_dvf_2025',
        python_callable=convert_dvf_to_parquet
    )

    # --- TACHES LEBONCOIN ---
    extract_lbc = PythonOperator(
        task_id='extract_lbc',
        python_callable=fetch_lbc_data
    )

    transform_lbc = PythonOperator(
        task_id='transform_lbc',
        python_callable=convert_lbc_to_parquet
    )

    extract_task >> transform_task
    extract_lbc >> transform_lbc