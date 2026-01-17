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
from lib.index_to_es import index_lbc_to_es, index_dvf_to_es, index_formatted_dvf_to_es, index_lbc_raw_to_es
from lib.compute_usage import compute_usage_layer

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'immobilier_big_data_pipeline',
    default_args=default_args,
    description='Pipeline DVF 2025',
    schedule='0 * * * *',
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

    extract_lbc = PythonOperator(
        task_id='extract_lbc',
        python_callable=fetch_lbc_data
    )

    transform_lbc = PythonOperator(
        task_id='transform_lbc',
        python_callable=convert_lbc_to_parquet
    )

    compute_usage = PythonOperator(
        task_id='compute_usage_spark',
        python_callable=compute_usage_layer
    )

    index_lbc = PythonOperator(
        task_id='index_opportunities_to_es',
        python_callable=index_lbc_to_es
    )

    index_dvf_stats = PythonOperator(
        task_id='index_market_stats_to_es',
        python_callable=index_dvf_to_es
    )

    index_dvf_raw = PythonOperator(
        task_id='index_raw_dvf_to_es',
        python_callable=index_formatted_dvf_to_es
    )
    
    index_lbc_raw = PythonOperator(
        task_id='index_lbc_raw',
        python_callable=index_lbc_raw_to_es
    )

    extract_task >> transform_task
    extract_lbc >> transform_lbc

    [transform_task, transform_lbc] >> compute_usage

    compute_usage >> [index_lbc, index_dvf_stats]
    transform_task >> index_dvf_raw
    transform_lbc >> index_lbc_raw