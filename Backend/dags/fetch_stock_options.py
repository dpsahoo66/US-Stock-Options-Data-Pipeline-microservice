from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def log_response(**context):
    response = context['task_instance'].xcom_pull(task_ids='fetch_data')
    logging.info(f"API Response: {response}")

with DAG(
    'fetch_stock_options',
    default_args=default_args,
    description='DAG to fetch stock and options data periodically',
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 6, 13),
    catchup=False,
) as dag:
    
    fetch_task = HttpOperator(
        task_id='fetch_data',
        http_conn_id='data_collector_service',
        endpoint='/api/fetch_data',
        method='POST',
        headers={'Content-Type': 'application/json'},
        data='{"symbol": "AAPL", "sources": ["twelvedata", "yfinance"]}',
        response_check=lambda response: response.status_code == 200,
        dag=dag,
    )

    log_task = PythonOperator(
        task_id='log_response',
        python_callable=log_response,
        dag=dag,
    )

    fetch_task >> log_task