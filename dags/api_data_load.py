import json
from datetime import timedelta
from urllib.parse import urljoin

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'concurrency': 1,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

API_host = 'https://de-start-sprint-etl-airflow-api.de.education-services.ru'
API_method = 'user_order_log'

payload ={'limit': '2', 'filter': {'date': '2024-09-19'}}


def print_user_order_log():
    resp = requests.post(urljoin(API_host, API_method), data=json.dumps(payload))
    resp.raise_for_status()
    print(resp.text)


def print_user_activity_log():
    resp = requests.post(urljoin(API_host, 'user_activity_log'), data=json.dumps(payload))
    resp.raise_for_status()
    print(resp.text)


with DAG(
        dag_id='api_data_load',
        default_args=default_args,
        start_date=days_ago(2),
        max_active_runs=1,
        catchup=False,
        schedule_interval='@daily',
        tags=['API']
) as dag:
    user_order_task = PythonOperator(
        task_id='user_order_task',
        python_callable=print_user_order_log
    )

    user_activity_task = PythonOperator(
        task_id='user_activity_task',
        python_callable=print_user_activity_log
    )   

    user_order_task >> user_activity_task
