from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.parsing_utils import get_insert_user_activity_log, get_insert_user_order_log
from utils.postgres_utils import execute_postgres_query

default_args = {
    'owner': 'airflow',
    'concurrency': 1,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

with DAG('api_data_load',
         default_args=default_args,
         start_date=datetime(2024, 1, 1),
         schedule_interval='@daily',
         catchup=True,
         max_active_runs=1) as dag:
    clean_order_log = SQLExecuteQueryOperator(
        task_id='clean_order_log',
        conn_id='conn_pg',
        sql=""" DELETE FROM public.user_order_log
                WHERE date_time::date='{{ ds }}'"""
    )

    order_log = PythonOperator(
        task_id='order_log',
        python_callable=get_insert_user_order_log,
        provide_context=True,
    )

    clean_activity_log = SQLExecuteQueryOperator(
        task_id='clean_activity_log',
        conn_id='conn_pg',
        sql=""" DELETE FROM public.user_activity_log
                 WHERE date_time::date='{{ ds }}'"""
    )

    activity_log = PythonOperator(
        task_id='activity_log',
        python_callable=get_insert_user_activity_log,
        provide_context=True,
    )

    clean_d_customer_stg = SQLExecuteQueryOperator(
        task_id='clean_d_customer_stg',
        conn_id='conn_pg',
        sql="TRUNCATE d_customer_stg"  # очистка стг таблицы
    )

    d_customer_stg = SQLExecuteQueryOperator(
        task_id='d_customer_stg',
        conn_id='conn_pg',
        sql="sql/insert_d_customer_stg.sql"
    )

    d_customer = SQLExecuteQueryOperator(
        task_id='load_d_customer',
        conn_id='conn_pg',
        sql="sql/insert_d_customer.sql"
    )

    clean_d_city_stg = SQLExecuteQueryOperator(
        task_id='clean_d_city_stg',
        conn_id='conn_pg',
        sql="TRUNCATE d_city_stg"
    )

    d_city_stg = SQLExecuteQueryOperator(
        task_id='d_city_stg',
        conn_id='conn_pg',
        sql="sql/insert_d_city_stg.sql"
    )

    d_city = SQLExecuteQueryOperator(
        task_id='load_d_city',
        conn_id='conn_pg',
        sql="sql/insert_d_city.sql"
    )

    clean_d_item_stg = SQLExecuteQueryOperator(
        task_id='clean_d_item_stg',
        conn_id='conn_pg',
        sql="TRUNCATE d_item_stg"
    )

    d_item_stg = SQLExecuteQueryOperator(
        task_id='d_item_stg',
        conn_id='conn_pg',
        sql="sql/insert_d_item_stg.sql"
    )

    d_item = SQLExecuteQueryOperator(
        task_id='load_d_item',
        conn_id='conn_pg',
        sql="sql/insert_d_item.sql"
    )
    clean_f_order = SQLExecuteQueryOperator(
        task_id='clean_f_order',
        conn_id='conn_pg',
        sql="""DELETE FROM public.f_order
            WHERE create_date::date='{{ ds }}'"""
    )

    f_order = SQLExecuteQueryOperator(
        task_id='load_f_order',
        conn_id='conn_pg',
        sql='sql/insert_f_order.sql'
    )

    clean_f_activity = SQLExecuteQueryOperator(
        task_id='clean_f_activity',
        conn_id='conn_pg',
        sql="""DELETE FROM public.f_activity
                WHERE create_date::date='{{ ds }}'"""
    )

    f_activity = SQLExecuteQueryOperator(
        task_id='load_f_activity',
        conn_id='conn_pg',
        sql='sql/insert_f_activity.sql'
    )

    customer_report = SQLExecuteQueryOperator(
        task_id='load_customer_report',
        conn_id='conn_pg',
        sql='sql/insert_customer_report.sql'
    )

    (clean_order_log >> order_log >> clean_activity_log >> activity_log >>
     clean_d_customer_stg >> d_customer_stg >> d_customer >>
     clean_d_city_stg >> d_city_stg >> d_city >>
     clean_d_item_stg >> d_item_stg >> d_item >>
     clean_f_order >> f_order >> f_activity >> customer_report)  # добавляем новые Task
