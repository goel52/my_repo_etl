import json
from datetime import datetime, timedelta
from urllib.parse import urljoin

import psycopg
import pydantic
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

def load_f_order():
    query = """
            INSERT INTO public.f_order (order_id, create_date, customer_id, city_id, item_id, quantity, payment_amount)
            SELECT DISTINCT \
            ON (uniq_id)
                uniq_id AS order_id,
                date_time:: date AS create_date,
                cus.id AS customer_id,
                c.id AS city_id,
                it.id AS item_id,
                quantity,
                payment_amount
            FROM public.user_order_log uol
                JOIN public.d_city c \
            on c.city_id = uol.city_id and '{{ds}}' BETWEEN c.start_date AND c.end_date
                JOIN public.d_customer cus ON cus.customer_id = uol.customer_id and '{{ds}}' BETWEEN cus.start_date AND cus.end_date
                JOIN public.d_item it on it.item_id = uol.item_id and '{{ds}}' BETWEEN it.start_date AND it.end_date
            WHERE uol.date_time:: date = '{{ds}}'
            ORDER BY uniq_id, date_time DESC; \
            """
    with psycopg.connect(
            **PG_CONNECTION
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def load_f_activity():
    query = """
                INSERT INTO public.f_activity (activity_id, create_date, customer_id, action_id, quantity)
                SELECT DISTINCT ON (uniq_id)
                       uniq_id         AS activity_id,
                       date_time::date AS create_date,
                       cus.id      AS customer_id,
                       ual.action_id      ,
                       quantity
                FROM public.user_activity_log ual
                         JOIN public.d_customer cus ON cus.customer_id = ual.customer_id and '{{ ds }}' BETWEEN cus.start_date AND cus.end_date
                         WHERE ual.date_time::date = '{{ ds }}'
                            ORDER BY uniq_id, date_time DESC
            """
    with psycopg.connect(
            **PG_CONNECTION
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


# ваш код здесь


with DAG('api_data_load',
         default_args=default_args,
         start_date=datetime(2024, 1, 1),
         schedule_interval='@daily',
         catchup=True,
         max_active_runs=1) as dag:
    # order_log = PythonOperator(task_id='order_log',
    #                            python_callable=insert_user_order_log,
    #                            provide_context=True)
    #
    # activity_log = PythonOperator(task_id='activity_log',
    #                               python_callable=insert_user_activity_log,
    #                               provide_context=True)

    ##############################################################################
    #  Task таблиц измерений
    ##############################################################################

    f_order = PythonOperator(task_id='f_activity',
                             python_callable=load_f_order,
                             provide_context=True)

    f_activity = PythonOperator(task_id='f_order',
                             python_callable=load_f_activity,
                             provide_context=True)

    # (order_log >> activity_log >> d_customer >> d_city >>d_item >> f_order)  # не забудьте добавить новую задачу в последовательность
    ( f_order >> f_activity)