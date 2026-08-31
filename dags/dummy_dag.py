from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Описание параметров текущего DAG 
with DAG(
        dag_id='dummy_dag',  # id DAG. Рекомендуем именовать DAG так же, как и сами файлы.
        start_date=datetime(2023, 7, 13),  # Дата, с которой DAG начинает работу
        schedule_interval='@daily',  # Расписание работы DAG
        catchup=False,  # Параметр указывает, нужно ли запускать все DAG Run с момента start_date.
) as dag:
    dummy_task = EmptyOperator(  # Каждая задача, Task, является экземпляром оператора Airflow
        task_id='dummy_task'  # id задачи
    )

    second_task = EmptyOperator(  # Каждая задача, Task, является экземпляром оператора Airflow
        task_id='second_task'  # id задачи
    )

    third_task = EmptyOperator(  # Каждая задача, Task, является экземпляром оператора Airflow
        task_id='third_task'  # id задачи
    )

    dummy_task >> second_task
    dummy_task >> third_task

