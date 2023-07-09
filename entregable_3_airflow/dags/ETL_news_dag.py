from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task


from pyscripts.ETL_newsapi import extract, transform_load

@task()
def extract_task():
    print("Ejecutando Tarea 1: EXTRACT ======")
    return extract()

@task()
def transform_and_load_task():
    print("Ejecutando Tarea 2: TRANSFORM + LOAD ======")
    transform_load()


default_args={
    'owner': 'Illak',
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    default_args=default_args,
    dag_id='DAG_ETL_newsapi',
    description= 'Este Dag consiste de 2 etapas: primero (EXTRACT) luego (TRANSFORM + LOAD)',
    start_date=datetime(2023,7,6,2),
    schedule_interval='@daily',
    catchup=False
    ) as dag:

    extract_task() >> transform_and_load_task()