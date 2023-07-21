from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

# Para envío de mails automáticos
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email


from pyscripts.ETL_newsapi import extract, transform_load

# Función que realiza el envío de email cuando falla una task
def failure_function(context):
    dag_run = context.get('dag_run')
    msg = "Falló la tarea 😟"
    subject = f"DAG {dag_run} falló 😟"
    send_email(to="i.zapata1989@gmail.com", subject=subject, html_content=msg)

# Función que realiza el envío de email cuando se completa exitosamente una task
def success_function(context):
    dag_run = context.get('ti')
    msg = "La terea se completó de manera exitosa 👌"
    subject = f"DAG {dag_run} exitoso 👌"
    send_email(to="i.zapata1989@gmail.com", subject=subject, html_content=msg)


@task(  task_id="extract_task",
        retries=2,
        on_success_callback=success_function
)
def extract_task():
    print("Ejecutando Tarea 1: EXTRACT ======")
    return extract()

@task(  task_id="transform_load_task",
        retries=2,
        on_success_callback=success_function
)
def transform_and_load_task():
    print("Ejecutando Tarea 2: TRANSFORM + LOAD ======")
    transform_load()


default_args={
    'owner': 'Illak',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'email': ['i.zapata1989@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}

with DAG(
    default_args=default_args,
    dag_id='DAG_ETL_newsapi',
    description= 'Este Dag consiste de 2 etapas: primero (EXTRACT) luego (TRANSFORM + LOAD)',
    start_date=datetime(2023,7,6,2),
    schedule_interval='@daily',
    catchup=False,
    ) as dag:

    extract_task() >> transform_and_load_task()