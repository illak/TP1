from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# Para envío de mails automáticos
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
def extract_task(ti=None):
    print("Ejecutando Tarea 1: EXTRACT ======")

    try:
        topic = str(Variable.get("topic"))
        rows = extract(topic)
    except:
        rows = extract()
    #rows = extract()
    print(f"ROWS: {rows}")
    """Pushes an XCom without a specific target"""
    #ti.xcom_push(task_ids='extract_task', key='num_regs')
    return rows


@task(  task_id="transform_load_task",
        retries=2,
        on_success_callback=success_function
)
def transform_and_load_task():
    print("Ejecutando Tarea 2: TRANSFORM + LOAD ======")
    transform_load()


@task.branch( 
        task_id="check_num_rows",
        retries=2)
def check_number_of_regs_task(ti=None):
    # Función que verifica la cantidad de registros que se obtuvieron desde la API (EXTRACT)
    num_regs = int(ti.xcom_pull(task_ids='extract_task'))
    min_regs = int(Variable.get("min_regs"))

    print("==============")
    print(f"Min regs expected: {min_regs}")
    print("==============")

    if num_regs > min_regs:
        return "transform_load_task"
    else:
        return "send_email_no_condition"


@task(  task_id="send_email_no_condition")
def enviar_mail_eror_task():
    import smtplib
    from email.message import EmailMessage
    import datetime
    from datetime import datetime  
    
    usr = Variable.get("mail")
    pwd = Variable.get("mail_pass")
    min_regs = int(Variable.get("min_regs"))

    try:

        ts = datetime.now()
        mensaje=f'No se han generado la cantidad suficiente de registros ({min_regs}) para la siguiente tarea: [TRANSFORM & LOAD]\n \
Fecha y hora del incidente: {ts}'

        email = EmailMessage()
        email["From"] = usr
        email["To"] = usr
        email["Subject"] = "Insuficientes registros"
        email.set_content(mensaje)


        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login(usr, pwd)    
        x.sendmail(usr, usr, email.as_string())
        print('Exito')

    except Exception as exception:
        print(exception)
        print('Failure')


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

    extract_task() >> check_number_of_regs_task() >> [enviar_mail_eror_task(), transform_and_load_task()]