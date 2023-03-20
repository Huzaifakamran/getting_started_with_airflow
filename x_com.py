from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Huzaifa',
    'start_date': days_ago(0),
    'email': ['muhammad.huzaifa@tmcltd.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# define the DAG
dag = DAG(
    dag_id = 'checking-X_Com',
    default_args=default_args,
    description='This is my python Dag for testing',
    schedule_interval=timedelta(days=1),
)

def greet(age,ti):
    #ti = task instance (make sure to type ti inorder to pull x_com value)
    name = ti.xcom_pull(task_ids = 'get_name')
    print(f"Hello world, my name is {name} and my age is {age}")

# def greet(age,ti):
#     first_name = ti.xcom_pull(task_ids = 'get_name',key='first_name')
#     last_name = ti.xcom_pull(task_ids = 'get_name',key='last_name')
#     print(f"Hello world, my first name is {first_name} and my last name is {last_name} my age is {age}")

def get_name():
    return 'M.Huzaifa'

# def get_name(ti):
#     ti.xcom_push(key='first_name',value='Muhammad')
#     ti.xcom_push(key='last_name',value='Huzaifa')

task1 = PythonOperator(
    task_id = 'get_name',
    python_callable = get_name,
    dag=dag
)

task2 = PythonOperator(
    task_id = 'greet',
    python_callable = greet,
    op_kwargs = {'age':24},
    dag=dag
)

task1 >> task2