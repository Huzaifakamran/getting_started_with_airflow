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
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id = 'checking-python-operator-dag',
    default_args=default_args,
    description='This is my python Dag for testing',
    schedule_interval=timedelta(days=1),
)

def greet(name,age):
    print(f"Hello world, my name is {name} and my age is {age}")

task1 = PythonOperator(
    task_id = 'greet',
    python_callable = greet,
    op_kwargs = {'name':'huzaifa','age':24},
    dag=dag
)

task1