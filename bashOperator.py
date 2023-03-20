# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
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
    dag_id = 'etl-log-processsing-dag',
    default_args=default_args,
    description='This is my ETL for server access log',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the task 'download'

task1 = BashOperator(
    task_id='first_task',
    bash_command='echo hello world, this is the first task!',
    dag=dag,
)

task2 = BashOperator(
    task_id='second_task',
    bash_command='echo hello world, this is the second task!',
    dag=dag,
)

task3 = BashOperator(
    task_id='third_task',
    bash_command='echo hello world, this is the third task!',
    dag=dag,
)


#One Way
task1.set_downstream(task2)
task1.set_downstream(task3)
#2nd Way
# task1 >> task2
# task1 >> task3
#3rd Way
# task1 >> [task2,task3]