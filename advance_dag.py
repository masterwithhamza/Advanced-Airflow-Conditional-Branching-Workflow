from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import random

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'advanced_dag',
    default_args=default_args,
    description='An advanced tutorial DAG',
    schedule_interval=timedelta(days=1),
)

# Define Python function for PythonOperator
def print_random_number(**kwargs):
    number = random.randint(1, 100)
    print(f"Generated random number: {number}")
    # Push the number to XCom
    kwargs['ti'].xcom_push(key='random_number', value=number)

# Define a branching function
def branch_on_number(**kwargs):
    # Pull the number from XCom
    number = kwargs['ti'].xcom_pull(task_ids='generate_random_number', key='random_number')
    if number <= 50:
        return 'below_50'
    else:
        return 'above_50'

# Task 1: Generate a random number
generate_random_number = PythonOperator(
    task_id='generate_random_number',
    python_callable=print_random_number,
    provide_context=True,
    dag=dag,
)

# Task 2: Branch based on the random number
branch_task = BranchPythonOperator(
    task_id='branching',
    python_callable=branch_on_number,
    provide_context=True,
    dag=dag,
)

# Task 3: Execute if the number is below 50
below_50_task = BashOperator(
    task_id='below_50',
    bash_command='echo "The number is below 50."',
    dag=dag,
)

# Task 4: Execute if the number is above 50
above_50_task = BashOperator(
    task_id='above_50',
    bash_command='echo "The number is above 50."',
    dag=dag,
)

# Dummy task to join branches
join = DummyOperator(
    task_id='join',
    trigger_rule='none_failed_min_one_success',
    dag=dag,
)

# Set up task dependencies
generate_random_number >> branch_task
branch_task >> [below_50_task, above_50_task] >> join
