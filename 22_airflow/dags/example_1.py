# otus example
from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

EXAMPLE_PATH = "/Users/mikhail.maryufich/PycharmProjects/airflow_tutorial"
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "example_1",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id="touch_file_1",
    bash_command=f"touch {EXAMPLE_PATH}/example_1_output/1.txt",
    dag=dag,
)

t2 = BashOperator(
    task_id="sleep", depends_on_past=False, bash_command="sleep 2", retries=3, dag=dag,
)

t3 = BashOperator(
    task_id="touch_file_2",
    depends_on_past=False,
    bash_command=f"touch {EXAMPLE_PATH}/example_1_output/2.txt",
    retries=3,
    dag=dag,
)

t1 >> t2 >> t3
