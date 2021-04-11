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
    "example_2",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
)


def mkdir_and_create_file(path: str, filename: str) -> str:
    folder_path = f"{path}/example_2_output/{{{{ ds }}}}"

    cmd = f"mkdir -p {folder_path}\n"
    cmd += f"touch {folder_path}/{filename}"

    return cmd


t1 = BashOperator(
    task_id="touch_file_1",
    bash_command=mkdir_and_create_file(EXAMPLE_PATH, "1.txt"),
    dag=dag,
)

t2 = BashOperator(
    task_id="sleep", depends_on_past=False, bash_command="sleep 2", retries=3, dag=dag,
)

t3 = BashOperator(
    task_id="touch_file_2",
    depends_on_past=False,
    bash_command=mkdir_and_create_file(EXAMPLE_PATH, "2.txt"),
    retries=3,
    dag=dag,
)

t1 >> t2 >> t3

# http://airflow.apache.org/docs/stable/macros-ref.html
