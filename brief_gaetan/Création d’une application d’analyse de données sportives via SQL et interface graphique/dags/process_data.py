from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.file_processor import create_tables, check_and_register_files, insert_file_data

@task
def only_run_on_even_years(dag_run=None, **kwargs):
    # Skip the check if this DAG run was manually triggered
    if dag_run and dag_run.external_trigger:
        print("Manual run detected. Skipping year check.")
        return

    # If scheduled, enforce even year rule
    if datetime.now().year % 2 != 0:
        raise ValueError("Skipping because it's not an even year.")

with DAG(
    dag_id="process_csv_file",
    start_date=datetime(2025, 5, 5),
    schedule_interval="@yearly",
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    check_year = only_run_on_even_years()

    t1 = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    t2 = PythonOperator(
        task_id="check_and_register_files",
        python_callable=check_and_register_files,
    )

    t3 = PythonOperator(
        task_id="insert_file_data",
        python_callable=insert_file_data,
    )

    check_year >> t1 >> t2 >> t3
