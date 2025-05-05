from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.file_processor import create_tables, check_and_register_files, insert_file_data

with DAG(
    dag_id="process_csv_file",
    start_date=datetime(2025, 5, 5),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

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

    t1 >> t2 >> t3
