from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="poc_01_ingest_and_clean",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    ingest = BashOperator(
        task_id="ingest_excels",
        bash_command="python /opt/airflow/project/src/ingestion/load_excels_to_raw.py",
    )

    clean = BashOperator(
        task_id="build_clean",
        bash_command="python /opt/airflow/project/src/transformations/build_clean_employees.py",
    )

    ingest >> clean
