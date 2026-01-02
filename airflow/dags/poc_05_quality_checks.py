from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="poc_05_quality_checks",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    quality = BashOperator(
        task_id="run_ge",
        bash_command="python /opt/airflow/project/src/quality/run_ge_checks.py",
    )

    quality
