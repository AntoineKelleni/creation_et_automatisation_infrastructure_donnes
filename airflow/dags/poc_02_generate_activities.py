from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="poc_02_generate_activities",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    generate = BashOperator(
        task_id="generate_clean_activities",
        bash_command="python /opt/airflow/project/src/transformations/generate_clean_activities.py",
    )

    generate
