from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="poc_02_generate_activities",
    start_date=datetime(2025, 1, 1),
    schedule="0 7 * * *",   # <-- CRON : tous les jours à 07:00
    catchup=False,
) as dag:

    generate = BashOperator(
        task_id="generate_clean_activities",
        bash_command="python /opt/airflow/project/src/transformations/generate_clean_activities.py",
    )

    trigger_poc_05 = TriggerDagRunOperator(
        task_id="trigger_poc_05_quality_checks",
        trigger_dag_id="poc_05_quality_checks",
        # optionnel : pour éviter de relancer si déjà déclenché
        reset_dag_run=True,
        wait_for_completion=False,
    )

    generate >> trigger_poc_05
