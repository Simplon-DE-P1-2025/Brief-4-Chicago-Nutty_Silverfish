from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="soda_checks",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    run_soda_checks = BashOperator(
        task_id="run_soda_checks",
        bash_command="soda scan -d analytics_warehouse -c include/soda/configuration.yml include/soda/checks"
    )