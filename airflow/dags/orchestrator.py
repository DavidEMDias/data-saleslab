import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago




default_args = {
    "description": "A DAG to orchestrate data",
    "start_date": days_ago(1),
    "catchup":False, #backfill with previous runs
}

dag = DAG(
    dag_id = 'dags-orchestrator',
    default_args=default_args,
    schedule=None
)

with dag:

    
    # Task to trigger 
    trigger_ingestion_dag = TriggerDagRunOperator(
        task_id='trigger_ingestion',
        trigger_dag_id='data_ingestion',  # DAG 2
        wait_for_completion=True,  # doesn't wait for dag 2 to finish
        reset_dag_run=True,

    )


    # Task to trigger 
    trigger_dbt_dag = TriggerDagRunOperator(
        task_id='trigger_dbt',
        trigger_dag_id='dbt_run',  # DAG 2
        wait_for_completion=False,  # doesn't wait for dag 2 to finish
        reset_dag_run=True,
    )


     # Ordem de execução
    trigger_ingestion_dag >> trigger_dbt_dag