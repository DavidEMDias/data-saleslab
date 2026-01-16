from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ingestion.bucket_to_postgres import load_preco_competidores_bronze
from ingestion.csv_to_postgres import load_entity_to_bronze


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="data_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    load_bronze_competidores = PythonOperator(
        task_id="load_preco_competidores_bronze",
        python_callable=load_preco_competidores_bronze,
    )

    load_bronze_clientes = PythonOperator(
    task_id="load_clientes_bronze",
    python_callable=load_entity_to_bronze,
    op_kwargs={
            "entity": "clientes",
        },
    )

    load_bronze_produtos = PythonOperator(
    task_id="load_produtos_bronze",
    python_callable=load_entity_to_bronze,
    op_kwargs={
            "entity": "produtos",
        },
    )

    load_bronze_vendas = PythonOperator(
    task_id="load_vendas_bronze",
    python_callable=load_entity_to_bronze,
    op_kwargs={
            "entity": "vendas",
        },
    )
