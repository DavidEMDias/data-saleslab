from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 16),
    'retries': 1,
}

with DAG(
    dag_id='dbt_run',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:


    dbt_run = DockerOperator(
        task_id='dbt_run',
        image='ghcr.io/dbt-labs/dbt-postgres:1.8.latest',
        api_version='auto',
        auto_remove=True,
        command='run',
        docker_url='unix://var/run/docker.sock',
        network_mode='data-platform',
        working_dir='/usr/app',
        mounts=[
            Mount(source='C:/Users/david/Documents/api_parquet_to_postgres/dbt/dbt_proj', target='/usr/app', type='bind'),
            Mount(source='C:/Users/david/Documents/api_parquet_to_postgres/dbt/profiles', target='/root/.dbt', type='bind'),
        ],
        environment={
            'DBT_PROFILES_DIR': '/root/.dbt',
            'POSTGRES_USER': os.getenv('DB_USER'),
            'POSTGRES_PASSWORD': os.getenv('DB_PASSWORD'),
            'POSTGRES_DB': os.getenv('DB_NAME'),
        },
    )

