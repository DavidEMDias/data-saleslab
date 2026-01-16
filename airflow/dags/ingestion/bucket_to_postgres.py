import os
import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Table, MetaData, Column, String, Numeric, DateTime
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import io

def load_preco_competidores_bronze():
    load_dotenv()

    # BUCKET
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    ENDPOINT_URL = "http://minio:9000"
    BUCKET_NAME = os.getenv("BUCKET")
    FILE_NAME = "preco_competidores.parquet"

    # DB
    DB_HOST = "postgres"
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # Connect to MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    # Get parquet file from bucket
    response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
    parquet_bytes = response["Body"].read()
    parquet_io = io.BytesIO(parquet_bytes)

    df_preco_competidores = pd.read_parquet(parquet_io)
    df_preco_competidores["updated_at"] = pd.Timestamp.utcnow()

    bronze_table = "preco_competidores_bronze"
    schema_name = "bronze"

    # Create metadata and table definition
    metadata = MetaData()
    
    # Define the table structure
    table = Table(
        bronze_table,
        metadata,
        Column('id_produto', String),
        Column('nome_concorrente', String),
        Column('preco_concorrente', Numeric),
        Column('data_coleta', DateTime),
        Column('updated_at', DateTime),
        schema=schema_name
    )

    # Use a single connection block
    with engine.begin() as conn:
        # Create schema if not exists
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        
        # Create table if not exists
        metadata.create_all(conn)
        
        # Get the max data_coleta
        result = conn.execute(text(f"SELECT MAX(data_coleta) FROM {schema_name}.{bronze_table}"))
        max_data_coleta = result.scalar()

        # Prepare dataframe to insert
        df_preco_competidores["data_coleta"] = pd.to_datetime(df_preco_competidores["data_coleta"])
        if max_data_coleta is not None:
            df = df_preco_competidores[df_preco_competidores["data_coleta"] > max_data_coleta]
        else:
            df = df_preco_competidores

        # Insert new records only if dataframe is not empty
        if not df.empty:
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Insert records
            conn.execute(table.insert(), records)
            
            print(f"{len(df)} registros inseridos na tabela {schema_name}.{bronze_table}.")
        else:
            print("Nenhum registro novo para inserir.")

if __name__ == "__main__":
    load_preco_competidores_bronze()