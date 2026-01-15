import os
import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import io

load_dotenv()

# BUCKET
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
ENDPOINT_URL = "http://localhost:9000"
BUCKET_NAME = os.getenv("BUCKET")
FILE_NAME = "preco_competidores.parquet"

# DB
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")



engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1"
)



# list objects
response = s3.list_objects(Bucket=BUCKET_NAME)
arquivos = [obj["Key"] for obj in response["Contents"]]

# get object (file)
object = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
parquet_bytes = object["Body"].read() # Get body contents

parquet = io.BytesIO(parquet_bytes) # <_io.BytesIO object at 0x000001A3FCABCB80>

df_preco_competidores = pd.read_parquet(parquet)
df_preco_competidores["updated_at"] = pd.Timestamp.utcnow()
bronze_table = "preco_competidores_bronze"
schema_name = "bronze"

with engine.begin() as conn:
    # Cria schema se não existir
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

    # Cria tabela se não existir
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{bronze_table} (
        id_produto TEXT,
        nome_concorrente TEXT,
        preco_concorrente NUMERIC,
        data_coleta TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    conn.execute(text(create_sql))

    # Pega a maior data_coleta já inserida
    result = conn.execute(
        text(f"SELECT MAX(data_coleta) FROM {schema_name}.{bronze_table}")
    )
    max_data_coleta = result.scalar()

df_preco_competidores["data_coleta"] = pd.to_datetime(df_preco_competidores["data_coleta"])

# Filtra apenas registros mais recentes que já existem na tabela
if max_data_coleta is not None:
    df = df_preco_competidores[df_preco_competidores["data_coleta"] > max_data_coleta]
else:
    df = df_preco_competidores
# Só insere se houver registros novos
if not df.empty:
    df.to_sql(
        name=bronze_table,
        con=engine,
        schema=schema_name,
        if_exists="append",
        index=False
    )
    print(f"{len(df)} registros inseridos na tabela {schema_name}.{bronze_table}.")
else:
    print("Nenhum registro novo para inserir.")