import os
import boto3
from dotenv import load_dotenv
from sqlalchemy import create_engine
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

# Salva no schema "bronze"
df_preco_competidores.to_sql(
    name="preco_competidores_bronze",
    con=engine,
    schema="bronze",
    if_exists="replace",
    index=False
)

print("Tabela 'bronze.preco_competidores' criada/atualizada com sucesso!")









# s3.create_bucket(Bucket="meu-bucket") create bucket
# s3.upload_file("arquivo.txt", "meu-bucket", "arquivo.txt") #create arquivo
# s3.download_file("meu-bucket", "arquivo.txt", "arquivo_baixado.txt") #download arquivo

# list arquivos
# response = s3.list_objects_v2(Bucket="meu-bucket")
# print(response.get("Contents", []))


#obj = s3.get_object(Bucket="meu-bucket", Key="arquivo.txt")
#conteudo = obj["Body"].read()
#print(conteudo)

# Retorna um dict com:
# Body (stream do arquivo)
# ContentType
# ContentLength
# Metadados


# import json

# obj = s3.get_object(Bucket="meu-bucket", Key="dados.json")
# dados = json.loads(obj["Body"].read())

# print(dados)


#Arquivos grandes
#Streaming (iter_chunks)