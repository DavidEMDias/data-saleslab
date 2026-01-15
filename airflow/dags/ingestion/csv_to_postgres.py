import pandas as pd
import hashlib
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

# Carrega variáveis de ambiente
load_dotenv()

DB_HOST = "postgres"
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "my_db")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")

# Conecta ao Postgres via SQLAlchemy
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Configuração das tabelas e seus arquivos CSV
tables = {
    "clientes": {"file": "sample_data/clientes.csv", "pk": "id_cliente"},
    "vendas": {"file": "sample_data/vendas.csv", "pk": "id_venda"},
    "produtos": {"file": "sample_data/produtos.csv", "pk": "id_produto"},
}

def compute_row_hash(row, cols):
    """
    Gera um hash MD5 da linha baseado nas colunas de dados.
    Serve para detectar alterações entre pipelines.
    """
    values = [str(row[c]) for c in cols]
    s = "|".join(values)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def verify_schema_and_table(schema, bronze_table, df, pk, conn):
    """Cria schema e tabela se não existirem, e dá mensagens."""
    
    # Cria schema se não existir
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
    
    # Verifica se a tabela existe no schema
    result = conn.execute(
        text("SELECT EXISTS (SELECT 1 FROM information_schema.tables "
             "WHERE table_schema = :schema AND table_name = :table)"),
        {"schema": schema, "table": bronze_table}
    )
    exists = result.scalar()
    
    if exists:
        print(f"Tabela {schema}.{bronze_table} já existia.")
    else:
        # Colunas de dados em TEXT (exceto PK, row_hash, updated_at)
        columns_sql = ", ".join([f"{c} TEXT" for c in df.columns if c not in [pk, "row_hash", "updated_at"]])
        pk_sql = f"{pk} TEXT PRIMARY KEY"
        create_sql = f"""
        CREATE TABLE {schema}.{bronze_table} (
            {columns_sql},
            {pk_sql},
            row_hash TEXT,
            updated_at TIMESTAMP
        );
        """
        conn.execute(text(create_sql))
        print(f"Tabela {schema}.{bronze_table} criada com sucesso.")

def load_entity_to_bronze(entity: str):
    if entity not in tables:
        raise ValueError(f"Entidade inválida: {entity}. Use uma de {list(tables.keys())}")

    cfg = tables[entity]

    try:
        # Leitura do CSV
        df = pd.read_csv(cfg["file"], sep=",", encoding="utf-8")

        # Colunas de dados (exceto PK)
        data_cols = [c for c in df.columns if c != cfg["pk"]]

        # Calcula hash da linha
        df["row_hash"] = df.apply(lambda r: compute_row_hash(r, data_cols), axis=1)

        # Timestamp
        df["updated_at"] = pd.Timestamp.utcnow()

        schema = "bronze"
        bronze_table = f"{entity}_bronze"

        # Criar schema e tabela se necessário
        with engine.begin() as conn:
            verify_schema_and_table(schema, bronze_table, df, cfg["pk"], conn)

        # Inserção incremental
        cols = list(df.columns)
        cols_str = ", ".join(cols)
        placeholders = ", ".join([f":{c}" for c in cols])
        update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != cfg["pk"]])

        insert_sql = f"""
        INSERT INTO {schema}.{bronze_table} ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT ({cfg["pk"]})
        DO UPDATE SET
            {update_cols}
        WHERE {schema}.{bronze_table}.row_hash
              IS DISTINCT FROM EXCLUDED.row_hash;
        """

        with engine.begin() as conn:
            result = conn.execute(text(insert_sql), df.to_dict(orient="records"))

        print(
            f"Load concluído para {entity} "
            f"({schema}.{bronze_table}) — {result.rowcount} linhas processadas."
        )

    except FileNotFoundError as e:
        print(f"Arquivo CSV não encontrado: {cfg['file']}. Detalhes: {e}")
        raise
    except SQLAlchemyError as e:
        print(f"Erro SQL ao processar {entity}: {e}")
        raise
    except Exception as e:
        print(f"Erro inesperado ao processar {entity}: {e}")
        raise


# # Loop principal por cada tabela
# for table, cfg in tables.items():
#     try:
#         # Leitura do CSV
#         df = pd.read_csv(cfg["file"], sep=",", encoding="utf-8")

#         # Colunas de dados (exceto PK)
#         data_cols = [c for c in df.columns if c != cfg["pk"]]

#         # Calcula hash da linha para controle de alterações
#         df["row_hash"] = df.apply(lambda r: compute_row_hash(r, data_cols), axis=1)
        
#         # Timestamp da execução da pipeline
#         df["updated_at"] = pd.Timestamp.utcnow()

#         schema = "bronze"
#         bronze_table = f"{table}_bronze"

#         # Criar tabela Bronze se não existir
#         with engine.begin() as conn:
#             verify_schema_and_table(schema, bronze_table, df, cfg["pk"], conn)
           

#         # Inserção incremental com update apenas se o hash mudou
#         cols = list(df.columns)
#         cols_str = ", ".join(cols)
#         placeholders = ", ".join([f":{c}" for c in cols])
#         update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != cfg['pk']])

#         insert_sql = f"""
#         INSERT INTO {schema}.{bronze_table} ({cols_str})
#         VALUES ({placeholders})
#         ON CONFLICT ({cfg['pk']})
#         DO UPDATE SET
#             {update_cols}
#         WHERE {schema}.{bronze_table}.row_hash IS DISTINCT FROM EXCLUDED.row_hash;
#         """

#         with engine.begin() as conn:
#             result = conn.execute(text(insert_sql), df.to_dict(orient="records"))

#         print(f"Inserção incremental feita em {schema}.{bronze_table}, {result.rowcount} linhas processadas.")

#     except FileNotFoundError as e:
#         print(f"Arquivo CSV não encontrado: {cfg['file']}. Detalhes: {e}")
#     except SQLAlchemyError as e:
#         # Captura erros de SQL ou conexão
#         print(f"Erro ao processar tabela {table}: {e}")
#     except Exception as e:
#         # Captura outros erros inesperados
#         print(f"Erro inesperado: {e}")

#load_entity_to_bronze("clientes")
