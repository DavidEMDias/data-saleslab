# Use a imagem oficial do Airflow
FROM apache/airflow:2.9.3-python3.11

# Torna root para instalar pacotes do sistema
USER root

# Instala dependências do sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Volta para o usuário airflow
USER airflow

# Copia requirements.txt para dentro do container
COPY requirements.txt /requirements.txt

# Instala pacotes Python do requirements.txt **sem constraints**
RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

# Define diretório de trabalho
WORKDIR /workspace
