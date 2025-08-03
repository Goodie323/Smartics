FROM python:3.9-slim

WORKDIR /opt/airflow

# Install system dependencies for Airflow + psycopg2
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Airflow (core) + providers you need
ENV AIRFLOW_VERSION=2.7.3
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" psycopg2-binary pandas requests

# Copy requirements for your DAG (if any extra deps)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs and helper scripts
COPY etl.py /opt/airflow/dags/
COPY ensure_conflict_key.py /opt/airflow/
COPY run_etl_manual.py /opt/airflow/

ENV PYTHONUNBUFFERED=1

# Start scheduler + webserver (memory friendly)
CMD ["bash", "-lc", "export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=$PORT && airflow db init && airflow scheduler & airflow webserver"]
