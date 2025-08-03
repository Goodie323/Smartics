FROM apache/airflow:2.7.3-python3.9-slim

WORKDIR /opt/airflow

# Install Python packages for your DAGs
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs and helper scripts
COPY etl.py /opt/airflow/dags/
COPY ensure_conflict_key.py /opt/airflow/
COPY run_etl_manual.py /opt/airflow/

ENV PYTHONUNBUFFERED=1

# Start only scheduler + webserver (less memory than standalone)
CMD ["bash", "-lc", "export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=$PORT && airflow db init && airflow scheduler & airflow webserver"]
