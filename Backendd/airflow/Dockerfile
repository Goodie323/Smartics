FROM apache/airflow:2.7.3

WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAG and helpers
COPY etl.py /opt/airflow/dags/
COPY ensure_conflict_key.py /opt/airflow/
COPY run_etl_manual.py /opt/airflow/

ENV PYTHONUNBUFFERED=1

# Start Airflow standalone, binding webserver to Render's provided port
CMD ["bash", "-lc", "export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=$PORT && airflow standalone"]
