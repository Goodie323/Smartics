FROM apache/airflow:2.7.3
WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs / helpers
COPY etl.py /opt/airflow/dags/
COPY ensure_conflict_key.py /opt/airflow/
COPY run_etl_manual.py /opt/airflow/
COPY start.sh /opt/airflow/

# Make startup script executable
RUN chmod +x start.sh

ENV PYTHONUNBUFFERED=1

# Use start.sh as the container startup
CMD ["./start.sh"]
