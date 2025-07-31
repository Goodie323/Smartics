FROM apache/airflow:2.7.3
WORKDIR /opt/airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY etl.py /opt/airflow/dags/
ENV PYTHONUNBUFFERED=1