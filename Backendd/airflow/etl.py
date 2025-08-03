from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import psycopg2
import os
import logging

# === FIX: missing import used inside load_to_postgres ===
from datetime import datetime as dt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Extract
def extract_crypto_data():
    logger.info("Extracting data from CoinGecko...")
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 10,
            'page': 1,
            'sparkline': False
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        df.to_csv('/opt/airflow/crypto_data.csv', index=False)
        logger.info("Extraction complete")
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise

# Transform
def transform_data():
    logger.info("Transforming data...")
    try:
        df = pd.read_csv('/opt/airflow/crypto_data.csv')
        df = df[['id', 'symbol', 'current_price', 'market_cap', 'total_volume']]
        # Avoid division by zero
        df['market_cap_to_volume'] = df.apply(
            lambda r: r['market_cap'] / r['total_volume'] if r['total_volume'] else None,
            axis=1
        )
        df.to_csv('/opt/airflow/crypto_data_cleaned.csv', index=False)
        logger.info("Transformation complete")
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise

# Load
def load_to_postgres():
    logger.info("Loading data to PostgreSQL...")
    conn = None
    cur = None
    try:
        df = pd.read_csv('/opt/airflow/crypto_data_cleaned.csv')
        df['fetched_at'] = dt.utcnow()

        DATABASE_URL = os.getenv("DATABASE_URL")
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set!")

        # === FIX: safely append sslmode if missing ===
        if "sslmode" not in DATABASE_URL:
            sep = "&" if "?" in DATABASE_URL else "?"
            DATABASE_URL = f"{DATABASE_URL}{sep}sslmode=require"

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # === FIX: use symbol+fetched_at as composite primary key to avoid duplicates ===
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                symbol TEXT,
                price NUMERIC,
                market_cap BIGINT,
                total_volume BIGINT,
                market_cap_to_volume DOUBLE PRECISION,
                fetched_at TIMESTAMP,
                PRIMARY KEY (symbol, fetched_at)
            )
        """)

        insert_sql = """
            INSERT INTO crypto_prices (
                symbol, price, market_cap, total_volume, market_cap_to_volume, fetched_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, fetched_at) DO NOTHING
        """

        for _, row in df.iterrows():
            cur.execute(insert_sql, (
                row['symbol'],
                row['current_price'],  # mapped to price
                row['market_cap'],
                row['total_volume'],
                row['market_cap_to_volume'],
                row['fetched_at']
            ))

        conn.commit()
        logger.info("Data loaded successfully, %d rows processed", len(df))
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to load data: {e}", exc_info=True)
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# DAG definition
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG('crypto_etl_pipeline',
         schedule_interval='*/10 * * * *',  # Every 10 minutes
         default_args=default_args,
         catchup=False) as dag:

    t1 = PythonOperator(task_id='extract', python_callable=extract_crypto_data)
    t2 = PythonOperator(task_id='transform', python_callable=transform_data)
    t3 = PythonOperator(task_id='load', python_callable=load_to_postgres)

    t1 >> t2 >> t3
