from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import psycopg2
import os
import logging

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
        response.raise_for_status()  # Raise exception for bad responses
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
        df['market_cap_to_volume'] = df['market_cap'] / df['total_volume']
        df.to_csv('/opt/airflow/crypto_data_cleaned.csv', index=False)
        logger.info("Transformation complete")
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise

# Load
def load_to_postgres():
    logger.info("Loading data to PostgreSQL...")
    try:
        df = pd.read_csv('/opt/airflow/crypto_data_cleaned.csv')
        df['fetched_at'] = datetime.utcnow()

        # Use Render database URL
        DATABASE_URL = os.getenv("DATABASE_URL")
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL environment variable not set!")

        # Append sslmode if not present
        if 'sslmode' not in DATABASE_URL:
            DATABASE_URL += "?sslmode=require"

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Create table with schema matching Flask app
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                price NUMERIC,
                market_cap BIGINT,
                total_volume BIGINT,
                market_cap_to_volume FLOAT,
                fetched_at TIMESTAMP
            )
        """)

        # Insert data
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO crypto_prices (
                    symbol, price, market_cap, total_volume, market_cap_to_volume, fetched_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['symbol'],
                row['current_price'],  # Map current_price to price
                row['market_cap'],
                row['total_volume'],
                row['market_cap_to_volume'],
                row['fetched_at']
            ))

        conn.commit()
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load data: {str(e)}")
        raise
    finally:
        cur.close()
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