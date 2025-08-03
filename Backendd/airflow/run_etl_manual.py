import os
import logging
from datetime import datetime
import pandas as pd
import requests
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_crypto_data():
    logger.info("Extracting data from CoinGecko...")
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
    df = pd.DataFrame(response.json())
    df.to_csv('crypto_data.csv', index=False)
    logger.info("Extraction complete: saved crypto_data.csv")

def transform_data():
    logger.info("Transforming data...")
    df = pd.read_csv('crypto_data.csv')
    df = df[['id', 'symbol', 'current_price', 'market_cap', 'total_volume']]
    df['market_cap_to_volume'] = df.apply(
        lambda r: r['market_cap'] / r['total_volume'] if r['total_volume'] else None,
        axis=1
    )
    df.to_csv('crypto_data_cleaned.csv', index=False)
    logger.info("Transformation complete: saved crypto_data_cleaned.csv")

def load_to_postgres():
    logger.info("Loading to Postgres...")
    df = pd.read_csv('crypto_data_cleaned.csv')
    df['fetched_at'] = datetime.utcnow()

    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var not set")

    if "sslmode" not in DATABASE_URL:
        sep = "&" if "?" in DATABASE_URL else "?"
        DATABASE_URL = f"{DATABASE_URL}{sep}sslmode=require"

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
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
                row['current_price'],
                row['market_cap'],
                row['total_volume'],
                row['market_cap_to_volume'],
                row['fetched_at']
            ))
        conn.commit()
        logger.info("Loaded %d rows into crypto_prices", len(df))
    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Error during load")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    extract_crypto_data()
    transform_data()
    load_to_postgres()
