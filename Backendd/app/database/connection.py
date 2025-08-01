import os
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Build DATABASE_URL from env or fallback to Render's connection info
DATABASE_URL = os.getenv("DATABASE_URL") or (
    "postgresql://smartics_db_user:1NK7RyePl1dnGRDweGq17NEs1Zj98isu"
    "@dpg-d252f79r0fns73dl8b70-a:5432/smartics_db"
)
logger.info("Effective DATABASE_URL (masked): %s", DATABASE_URL.replace("1NK7RyePl1dnGRDweGq17NEs1Zj98isu", "*****"))

# SQLAlchemy engine exposed for the app
engine: Engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
    future=True,
)

def get_engine() -> Engine:
    return engine


def load_to_postgres():
    df = pd.read_csv('/opt/airflow/crypto_data_cleaned.csv')
    df['fetched_at'] = datetime.utcnow()

    conn = None
    try:
        # Connect to Render's Postgres instance directly for ETL (psycopg2)
        conn = psycopg2.connect(
            host="dpg-d252f79r0fns73dl8b70-a",
            port=5432,
            database="smartics_db",
            user="smartics_db_user",
            password="1NK7RyePl1dnGRDweGq17NEs1Zj98isu",
            sslmode="require"  # Render typically requires SSL
        )
        cur = conn.cursor()

        # Ensure the Airflow-style table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id TEXT,
                symbol TEXT,
                current_price FLOAT,
                market_cap BIGINT,
                total_volume BIGINT,
                market_cap_to_volume FLOAT,
                fetched_at TIMESTAMP
            );
        """)

        for _, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO crypto_prices (
                        id, symbol, current_price, market_cap, total_volume, market_cap_to_volume, fetched_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))
            except Exception as inner_e:
                logger.error("Insert failed for row %s: %s", row.to_dict(), inner_e)

        conn.commit()
        cur.close()
    except Exception as e:
        logger.error("Error in load_to_postgres: %s", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
