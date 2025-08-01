from datetime import datetime
import pandas as pd
import psycopg2
import logging

logger = logging.getLogger(__name__)

def load_to_postgres():
    df = pd.read_csv('/opt/airflow/crypto_data_cleaned.csv')
    df['fetched_at'] = datetime.utcnow()

    conn = None
    try:
        # Connect to Render's Postgres instance
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
