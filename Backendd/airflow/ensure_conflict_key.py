import os
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL") or "postgresql://smartics_db_user:1NK7RyePl1dnGRDweGq17NEs1Zj98isu@dpg-d252f79r0fns73dl8b70-a.oregon-postgres.render.com:5432/smartics_db?sslmode=require"
if "sslmode" not in DATABASE_URL:
    sep = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{sep}sslmode=require"

conn = None
cur = None
try:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Add unique constraint if it doesn't exist
    # We name it so repeated runs won't error out if it already exists
    cur.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_class t ON c.conrelid = t.oid
            WHERE c.conname = 'uniq_symbol_fetched_at'
        ) THEN
            ALTER TABLE crypto_prices
              ADD CONSTRAINT uniq_symbol_fetched_at UNIQUE (symbol, fetched_at);
        END IF;
    END
    $$;
    """)

    conn.commit()
    logger.info("Ensured unique constraint on (symbol, fetched_at)")
except Exception:
    if conn:
        conn.rollback()
    logger.exception("Failed to ensure conflict key")
    raise
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
