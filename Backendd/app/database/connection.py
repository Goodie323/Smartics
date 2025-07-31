import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set!")

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600
)

def get_engine():
    return engine

def create_table_if_not_exists():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                price NUMERIC,
                fetched_at TIMESTAMP DEFAULT NOW()
            );
        """))
