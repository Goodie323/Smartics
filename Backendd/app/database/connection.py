import os
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Basic logging setup (customize handlers/level as needed)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set!")

# Create engine with connection-pool tuning
engine: Engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
    future=True,  # use 2.0-style if available
)


def get_engine() -> Engine:
    return engine


def ensure_table_and_index() -> None:
    """
    Creates the crypto_prices table and supporting index if they don't exist.
    """
    with engine.begin() as conn:
        logger.info("Ensuring table crypto_prices exists with proper schema.")
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS crypto_prices (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    price NUMERIC NOT NULL,
                    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        )
        logger.info("Ensuring index on (symbol, fetched_at DESC) exists.")
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_crypto_prices_symbol_fetched_at
                    ON crypto_prices (symbol, fetched_at DESC);
                """
            )
        )


def insert_price(symbol: str, price: float, fetched_at: Optional[datetime] = None) -> None:
    """
    Inserts a new price record for a symbol.
    """
    with engine.begin() as conn:
        if fetched_at:
            conn.execute(
                text(
                    """
                    INSERT INTO crypto_prices (symbol, price, fetched_at)
                    VALUES (:symbol, :price, :fetched_at);
                    """
                ),
                {"symbol": symbol, "price": price, "fetched_at": fetched_at},
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO crypto_prices (symbol, price)
                    VALUES (:symbol, :price);
                    """
                ),
                {"symbol": symbol, "price": price},
            )
    logger.debug("Inserted price for symbol=%s price=%s fetched_at=%s", symbol, price, fetched_at)


def get_latest_per_symbol() -> List[Dict[str, Any]]:
    """
    Returns the most recent row per symbol.
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT DISTINCT ON (symbol) id, symbol, price, fetched_at
                FROM crypto_prices
                ORDER BY symbol, fetched_at DESC;
                """
            )
        )
        rows = [dict(row) for row in result]
    logger.debug("Fetched latest per symbol: %s", rows)
    return rows


# Optional: if you ever needed the latest row by id (redundant if id is PK)
def get_latest_by_id() -> List[Dict[str, Any]]:
    """
    Returns the latest row per id (id is primary key so this is equivalent to all rows).
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT *
                FROM crypto_prices
                ORDER BY id, fetched_at DESC;
                """
            )
        )
        rows = [dict(row) for row in result]
    return rows


# Initialization on import / startup
ensure_table_and_index()


# Example usage if run as script
if __name__ == "__main__":
    # Insert a sample price
    insert_price("BTC", 60000.0)
    insert_price("ETH", 3500.5)
    # Fetch latest per symbol
    latest = get_latest_per_symbol()
    for row in latest:
        print(f"{row['symbol']} @ {row['price']} fetched_at {row['fetched_at']}")