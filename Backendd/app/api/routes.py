from flask import Blueprint, jsonify
from sqlalchemy import text
import pandas as pd
from ..database.connection import engine  # Absolute import from your structure

bp = Blueprint('api', __name__)  # API Blueprint

def _detect_columns(conn):
    res = conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'crypto_prices';
            """
        )
    )
    return {row[0] for row in res.fetchall()}

def _build_select_clause(columns: set) -> str:
    # Normalize to the richer Airflow schema if available, else fallback to app schema
    if "current_price" in columns:
        return """
            DISTINCT ON (symbol)
                id, symbol,
                current_price, market_cap, total_volume, market_cap_to_volume, fetched_at
        """
    else:
        # app schema with price
        return """
            DISTINCT ON (symbol)
                id, symbol,
                price AS current_price,
                NULL::bigint AS market_cap,
                NULL::bigint AS total_volume,
                NULL::double precision AS market_cap_to_volume,
                fetched_at
        """

@bp.route("/data")
def get_data():
    """Returns crypto price data using Pandas"""
    try:
        with engine.connect() as conn:
            cols = _detect_columns(conn)
            select_clause = _build_select_clause(cols)
            sql = f"""
                SELECT {select_clause}
                FROM crypto_prices
                ORDER BY symbol, fetched_at DESC;
            """
            df = pd.read_sql(text(sql), conn)
        return jsonify({
            "data": df.to_dict(orient="records"),
            "count": len(df),
            "status": "success"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/raw")
def get_raw():
    """Returns crypto price data using raw SQL"""
    try:
        with engine.connect() as conn:
            cols = _detect_columns(conn)
            select_clause = _build_select_clause(cols)
            sql = f"""
                SELECT {select_clause}
                FROM crypto_prices
                ORDER BY symbol, fetched_at DESC;
            """
            result = conn.execute(text(sql))
            rows = [dict(r) for r in result.mappings()]
        return jsonify({
            "data": rows,
            "count": len(rows),
            "status": "success"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
