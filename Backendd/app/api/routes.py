from flask import Blueprint, jsonify
from sqlalchemy import text
import pandas as pd
from ..database.connection import engine  # Absolute import from your structure

bp = Blueprint('api', __name__)  # API Blueprint

@bp.route("/data")
def get_data():
    """Returns crypto price data using Pandas"""
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                text(
                    """
                    SELECT DISTINCT ON (symbol) id, symbol, price, fetched_at
                    FROM crypto_prices
                    ORDER BY symbol, fetched_at DESC;
                    """
                ),
                conn,
            )
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
            result = conn.execute(
                text(
                    """
                    SELECT DISTINCT ON (symbol) id, symbol, price, fetched_at
                    FROM crypto_prices
                    ORDER BY symbol, fetched_at DESC;
                    """
                )
            )
            rows = [dict(r) for r in result.mappings()]
        return jsonify({
            "data": rows,
            "count": len(rows),
            "status": "success"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
