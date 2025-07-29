from flask import Blueprint, jsonify
from sqlalchemy import text
import pandas as pd
from ..database.connection import engine  # Absolute import from your structure

# Create a Blueprint (modular routes)
bp = Blueprint('api', __name__)

@bp.route("/data")
def get_data():
    """Pandas DataFrame endpoint"""
    try:
        with engine.connect() as conn:
            df = pd.read_sql("SELECT DISTINCT ON (id) * FROM crypto_prices ORDER BY id, fetched_at DESC", conn)
            return jsonify({
                "data": df.to_dict(orient="records"),
                "count": len(df),
                "status": "success"
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/raw")
def get_raw():
    """Raw SQL endpoint"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT ON (id) * 
                FROM crypto_prices 
                ORDER BY id, fetched_at DESC;
            """))
            rows = [dict(row._mapping) for row in result]
            return jsonify({
                "data": rows,
                "count": len(rows),
                "status": "success"
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500