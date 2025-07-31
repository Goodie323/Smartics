from flask import Flask, jsonify
from flask_cors import CORS
from app.api.routes import bp  # Import the Blueprint
from app.database import create_table_if_not_exists  # Optional: create table on startup

app = Flask(__name__)
CORS(app)

# Optional: ensure DB table exists before first request
@app.before_first_request
def init_db():
    create_table_if_not_exists()

# Register blueprint with URL prefix
app.register_blueprint(bp, url_prefix='/api')

@app.route("/")
def home():
    return jsonify({
        "status": "API is running ✅",
        "endpoints": {
            "/api/data": "Pandas DataFrame",
            "/api/raw": "Raw SQL results"
        }
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
