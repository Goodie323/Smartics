from flask import Flask, jsonify
from flask_cors import CORS
from app.api.routes import bp  # Import the Blueprint


app = Flask(__name__)
CORS(app)


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
