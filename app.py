import os
import psycopg2
from flask import Flask, request, jsonify

app = Flask(__name__)
DB_URL = os.environ["DATABASE_URL"]
API_KEY = os.environ["API_KEY"]

@app.route("/")
def home():
    return "API is running"

@app.route("/data", methods=["POST"])
def data():
    if request.headers.get("x-api-key") != API_KEY:
        return jsonify(error="unauthorized"), 401
    d = request.get_json(force=True)
    conn = psycopg2.connect(DB_URL)
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO sensor_data (temperature, humidity) VALUES (%s, %s)",
            (d["temperature"], d["humidity"]))
    conn.close()
    return jsonify(ok=True), 201

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)