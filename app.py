"""
app.py — SunSafety Flask API
FIT5120 Team 28 — US2.1
Serves melanoma and UV data from SQLite for the Amplify frontend.
"""

import os
import sqlite3
from pathlib import Path
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # allow requests from Amplify frontend

# DB is in data/clean/ relative to this file
BASE_DIR = Path(__file__).resolve().parent
DB_PATH  = BASE_DIR / "data" / "clean" / "sunsafety.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "SunSafety API",
        "team": "FIT5120 Team 28",
        "endpoints": [
            "/api/historical-impacts",
            "/api/cancer",
            "/api/uv",
        ]
    })


@app.route("/api/historical-impacts")
def historical_impacts():
    """Main endpoint — returns both cancer and UV data for frontend charts."""
    conn = get_db()

    # Chart 1: cancer incidence & mortality over time
    cancer_rows = conn.execute("""
        SELECT year,
               new_cases,
               crude_rate,
               asr_2025   AS incidence_rate,
               deaths,
               mortality_asr2025 AS mortality_rate
        FROM cancer_data
        ORDER BY year
    """).fetchall()

    # Chart 2: monthly UV index by city and year
    uv_rows = conn.execute("""
        SELECT c.city_name AS city,
               u.year,
               u.month,
               u.avg_uvi  AS uvi
        FROM uv_monthly u
        JOIN cities c ON u.city_id = c.city_id
        ORDER BY c.city_name, u.year, u.month
    """).fetchall()

    conn.close()

    return jsonify({
        "cancer_data": [dict(r) for r in cancer_rows],
        "uv_data":     [dict(r) for r in uv_rows],
    })


@app.route("/api/cancer")
def cancer():
    """Cancer incidence and mortality data only."""
    conn = get_db()
    rows = conn.execute("""
        SELECT year, new_cases, crude_rate,
               asr_2025 AS incidence_rate,
               deaths, mortality_asr2025 AS mortality_rate
        FROM cancer_data ORDER BY year
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/uv")
def uv():
    """UV index data only — optionally filter by city."""
    from flask import request
    city = request.args.get("city")  # e.g. /api/uv?city=Melbourne

    conn = get_db()
    if city:
        rows = conn.execute("""
            SELECT c.city_name AS city, u.year, u.month, u.avg_uvi AS uvi
            FROM uv_monthly u
            JOIN cities c ON u.city_id = c.city_id
            WHERE c.city_name = ?
            ORDER BY u.year, u.month
        """, (city,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT c.city_name AS city, u.year, u.month, u.avg_uvi AS uvi
            FROM uv_monthly u
            JOIN cities c ON u.city_id = c.city_id
            ORDER BY c.city_name, u.year, u.month
        """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
