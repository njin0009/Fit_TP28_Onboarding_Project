"""
app.py — SunSafety Flask API
FIT5120 Team 28 — US2.1

Builds the SQLite relational database from clean CSVs on startup.
This way we don't need to commit the .db file to git.
"""

import os
import sqlite3
import pandas as pd
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

BASE_DIR  = Path(__file__).resolve().parent
DB_PATH   = BASE_DIR / "sunsafety.db"
CLEAN_DIR = BASE_DIR / "data" / "clean"
UV_CSV    = CLEAN_DIR / "uv_monthly.csv"
CANCER_CSV= CLEAN_DIR / "cancer_data.csv"
CITIES_CSV= CLEAN_DIR / "cities.csv"


def build_db():
    """Create sunsafety.db from clean CSVs on first startup."""
    if DB_PATH.exists():
        print(f"[DB] Already exists: {DB_PATH}")
        return

    print("[DB] Building database from CSVs...")

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS cities (
            city_id   INTEGER PRIMARY KEY,
            city_name TEXT    NOT NULL UNIQUE,
            state     TEXT    NOT NULL,
            latitude  REAL    NOT NULL,
            longitude REAL    NOT NULL
        );
        CREATE TABLE IF NOT EXISTS uv_monthly (
            uv_id    INTEGER PRIMARY KEY,
            city_id  INTEGER NOT NULL REFERENCES cities(city_id),
            year     INTEGER NOT NULL,
            month    INTEGER NOT NULL CHECK(month BETWEEN 1 AND 12),
            avg_uvi  REAL    NOT NULL,
            UNIQUE(city_id, year, month)
        );
        CREATE TABLE IF NOT EXISTS cancer_data (
            cancer_id         INTEGER PRIMARY KEY,
            year              INTEGER NOT NULL UNIQUE,
            cancer_type       TEXT    NOT NULL,
            new_cases         INTEGER,
            crude_rate        REAL,
            asr_2001          REAL,
            asr_2025          REAL,
            deaths            INTEGER,
            mortality_crude   REAL,
            mortality_asr2001 REAL,
            mortality_asr2025 REAL,
            data_source       TEXT
        );
    """)
    conn.commit()

    cities = pd.read_csv(CITIES_CSV)
    cities.to_sql("cities", conn, if_exists="append", index=False)

    cancer = pd.read_csv(CANCER_CSV)
    cancer.to_sql("cancer_data", conn, if_exists="append", index=False)

    uv = pd.read_csv(UV_CSV)
    uv[["uv_id","city_id","year","month","avg_uvi"]].to_sql(
        "uv_monthly", conn, if_exists="append", index=False)

    conn.commit()
    n_uv     = conn.execute("SELECT COUNT(*) FROM uv_monthly").fetchone()[0]
    n_cancer = conn.execute("SELECT COUNT(*) FROM cancer_data").fetchone()[0]
    conn.close()
    print(f"[DB] Done — cancer={n_cancer}, uv={n_uv}")


build_db()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def health():
    return jsonify({
        "status":"ok","service":"SunSafety API",
        "team":"FIT5120 Team 28",
        "endpoints":["/api/historical-impacts","/api/cancer","/api/uv"]
    })


@app.route("/api/historical-impacts")
def historical_impacts():
    conn = get_db()
    cancer_rows = conn.execute("""
        SELECT year, new_cases, crude_rate,
               asr_2025 AS incidence_rate,
               deaths, mortality_asr2025 AS mortality_rate
        FROM cancer_data ORDER BY year
    """).fetchall()
    uv_rows = conn.execute("""
        SELECT c.city_name AS city, u.year, u.month, u.avg_uvi AS uvi
        FROM uv_monthly u
        JOIN cities c ON u.city_id = c.city_id
        ORDER BY c.city_name, u.year, u.month
    """).fetchall()
    conn.close()
    return jsonify({"cancer_data":[dict(r) for r in cancer_rows],
                    "uv_data":[dict(r) for r in uv_rows]})


@app.route("/api/cancer")
def cancer():
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
    city = request.args.get("city")
    conn = get_db()
    if city:
        rows = conn.execute("""
            SELECT c.city_name AS city, u.year, u.month, u.avg_uvi AS uvi
            FROM uv_monthly u JOIN cities c ON u.city_id = c.city_id
            WHERE c.city_name = ? ORDER BY u.year, u.month
        """, (city,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT c.city_name AS city, u.year, u.month, u.avg_uvi AS uvi
            FROM uv_monthly u JOIN cities c ON u.city_id = c.city_id
            ORDER BY c.city_name, u.year, u.month
        """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
