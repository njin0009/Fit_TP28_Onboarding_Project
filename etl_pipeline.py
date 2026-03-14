"""
etl_pipeline.py
===============
FIT5120 Team 28 — US2.1 SunSafety Data Pipeline
Author : Shavinthi
Date   : 2026-03-13

PIPELINE STEPS
--------------
  Step 1 — Clean & wrangle UV CSVs   → data/clean/uv_monthly.csv
  Step 2 — Parse AIHW Excel Book 7   → data/clean/cancer_data.csv
  Step 3 — Build cities lookup table → data/clean/cities.csv
  Step 4 — Git add + commit + push   → origin/shavinthi
  Step 5 — Build SQLite relational DB → data/clean/sunsafety.db

EXCEL STRUCTURE (CDiA-2025-Book-7-...)
---------------------------------------
  Table S7.1 — Incidence  (1982–2021/2022)
  Table S7.2 — Mortality  (2007–2021)
  Header row : row index 5 (0-based)
  Columns    : data_type | cancer_site | year | sex | state |
               count | crude_rate | asr_2001 | asr_2025
  Filter     : cancer_site CONTAINS "Melanoma of the skin"
               sex   = "Persons"
               state = "Australia"

UV CSV STRUCTURE (ARPANSA data.gov.au)
---------------------------------------
  Filename   : uv-{city}-{year}.csv
  Columns    : Date-Time, Lat, Lon, UV_Index   (tab OR comma separated)
  Frequency  : 1-minute readings (1,440 rows/day)
  Cleaning   : UV_Index > 0  (daytime only), aggregate to monthly mean

USAGE
-----
    conda activate fit5120
    python etl_pipeline.py                        # full pipeline
    python etl_pipeline.py --no-push             # skip git push
    python etl_pipeline.py --data-dir ./mydata   # custom data folder
    python etl_pipeline.py --branch shavinthi    # default branch

REQUIREMENTS
------------
    pip install pandas openpyxl --break-system-packages
"""

import argparse
import glob
import logging
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
CITY_MAP = {
    "brisbane":  "Brisbane",
    "melbourne": "Melbourne",
    "sydney":    "Sydney",
}

CITIES_DATA = [
    # (city_id, city_name, state, latitude, longitude)
    (1, "Brisbane",  "QLD", -27.4698, 153.0251),
    (2, "Melbourne", "VIC", -37.8136, 144.9631),
    (3, "Sydney",    "NSW", -33.8688, 151.2093),
]

# Column names used after parsing header row 5 of the AIHW Excel
EXCEL_COLS = [
    "data_type", "cancer_site", "year", "sex", "state",
    "count", "crude_rate", "asr_2001", "asr_2025", "col9", "col10"
]

# Values AIHW uses to mean "no data"
AIHW_NA = ["n.a.", "n.p.", "N.A.", "N.P.", "na", "np", "", " "]

SCHEMA_SQL = """\
-- ============================================================
--  SunSafety Relational Schema — FIT5120 Team 28 US2.1
--  Sources: ARPANSA (UV) | AIHW CDiA Book 7 (Cancer)
-- ============================================================
PRAGMA foreign_keys = ON;

-- Dimension: city reference table
CREATE TABLE IF NOT EXISTS cities (
    city_id   INTEGER PRIMARY KEY,
    city_name TEXT    NOT NULL UNIQUE,
    state     TEXT    NOT NULL,
    latitude  REAL    NOT NULL,
    longitude REAL    NOT NULL
);

-- Fact: monthly average UV Index per city/year/month
CREATE TABLE IF NOT EXISTS uv_monthly (
    uv_id    INTEGER PRIMARY KEY,
    city_id  INTEGER NOT NULL REFERENCES cities(city_id),
    year     INTEGER NOT NULL,
    month    INTEGER NOT NULL CHECK(month BETWEEN 1 AND 12),
    avg_uvi  REAL    NOT NULL,
    UNIQUE(city_id, year, month)
);

-- Fact: national melanoma incidence and mortality per year
CREATE TABLE IF NOT EXISTS cancer_data (
    cancer_id         INTEGER PRIMARY KEY,
    year              INTEGER NOT NULL UNIQUE,
    cancer_type       TEXT    NOT NULL DEFAULT 'Melanoma of the skin',
    new_cases         INTEGER,           -- raw count, national
    crude_rate        REAL,              -- per 100,000 persons
    asr_2001          REAL,              -- age-standardised rate 2001 Aus pop
    asr_2025          REAL,              -- age-standardised rate 2025 Aus pop
    deaths            INTEGER,           -- raw mortality count
    mortality_crude   REAL,              -- mortality crude rate per 100,000
    mortality_asr2001 REAL,              -- mortality age-standardised 2001
    mortality_asr2025 REAL,              -- mortality age-standardised 2025
    data_source       TEXT DEFAULT 'AIHW CDiA Book 7 (2025)'
);

-- ── Query examples ───────────────────────────────────────────
-- Chart 1 — cancer incidence over time:
--   SELECT year, new_cases, asr_2025 AS incidence_rate
--   FROM cancer_data ORDER BY year;
--
-- Chart 2 — UV by city and month:
--   SELECT c.city_name, u.year, u.month, u.avg_uvi
--   FROM uv_monthly u JOIN cities c ON u.city_id = c.city_id
--   ORDER BY c.city_name, u.year, u.month;
"""


# ─────────────────────────────────────────────────────────────
# STEP 1 — CLEAN & WRANGLE UV DATA
# ─────────────────────────────────────────────────────────────
def clean_uv(data_dir: Path) -> pd.DataFrame:
    """
    Read all uv-{city}-{year}.csv files and produce monthly averages.

    Cleaning steps applied:
      1. utf-8-sig encoding  → strips invisible BOM from column names
      2. Strip whitespace    → normalise column names
      3. Parse Date-Time     → extract month (year taken from filename)
      4. Coerce UV_Index     → to numeric, drop non-numeric values
      5. Drop UV_Index <= 0  → removes night-time and sensor-off readings
      6. Group by year/month → mean of all valid daytime readings
      7. Round avg_uvi       → 2 decimal places

    Returns DataFrame columns:
        uv_id | city_id | city_name | year | month | avg_uvi
    """
    log.info("STEP 1 ── Cleaning UV data from: %s", data_dir)

    files = sorted(glob.glob(str(data_dir / "uv-*.csv")))
    if not files:
        log.warning("  No UV files found at: %s/uv-*.csv", data_dir)
        return pd.DataFrame(columns=["uv_id", "city_id", "city_name", "year", "month", "avg_uvi"])

    city_id_map = {row[1]: row[0] for row in CITIES_DATA}
    dfs = []

    for f in files:
        basename = os.path.basename(f)

        # ── Parse city + year from filename ───────────────────
        m = re.search(r"uv-([a-zA-Z]+)-(\d{4})\.csv", basename, re.IGNORECASE)
        if not m:
            log.warning("  SKIP — unrecognised filename: %s", basename)
            continue

        city_key = m.group(1).lower()
        year     = int(m.group(2))
        city     = CITY_MAP.get(city_key)
        if not city:
            log.warning("  SKIP — unknown city '%s' in: %s", city_key, basename)
            continue

        # ── Read CSV (handle both comma and tab separators) ───
        try:
            df = pd.read_csv(f, encoding="utf-8-sig", sep=None,
                             engine="python")  # sep=None auto-detects delimiter
        except Exception as e:
            log.error("  ERROR reading %s: %s", basename, e)
            continue

        df.columns = df.columns.str.strip()

        if "Date-Time" not in df.columns or "UV_Index" not in df.columns:
            log.warning("  SKIP — missing columns in %s. Found: %s",
                        basename, list(df.columns))
            continue

        # ── Clean ─────────────────────────────────────────────
        df["dt"]       = pd.to_datetime(df["Date-Time"], errors="coerce")
        df["UV_Index"] = pd.to_numeric(df["UV_Index"],   errors="coerce")

        rows_before = len(df)
        df = df.dropna(subset=["dt", "UV_Index"])
        df = df[df["UV_Index"] > 0]           # daytime only
        rows_after = len(df)

        log.info("  %-32s  %7d → %6d daytime rows", basename, rows_before, rows_after)

        if df.empty:
            log.warning("  SKIP — no valid rows after cleaning: %s", basename)
            continue

        df["year"]  = year                    # trust the filename
        df["month"] = df["dt"].dt.month
        df["city"]  = city

        # ── Aggregate to monthly mean ─────────────────────────
        monthly = (
            df.groupby(["year", "month", "city"])["UV_Index"]
              .mean()
              .round(2)
              .reset_index()
              .rename(columns={"UV_Index": "avg_uvi"})
        )
        dfs.append(monthly)

    if not dfs:
        log.error("  No valid UV files processed.")
        return pd.DataFrame(columns=["uv_id", "city_id", "city_name", "year", "month", "avg_uvi"])

    # ── Combine and tidy ──────────────────────────────────────
    uv = pd.concat(dfs, ignore_index=True)
    uv = uv.sort_values(["city", "year", "month"]).reset_index(drop=True)
    uv["city_id"]   = uv["city"].map(city_id_map)
    uv["city_name"] = uv["city"]
    uv = uv.drop(columns=["city"])
    uv.insert(0, "uv_id", range(1, len(uv) + 1))
    uv = uv[["uv_id", "city_id", "city_name", "year", "month", "avg_uvi"]]

    # ── Quality check ─────────────────────────────────────────
    log.info("  ✓ UV total rows: %d | cities: %s | years: %s",
             len(uv),
             sorted(uv["city_name"].unique()),
             sorted(uv["year"].unique()))

    coverage = uv.groupby(["city_name", "year"])["month"].count()
    gaps = coverage[coverage < 12]
    if not gaps.empty:
        log.warning("  ⚠ Incomplete months for:\n%s", gaps.to_string())

    return uv


# ─────────────────────────────────────────────────────────────
# STEP 2 — PARSE AIHW EXCEL
# ─────────────────────────────────────────────────────────────
def clean_cancer(data_dir: Path) -> pd.DataFrame:
    """
    Parse CDiA-2025-Book-7 Excel file.

    Table S7.1 (Incidence, 1982-2021):
      - Header at row 5 (0-indexed)
      - 11 columns: data_type, cancer_site, year, sex, state,
                    count, crude_rate, asr_2001, asr_2025, ...
      - Filter: Melanoma of the skin | Persons | Australia

    Table S7.2 (Mortality, 2007-2021):
      - Same structure, same filter

    Cleaning steps:
      1. Read with header=5 so row 5 becomes the column header
      2. Assign fixed column names (EXCEL_COLS)
      3. Strip whitespace from all string cells
      4. Replace AIHW NA strings (n.a., n.p.) with NaN
      5. Filter to melanoma / Persons / Australia rows
      6. Coerce year, count, rates to numeric
      7. Drop rows where new_cases is NaN (e.g. 2022 in incidence)
      8. Left-join incidence + mortality on year

    Returns DataFrame columns:
        cancer_id | year | cancer_type | new_cases | crude_rate |
        asr_2001 | asr_2025 | deaths | mortality_crude |
        mortality_asr2001 | mortality_asr2025 | data_source
    """
    log.info("STEP 2 ── Parsing AIHW Excel cancer data")

    # Find the Excel file
    excel_files = list(data_dir.glob("CDiA-*.xlsx")) + list(data_dir.glob("*.xlsx"))
    if not excel_files:
        log.error("  No Excel file found in: %s", data_dir)
        log.error("  Expected: CDiA-2025-Book-7-Cancer-incidence-and-mortality-*.xlsx")
        sys.exit(1)

    xlsx_path = excel_files[0]
    log.info("  File: %s", xlsx_path.name)

    def parse_sheet(sheet_name: str) -> pd.DataFrame:
        """Parse one AIHW sheet and filter to melanoma/Persons/Australia."""
        df = pd.read_excel(
            xlsx_path,
            sheet_name=sheet_name,
            header=5,                 # real header is at row index 5
            names=EXCEL_COLS,
            dtype=str                 # read all as string first; coerce later
        )

        # Strip whitespace from all string cells
        df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)

        # Replace AIHW "no data" strings with NaN
        df = df.replace(AIHW_NA, pd.NA)

        # Filter: Melanoma of the skin | Persons | Australia
        mask = (
            df["cancer_site"].astype(str).str.contains(
                "Melanoma of the skin", case=False, na=False
            ) &
            (df["sex"].astype(str).str.strip()   == "Persons") &
            (df["state"].astype(str).str.strip() == "Australia")
        )
        df = df[mask].copy()

        # Coerce numeric columns
        for col in ["year", "count", "crude_rate", "asr_2001", "asr_2025"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["year"])
        df["year"] = df["year"].astype(int)
        return df.sort_values("year").reset_index(drop=True)

    # ── Parse both sheets ─────────────────────────────────────
    inc = parse_sheet("Table S7.1")
    mor = parse_sheet("Table S7.2")

    log.info("  Incidence rows found: %d  (years %s–%s)",
             len(inc), inc["year"].min(), inc["year"].max())
    log.info("  Mortality rows found: %d  (years %s–%s)",
             len(mor), mor["year"].min(), mor["year"].max())

    # ── Rename for clarity before merge ──────────────────────
    inc = inc[["year", "count", "crude_rate", "asr_2001", "asr_2025"]].rename(columns={
        "count": "new_cases",
    })
    mor = mor[["year", "count", "crude_rate", "asr_2001", "asr_2025"]].rename(columns={
        "count":      "deaths",
        "crude_rate": "mortality_crude",
        "asr_2001":   "mortality_asr2001",
        "asr_2025":   "mortality_asr2025",
    })

    # ── Merge on year (left = keep all incidence years) ───────
    cancer = pd.merge(inc, mor, on="year", how="left")

    # Drop rows where incidence count is still NaN (e.g. 2022 = n.a.)
    cancer = cancer.dropna(subset=["new_cases"])
    cancer["new_cases"] = cancer["new_cases"].astype(int)

    # Safely cast deaths to int where not null
    cancer["deaths"] = pd.to_numeric(cancer["deaths"], errors="coerce")
    cancer.loc[cancer["deaths"].notna(), "deaths"] = \
        cancer.loc[cancer["deaths"].notna(), "deaths"].astype(int)

    cancer = cancer.sort_values("year").reset_index(drop=True)

    # ── Add metadata ─────────────────────────────────────────
    cancer.insert(0, "cancer_id",   range(1, len(cancer) + 1))
    cancer.insert(2, "cancer_type", "Melanoma of the skin")
    cancer["data_source"] = "AIHW CDiA Book 7 (2025)"

    final_cols = [
        "cancer_id", "year", "cancer_type",
        "new_cases", "crude_rate", "asr_2001", "asr_2025",
        "deaths", "mortality_crude", "mortality_asr2001", "mortality_asr2025",
        "data_source",
    ]
    cancer = cancer[final_cols]

    log.info("  ✓ Cancer rows: %d  (years %d–%d)",
             len(cancer), cancer["year"].min(), cancer["year"].max())

    return cancer


# ─────────────────────────────────────────────────────────────
# STEP 3 — CITIES LOOKUP TABLE
# ─────────────────────────────────────────────────────────────
def build_cities() -> pd.DataFrame:
    log.info("STEP 3 ── Building cities lookup table")
    cities = pd.DataFrame(
        CITIES_DATA,
        columns=["city_id", "city_name", "state", "latitude", "longitude"]
    )
    log.info("  ✓ %d cities: %s", len(cities), cities["city_name"].tolist())
    return cities


# ─────────────────────────────────────────────────────────────
# STEP 4 — SAVE CSVs + GIT PUSH
# ─────────────────────────────────────────────────────────────
def save_clean_csvs(uv, cancer, cities, out_dir: Path) -> None:
    log.info("STEP 4a ── Saving clean CSVs to: %s", out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    uv.to_csv(    out_dir / "uv_monthly.csv",   index=False)
    cancer.to_csv(out_dir / "cancer_data.csv",  index=False)
    cities.to_csv(out_dir / "cities.csv",        index=False)

    log.info("  ✓ uv_monthly.csv   — %d rows", len(uv))
    log.info("  ✓ cancer_data.csv  — %d rows", len(cancer))
    log.info("  ✓ cities.csv       — %d rows", len(cities))


def save_schema(repo_root: Path) -> Path:
    schema_dir = repo_root / "src" / "backend" / "epic2_awareness"
    schema_dir.mkdir(parents=True, exist_ok=True)
    path = schema_dir / "schema.sql"
    path.write_text(SCHEMA_SQL)
    log.info("  ✓ schema.sql → %s", path.relative_to(repo_root))
    return path


def git_push(repo_root: Path, files_to_add: list, branch: str = "shavinthi") -> None:
    """Stage, commit and push specific files to origin/{branch}."""
    log.info("STEP 4b ── Git push to origin/%s", branch)

    def run(cmd):
        r = subprocess.run(cmd, cwd=str(repo_root), capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(
                f"Command failed: {' '.join(cmd)}\n"
                f"stdout: {r.stdout}\nstderr: {r.stderr}"
            )
        return r.stdout.strip()

    current = run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    log.info("  Current branch: %s", current)
    if current != branch:
        run(["git", "checkout", branch])
        log.info("  Switched to: %s", branch)

    for f in files_to_add:
        p = Path(f)
        if p.exists():
            run(["git", "add", str(p)])
            log.info("  git add: %s", p.name)
        else:
            log.warning("  SKIP — not found: %s", p)

    status = run(["git", "status", "--porcelain"])
    if not status:
        log.info("  Nothing to commit — already up to date")
        return

    msg = "data(US2.1): add cleaned UV + cancer CSVs and schema"
    run(["git", "commit", "-m", msg])
    log.info("  ✓ Committed: '%s'", msg)

    run(["git", "push", "origin", branch])
    log.info("  ✓ Pushed → origin/%s", branch)


# ─────────────────────────────────────────────────────────────
# STEP 5 — BUILD SQLITE RELATIONAL DATABASE
# ─────────────────────────────────────────────────────────────
def build_database(uv, cancer, cities, db_path: Path) -> None:
    """
    Build a fresh SQLite database with 3 normalised tables.
    Enforces PRAGMA foreign_keys = ON and UNIQUE constraints.
    Runs verification queries after insert.
    """
    log.info("STEP 5 ── Building SQLite database: %s", db_path)

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")
    cur.executescript(SCHEMA_SQL)
    conn.commit()

    # Insert
    cities.to_sql("cities", conn, if_exists="append", index=False)

    cancer_db = cancer[[
        "cancer_id", "year", "cancer_type",
        "new_cases", "crude_rate", "asr_2001", "asr_2025",
        "deaths", "mortality_crude", "mortality_asr2001", "mortality_asr2025",
        "data_source",
    ]]
    cancer_db.to_sql("cancer_data", conn, if_exists="append", index=False)

    uv[["uv_id", "city_id", "year", "month", "avg_uvi"]].to_sql(
        "uv_monthly", conn, if_exists="append", index=False
    )
    conn.commit()

    # ── Verification ──────────────────────────────────────────
    log.info("  ── Row counts ──────────────────────────────────")
    for t in ["cities", "cancer_data", "uv_monthly"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        log.info("    %-16s : %d rows", t, n)

    fk_errs = conn.execute("PRAGMA foreign_key_check").fetchall()
    log.info("    FK violations    : %d %s",
             len(fk_errs), "✓" if not fk_errs else "✗ CHECK ERRORS")

    log.info("  ── Sample JOIN: uv_monthly + cities (Jan) ──────")
    rows = conn.execute("""
        SELECT c.city_name, u.year, u.month, u.avg_uvi
        FROM uv_monthly u JOIN cities c ON u.city_id = c.city_id
        WHERE u.month = 1 ORDER BY u.year DESC, c.city_name LIMIT 6
    """).fetchall()
    for r in rows:
        log.info("    %s  %d-Jan  avg_uvi=%.2f", r[0], r[1], r[3])

    log.info("  ── Cancer data (latest 5 years) ────────────────")
    rows = conn.execute("""
        SELECT year, new_cases, asr_2025, deaths, mortality_asr2025
        FROM cancer_data ORDER BY year DESC LIMIT 5
    """).fetchall()
    for r in rows:
        log.info("    %d  cases=%-6s  asr=%-5s  deaths=%-5s  mort_asr=%s",
                 r[0],
                 r[1] if r[1] is not None else "n/a",
                 r[2] if r[2] is not None else "n/a",
                 r[3] if r[3] is not None else "n/a",
                 r[4] if r[4] is not None else "n/a")

    conn.close()
    log.info("  ✓ Database ready")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="FIT5120 US2.1 ETL Pipeline")
    parser.add_argument("--data-dir", default="data",
                        help="Folder with raw CSVs + Excel (default: ./data)")
    parser.add_argument("--no-push",  action="store_true",
                        help="Skip git push")
    parser.add_argument("--branch",   default="shavinthi",
                        help="Git branch (default: shavinthi)")
    args = parser.parse_args()

    # ── Paths ─────────────────────────────────────────────────
    script_dir = Path(__file__).resolve().parent
    repo_root  = script_dir
    for _ in range(6):
        if (repo_root / ".git").exists():
            break
        repo_root = repo_root.parent
    else:
        repo_root = script_dir

    data_dir  = Path(args.data_dir).resolve()
    clean_dir = repo_root / "data" / "clean"
    db_path   = clean_dir / "sunsafety.db"

    log.info("══════════════════════════════════════════════════")
    log.info("  FIT5120 Team 28 — US2.1 ETL Pipeline")
    log.info("  Repo : %s", repo_root)
    log.info("  Data : %s", data_dir)
    log.info("══════════════════════════════════════════════════")

    # ── Run ───────────────────────────────────────────────────
    uv     = clean_uv(data_dir)
    cancer = clean_cancer(data_dir)
    cities = build_cities()

    save_clean_csvs(uv, cancer, cities, clean_dir)
    schema_path = save_schema(repo_root)

    if not args.no_push:
        try:
            git_push(
                repo_root,
                [clean_dir / "uv_monthly.csv",
                 clean_dir / "cancer_data.csv",
                 clean_dir / "cities.csv",
                 schema_path],
                branch=args.branch,
            )
        except RuntimeError as e:
            log.error("Git push failed:\n%s", e)
            log.info("Continuing to build DB anyway…")
    else:
        log.info("STEP 4b ── Skipped (--no-push)")

    build_database(uv, cancer, cities, db_path)

    # Copy DB next to script so Flask app finds it easily
    import shutil
    shutil.copy2(db_path, script_dir / "sunsafety.db")

    log.info("")
    log.info("══════════════════════════════════════════════════")
    log.info("  ALL DONE")
    log.info("  Clean CSVs → %s", clean_dir)
    log.info("  Database   → %s", db_path)
    log.info("══════════════════════════════════════════════════")


if __name__ == "__main__":
    main()
