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
