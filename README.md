# Smart Water Infrastructure ETL Pipeline

This project simulates a utility-style batch pipeline: **ingest raw sensor data (bronze) → cleanse and engineer features (silver) → reporting-ready aggregates (gold) → SQL analytics and Power BI exports**.

It mirrors **medallion-style** layering and Databricks-style Spark jobs (JDBC sinks, curated gold for consumers).

## Medallion layers (bronze / silver / gold)

| Layer | Table(s) | Purpose |
|-------|-----------|---------|
| **Bronze** | `bronze_sensor_data` | Raw ingested rows from `data/water_sensor.csv` plus **`ingested_at`** and **`source_file`**. Close to source; nulls and raw types preserved. |
| **Silver** | `silver_sensor_data` | Cleansed, typed readings with **`is_leak`** (pressure &lt; 30 psi demo rule). Core dataset for downstream jobs and detail visuals. |
| **Gold** | `gold_sensor_agg_by_location`, `gold_leaks_by_location`, `gold_daily_pressure_summary` | Business-ready summaries: per-site averages (**with `reading_count`** for weighted means), leak counts, and daily averages by location for dashboards. |

**Interview line:** *“Bronze stores raw ingested telemetry; silver applies validation, typing, and leak detection; gold holds curated aggregates so analytics and Power BI read stable reporting tables instead of raw operational data.”*

### Data dictionary (short)

- **Bronze** — Source-shaped records; may include nulls; includes lineage metadata.
- **Silver** — Validated sensor readings, consistent types, derived **`is_leak`** flag.
- **Gold** — Metrics and rollups for SQL in `sql/queries.sql` and for **`scripts/export_for_powerbi.py`**.

## What the pipeline does (stage order)

1. **Extract** — Read CSV into Spark as **bronze** (minimal change + **`ingested_at`** / **`source_file`**). Sample data uses **facility-style site names**; one site has a **pressure step-down** for leak demos.
2. **Silver** — `build_silver(bronze)`: drop incomplete rows, cast fields, add **`is_leak`**.
3. **Gold** — `build_gold(silver)`: per-location averages + **`reading_count`**, leak counts by location, daily averages by location.
4. **Load** — Write all layers to SQLite (`data/water.db`) or PostgreSQL via JDBC.
5. **Analytics** — `sql/queries.sql` targets **gold** (weighted overall pressure uses **`reading_count`** on `gold_sensor_agg_by_location`).

## Tech used

- **Python**
- **PySpark** (batch transforms)
- **SQL** (SQLite or PostgreSQL)
- **Pandas** (optional helper for SQLite load; convenient for local demos)

## Prerequisites

- **Python 3.10+** recommended
- **Java 11 or 17** (required by Spark when running locally). Set `JAVA_HOME` if Spark cannot find the JVM. The **Docker** image installs a JRE automatically.

## Configuration (no secrets in git)

1. Copy the example env file and edit values locally (never commit real credentials):

```bash
cp .env.example .env
```

2. Set database credentials and optional JDBC overrides in `.env`. The app reads **`WATER_ETL_DB_USER` / `WATER_ETL_DB_PASSWORD`** (or Docker-style **`POSTGRES_USER` / `POSTGRES_PASSWORD`**). When PostgreSQL is enabled, **password and user are required**—there are no default secrets in code.

## How to run

### Docker Compose (PostgreSQL + ETL)

From the project root (requires [Docker](https://docs.docker.com/get-docker/)):

```bash
cp .env.example .env
# Edit .env — set POSTGRES_USER and POSTGRES_PASSWORD (and optionally POSTGRES_DB)

docker compose up --build
```

This starts **Postgres**, waits until it is healthy, then runs the **ETL container** once (`main.py`). Data is written to Postgres via JDBC; analytics run over the same database. Postgres data is stored in the named volume `pgdata` Port `POSTGRES_PUBLISH_PORT` (default `5432`) is published to the host.

On first run, Spark may download the PostgreSQL JDBC driver (Maven); outbound network access is required once.

### Local Python (SQLite by default)

```bash
cd water-etl-project
python -m venv .venv
# Windows: .venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

With defaults, this writes **`data/water.db`** and runs analytics against SQLite.

- **Analytics only** (after a successful run):  
  `python main.py --analytics-only`

- **Verbose logs:**  
  `python main.py -v`

### Local Python + PostgreSQL (Spark JDBC)

1. Create a database and user, then set variables **in `.env` or your shell** (no passwords in the repo).

2. Enable Postgres mode and supply credentials, for example:

```text
WATER_ETL_USE_POSTGRES=1
WATER_ETL_JDBC_URL=jdbc:postgresql://localhost:5432/water_db
WATER_ETL_DB_USER=your_user
WATER_ETL_DB_PASSWORD=your_secret
```

Alternatively omit `WATER_ETL_JDBC_URL` and set **`WATER_ETL_PG_HOST`**, **`WATER_ETL_PG_PORT`**, **`WATER_ETL_PG_DB`**, plus user/password as above. The same variables power analytics via `psycopg2`.

## Project layout

```text
water-etl-project/
├── data/
│   ├── exports/          # CSVs from export_for_powerbi.py (gitignored)
│   └── water_sensor.csv
├── docs/
│   └── POWER_BI.md       # Power BI walkthrough (gold + silver exports)
├── etl/
│   ├── config.py
│   ├── extract.py        # Bronze extract only
│   ├── transform.py      # build_silver, build_gold
│   └── load.py           # load_medallion → bronze/silver/gold tables
├── scripts/
│   └── export_for_powerbi.py
├── sql/
│   └── queries.sql       # Gold-first analytics
├── main.py               # Orchestrates layer order
├── docker-compose.yml
├── Dockerfile
├── docker-entrypoint.sh
├── .env.example
├── requirements.txt
└── README.md
```

## Power BI Desktop

**Step-by-step dashboard build (clicks, visuals, model):** see **[docs/POWER_BI.md](docs/POWER_BI.md)**.

Power BI cannot open the SQLite file directly without an ODBC driver. Two easy options:

### A. CSV export (simplest)

After the pipeline has loaded data:

```bash
python scripts/export_for_powerbi.py
```

CSVs are written to **`data/exports/`**: **gold** (`gold_sensor_agg_by_location.csv`, `gold_leaks_by_location.csv`, `gold_daily_pressure_summary.csv`), **silver** (`silver_sensor_data.csv`), plus **aliases** `sensor_data.csv`, `sensor_agg_by_location.csv`, `leaks_by_location.csv` for older tutorials.

In **Power BI Desktop**: **Get data** > **Text/CSV** > load gold files for summaries, silver (or `sensor_data`) for detail. In **Model**, relate **`location`** between gold aggregate and silver detail when both are used.

**Suggested visuals (examples):**

| Visual | Fields |
|--------|--------|
| **Clustered bar** | `gold_leaks_by_location` / `leaks_by_location`: `location`, `leak_count` |
| **Clustered column** | `gold_sensor_agg_by_location`: `location`, `avg_pressure`, `avg_flow` |
| **Card** | `silver_sensor_data` / `sensor_data`: **Average** of `pressure` |
| **Line chart** | Silver: `timestamp` (axis), **Average** of `pressure`; **Slicer** on `location` |
| **Line (daily)** | `gold_daily_pressure_summary`: `reading_date`, `avg_pressure`, legend `location` |

Toggle **is_leak** as a legend or filter to highlight low-pressure readings.

### B. Connect to PostgreSQL (Docker / local server)

With Postgres running (e.g. `docker compose up` and port published), use **Get data** > **PostgreSQL database**. Load **`gold_*`** tables for reporting and **`silver_sensor_data`** for detail.

## Resume-oriented notes

- **Medallion layout** is explicit: bronze → silver → gold table names and stage order in **`main.run_pipeline()`**.
- **Extract** is bronze-only; **transform** exposes **`build_silver`** and **`build_gold`**; **load** uses **`load_medallion`**.
- **Structured logging** across stages; JDBC (Postgres) and SQLite paths for local demos.
