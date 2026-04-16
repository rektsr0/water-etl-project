# Smart Water Infrastructure ETL Pipeline

This project simulates a utility-style batch pipeline: **ingest sensor readings → clean and engineer features with PySpark → persist to SQL → run analytical queries**.

It mirrors patterns common in regulated water utilities and Databricks-style Spark jobs (structured extract/transform/load, JDBC sinks, reproducible SQL analytics).

## What the pipeline does

1. **Extract** — Reads `data/water_sensor.csv` into a Spark `DataFrame` (header + inferred schema). Sample rows use **facility-style site names** (e.g. pump stations, pressure zones); one site includes a **step-down in pressure** over time for leak-detection demos in Power BI.
2. **Transform** — Drops incomplete rows, casts numeric fields, flags **possible leaks** when pressure falls below a threshold (`is_leak`), and builds **per-location aggregates** (average pressure and flow).
3. **Load** — Writes cleaned rows to `sensor_data` and aggregates to `sensor_agg_by_location`:
   - **Default:** SQLite database at `data/water.db` (no server required; uses Pandas for the write).
   - **Optional:** PostgreSQL via **Spark JDBC** (same pattern as Fairfax Water / cloud warehouses).

4. **Analytics** — Runs the SQL in `sql/queries.sql` against the loaded database (SQLite or PostgreSQL).

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
├── etl/
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── scripts/
│   └── export_for_powerbi.py
├── sql/
│   └── queries.sql
├── main.py
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

CSVs are written to **`data/exports/`** (`sensor_data.csv`, `sensor_agg_by_location.csv`, **`leaks_by_location.csv`** pre-aggregated for charts).

In **Power BI Desktop**: **Get data** > **Text/CSV** > load those files. In **Model**, confirm relationships: relate **`location`** between `sensor_agg_by_location` and `sensor_data` (many-to-one from detail to aggregate, or use only one table for a first dashboard).

**Suggested visuals (examples):**

| Visual | Fields |
|--------|--------|
| **Clustered bar** | `leaks_by_location`: `location`, `leak_count` |
| **Clustered column** | `sensor_agg_by_location`: `location`, `avg_pressure`, `avg_flow` |
| **Card** | `sensor_data`: average of `pressure` |
| **Line chart** | `sensor_data`: `timestamp` (axis), `pressure` (values); **Slicer** on `location` |

Toggle **is_leak** as a legend or filter to highlight low-pressure readings.

### B. Connect to PostgreSQL (Docker / local server)

With Postgres running (e.g. `docker compose up` and port published), use **Get data** > **PostgreSQL database**. Host **`localhost`**, database from **`.env`**, user/password from **`.env`**. Load tables **`sensor_data`** and **`sensor_agg_by_location`**; build the same visuals as above.

## Resume-oriented notes

- ETL is split into **modules** (`extract` / `transform` / `load`) and orchestrated by **`run_pipeline()`** in `main.py`.
- **Structured logging** is used across steps (not only prints).
- The load path demonstrates both **production-like JDBC** and a **zero-ops SQLite** path for hiring managers who want to run your repo locally.
