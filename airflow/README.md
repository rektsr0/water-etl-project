# Apache Airflow (orchestration)

This repo includes a small DAG **`water_sensor_medallion_etl`** that runs:

1. **`run_medallion_etl`** — same as `python main.py` (PySpark medallion pipeline + SQL checks).
2. **`export_powerbi_csvs`** — `python scripts/export_for_powerbi.py`.

The DAG is defined in **`dags/water_etl_dag.py`**. It uses **`schedule_interval=None`** (manual trigger only). Set **`schedule_interval`** to `@daily` or a cron expression for automation.

---

## Option A — Docker (Docker Desktop on Windows)

From the **repository root**:

```bash
docker compose -f docker-compose.airflow.yml up --build
```

- **UI:** [http://localhost:8080](http://localhost:8080)  
- **Default login** (from init): username **`admin`**, password **`admin`** (change in production).

Wait until **scheduler** and **webserver** are healthy, then open the UI → **DAGs** → **`water_sensor_medallion_etl`** → **Trigger DAG** (play button).

**Notes**

- Project code is mounted at **`/opt/water-etl`**; DAGs are mounted at **`/opt/airflow/dags`**.
- **`WATER_ETL_PROJECT_ROOT`** is set so tasks find `main.py`.
- If permissions errors appear on Linux, set `AIRFLOW_UID` to your user id (see [Airflow Docker docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)).

**Stop:** `Ctrl+C` or `docker compose -f docker-compose.airflow.yml down`.

---

## Option B — Local Airflow (Linux / WSL, no Docker for Airflow)

Airflow does not officially support Windows as a **scheduler** host; use **WSL2** or the Docker path above.

1. Create a venv and install **both** pipeline deps and Airflow (use Airflow’s **constraint file** for the same Python version, e.g. 3.11):

   ```bash
   pip install -r requirements.txt
   pip install "apache-airflow==2.10.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.11.txt"
   ```

2. Point **`AIRFLOW_HOME`** at a folder **outside** the repo (or add `airflow_home/` to `.gitignore`):

   ```bash
   export AIRFLOW_HOME=~/airflow_home_water_etl
   airflow db migrate
   airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
   ```

3. Tell Airflow where the DAGs live (repo checkout path):

   ```bash
   export AIRFLOW__CORE__DAGS_FOLDER=/path/to/water-etl-project/airflow/dags
   ```

4. Install **Java 17** and set **`JAVA_HOME`** (required for PySpark in the same way as `python main.py`).

5. Start processes (two terminals):

   ```bash
   airflow scheduler
   ```

   ```bash
   airflow webserver --port 8080
   ```

6. Open the UI, enable the DAG, and trigger a run.

**Faster dev:** `airflow standalone` (single process; check Airflow docs for caveats).

---

## Changing the schedule

Edit **`dags/water_etl_dag.py`**: set `schedule_interval` to e.g. `timedelta(days=1)` or `"@daily"` and keep `catchup=False` unless you want backfill.

---

## Security

Do not use **`admin` / `admin`** outside local development. Use secrets managers and strong passwords in real deployments.
