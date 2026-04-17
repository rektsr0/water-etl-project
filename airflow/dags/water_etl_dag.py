"""Airflow DAG: medallion ETL (PySpark) and Power BI CSV export."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG uses schedule_interval=None; edit the DAG object to add a schedule.
DEFAULT_ARGS = {
    "owner": "water-etl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _project_root() -> Path:
    env = os.environ.get("WATER_ETL_PROJECT_ROOT")
    if env:
        return Path(env)
    return Path(__file__).resolve().parent.parent.parent


def _run_cmd(args: list[str]) -> None:
    root = _project_root()
    main_py = root / "main.py"
    if not main_py.is_file():
        raise FileNotFoundError(f"main.py not found under {root}")
    exe = sys.executable
    result = subprocess.run(
        [exe, str(main_py), *args],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {result.returncode}: {args}")


def run_etl_pipeline() -> None:
    """Run ``python main.py`` (ETL + SQL)."""
    _run_cmd([])


def export_powerbi_csvs() -> None:
    """Run ``scripts/export_for_powerbi.py``."""
    root = _project_root()
    script = root / "scripts" / "export_for_powerbi.py"
    if not script.is_file():
        raise FileNotFoundError(f"Missing {script}")
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if result.returncode != 0:
        raise RuntimeError("export_for_powerbi.py failed")


with DAG(
    dag_id="water_sensor_medallion_etl",
    default_args=DEFAULT_ARGS,
    description="Bronze/silver/gold water sensor pipeline (PySpark) + Power BI exports",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["water", "etl", "pyspark", "medallion"],
) as dag:
    t_etl = PythonOperator(
        task_id="run_medallion_etl",
        python_callable=run_etl_pipeline,
    )
    t_export = PythonOperator(
        task_id="export_powerbi_csvs",
        python_callable=export_powerbi_csvs,
    )
    t_etl >> t_export
