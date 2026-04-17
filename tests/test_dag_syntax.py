"""DAG module compiles."""

from __future__ import annotations

from pathlib import Path


def test_airflow_dag_compiles(project_root: Path):
    path = project_root / "airflow" / "dags" / "water_etl_dag.py"
    compile(path.read_text(encoding="utf-8"), str(path), "exec")
