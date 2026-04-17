"""sql/queries.sql targets gold-layer tables."""

from __future__ import annotations

from pathlib import Path


def test_queries_reference_gold_tables(project_root: Path):
    text = (project_root / "sql" / "queries.sql").read_text(encoding="utf-8")
    assert "gold_leaks_by_location" in text
    assert "gold_sensor_agg_by_location" in text
    assert "gold_daily_pressure_summary" in text
    assert "FROM sensor_data" not in text
