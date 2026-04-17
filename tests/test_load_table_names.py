"""Load module exposes expected medallion table names."""

from __future__ import annotations

from etl import load as load_mod


def test_table_constants():
    assert load_mod.TABLE_BRONZE == "bronze_sensor_data"
    assert load_mod.TABLE_SILVER == "silver_sensor_data"
    assert load_mod.TABLE_GOLD_AGG == "gold_sensor_agg_by_location"
    assert load_mod.TABLE_GOLD_LEAKS == "gold_leaks_by_location"
    assert load_mod.TABLE_GOLD_DAILY == "gold_daily_pressure_summary"
