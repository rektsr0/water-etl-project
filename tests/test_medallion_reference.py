"""Pandas reference tests for silver/gold rules (no Spark)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


def silver_from_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Same rules as build_silver (without bronze-only columns)."""
    df = df.dropna(subset=["pressure", "flow_rate", "location"]).copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["pressure"] = df["pressure"].astype(float)
    df["flow_rate"] = df["flow_rate"].astype(float)
    df["is_leak"] = (df["pressure"] < 30.0).astype(int)
    return df


def gold_from_silver(s: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    agg = (
        s.groupby("location", as_index=False)
        .agg(avg_pressure=("pressure", "mean"), avg_flow=("flow_rate", "mean"), reading_count=("pressure", "count"))
    )
    leaks = s[s["is_leak"] == 1].groupby("location").size().reset_index(name="leak_count")
    s = s.copy()
    s["reading_date"] = s["timestamp"].dt.date
    daily = (
        s.groupby(["reading_date", "location"], as_index=False)
        .agg(avg_pressure=("pressure", "mean"), avg_flow=("flow_rate", "mean"))
    )
    return agg, leaks, daily


def test_silver_row_count_matches_expected(sample_csv: Path):
    raw = pd.read_csv(sample_csv)
    s = silver_from_raw(raw)
    assert len(raw) == 22
    assert len(s) == 20


def test_leak_count_by_location(sample_csv: Path):
    raw = pd.read_csv(sample_csv)
    s = silver_from_raw(raw)
    leaks = s[s["is_leak"] == 1].groupby("location").size()
    assert leaks.sum() == 4
    assert set(leaks.index) == {
        "Poplar Run Pump Station",
        "Mason Hill Booster",
        "West Oak Pressure Zone",
        "Clifton Gateway PRV",
    }


def test_gold_weighted_avg_pressure(sample_csv: Path):
    raw = pd.read_csv(sample_csv)
    s = silver_from_raw(raw)
    agg, _, _ = gold_from_silver(s)
    w = (agg["avg_pressure"] * agg["reading_count"]).sum() / agg["reading_count"].sum()
    direct = s["pressure"].mean()
    assert abs(w - direct) < 1e-6
