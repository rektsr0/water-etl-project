"""etl/config.py environment handling."""

from __future__ import annotations

import pytest

from etl.config import DatabaseConfigError, jdbc_url


def test_jdbc_url_explicit(monkeypatch):
    monkeypatch.setenv("WATER_ETL_JDBC_URL", "jdbc:postgresql://db:5432/water_db")
    assert jdbc_url() == "jdbc:postgresql://db:5432/water_db"


def test_jdbc_url_requires_database(monkeypatch):
    monkeypatch.delenv("WATER_ETL_JDBC_URL", raising=False)
    monkeypatch.delenv("WATER_ETL_PG_DB", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    monkeypatch.setenv("WATER_ETL_PG_HOST", "localhost")
    with pytest.raises(DatabaseConfigError):
        jdbc_url()


def test_jdbc_url_from_parts(monkeypatch):
    monkeypatch.delenv("WATER_ETL_JDBC_URL", raising=False)
    monkeypatch.setenv("WATER_ETL_PG_HOST", "db")
    monkeypatch.setenv("WATER_ETL_PG_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "water_db")
    assert jdbc_url() == "jdbc:postgresql://db:5432/water_db"
