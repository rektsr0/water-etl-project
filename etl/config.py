"""Resolve database settings from environment (no secrets hardcoded in code)."""

from __future__ import annotations

import os


class DatabaseConfigError(ValueError):
    """Raised when required PostgreSQL-related environment variables are missing."""


def _first(*keys: str) -> str | None:
    for k in keys:
        v = os.environ.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return None


def jdbc_url() -> str:
    """JDBC URL for Spark, built from ``WATER_ETL_JDBC_URL`` or host/port/database parts."""
    explicit = _first("WATER_ETL_JDBC_URL")
    if explicit:
        return explicit

    host = _first("WATER_ETL_PG_HOST") or "localhost"
    port = _first("WATER_ETL_PG_PORT") or "5432"
    db = _first("WATER_ETL_PG_DB", "POSTGRES_DB")
    if not db:
        raise DatabaseConfigError(
            "PostgreSQL JDBC URL not set. Define WATER_ETL_JDBC_URL, "
            "or set WATER_ETL_PG_DB / POSTGRES_DB with host/port via WATER_ETL_PG_HOST / WATER_ETL_PG_PORT."
        )
    return f"jdbc:postgresql://{host}:{port}/{db}"


def db_user() -> str:
    u = _first("WATER_ETL_DB_USER", "POSTGRES_USER")
    if not u:
        raise DatabaseConfigError(
            "Set WATER_ETL_DB_USER or POSTGRES_USER when using PostgreSQL."
        )
    return u


def db_password() -> str:
    p = _first("WATER_ETL_DB_PASSWORD", "POSTGRES_PASSWORD")
    if not p:
        raise DatabaseConfigError(
            "Set WATER_ETL_DB_PASSWORD or POSTGRES_PASSWORD when using PostgreSQL."
        )
    return p


def psycopg2_kwargs() -> dict:
    """Keyword args for psycopg2.connect from the same env vars as JDBC (Docker-friendly)."""
    host = _first("WATER_ETL_PG_HOST", "POSTGRES_HOSTNAME") or "localhost"
    port_s = _first("WATER_ETL_PG_PORT", "POSTGRES_PORT") or "5432"
    dbname = _first("WATER_ETL_PG_DB", "POSTGRES_DB")
    if not dbname:
        raise DatabaseConfigError("Set WATER_ETL_PG_DB or POSTGRES_DB for PostgreSQL analytics.")
    try:
        port = int(port_s)
    except ValueError as e:
        raise DatabaseConfigError(f"Invalid WATER_ETL_PG_PORT / POSTGRES_PORT: {port_s!r}") from e
    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": db_user(),
        "password": db_password(),
    }
