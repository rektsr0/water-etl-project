"""main._read_sql_queries splits multi-statement SQL."""

from __future__ import annotations

from pathlib import Path

import pytest

from main import _read_sql_queries


def test_read_sql_queries_splits(project_root: Path):
    sql_path = project_root / "sql" / "queries.sql"
    stmts = _read_sql_queries(sql_path)
    assert len(stmts) >= 4
    assert all("gold_" in s for s in stmts)
