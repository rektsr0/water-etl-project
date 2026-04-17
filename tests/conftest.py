"""Pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def sample_csv(project_root: Path) -> Path:
    p = project_root / "data" / "water_sensor.csv"
    assert p.is_file(), f"Missing fixture data: {p}"
    return p
