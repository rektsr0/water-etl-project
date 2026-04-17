"""Integration: full PySpark pipeline (skipped without Java)."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.integration
def test_main_py_end_to_end(project_root: Path):
    if not shutil.which("java"):
        pytest.skip("Java not on PATH (required for PySpark)")
    result = subprocess.run(
        [sys.executable, str(project_root / "main.py")],
        cwd=project_root,
        capture_output=True,
        text=True,
        timeout=600,
        env={**os.environ},
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
    assert result.returncode == 0
    assert "Pipeline complete" in result.stdout or "pipeline" in result.stdout.lower()

    db = project_root / "data" / "water.db"
    assert db.is_file()
