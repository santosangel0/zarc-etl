from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture
def tmp_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _ensure_no_real_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INMET_TOKEN", "TEST_TOKEN")
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    if "DATA_DIR" not in os.environ:
        monkeypatch.setenv("DATA_DIR", "")
