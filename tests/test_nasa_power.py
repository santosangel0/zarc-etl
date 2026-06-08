from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import responses

from src.collect import nasa_power as collect_np
from src.transform import nasa_power as t_np


def test_parse_point(fixtures_dir: Path) -> None:
    payload = json.loads((fixtures_dir / "nasa_power_jf_2024_01.json").read_text())
    df = t_np.parse_point(payload)
    assert df.height == 3
    assert df["lat"][0] == -21.7
    assert df["lon"][0] == -43.4
    # Sentinel -999 → null
    null_row = df.filter(pl.col("date") == pl.lit("2024-01-03").str.to_date())
    assert null_row["t2m"].item() is None
    assert null_row["rh2m"].item() == 80.0


def test_parse_point_empty() -> None:
    df = t_np.parse_point({})
    assert df.height == 0
    assert set(df.columns) == {"date", "lat", "lon", "t2m", "rh2m", "prectotcorr"}


@responses.activate
def test_fetch_point_caches(tmp_data_dir: Path, fixtures_dir: Path) -> None:
    body = (fixtures_dir / "nasa_power_jf_2024_01.json").read_text()
    responses.add(
        responses.GET,
        collect_np.POWER_URL,
        body=body,
        status=200,
        content_type="application/json",
    )
    out = collect_np.fetch_point(-21.7, -43.4, "2024-01-01", "2024-01-03")
    assert "geometry" in out

    # Second call must hit cache (no responses registered would raise otherwise)
    responses.reset()
    cached = collect_np.fetch_point(-21.7, -43.4, "2024-01-01", "2024-01-03")
    assert cached == out


def test_write_parquet(tmp_data_dir: Path, fixtures_dir: Path) -> None:
    payload = json.loads((fixtures_dir / "nasa_power_jf_2024_01.json").read_text())
    df = t_np.parse_point(payload)
    out = t_np.write_parquet(df, -21.7, -43.4, "2024-01-01", "2024-01-03")
    assert out.exists()
    rt = pl.read_parquet(out)
    assert rt.height == 3
