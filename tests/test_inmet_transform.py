from __future__ import annotations

import json
from pathlib import Path

import polars as pl
from shapely import wkb as shapely_wkb

from src.transform import inmet as t_inmet


def test_normalize_via_compute_itu_simple() -> None:
    out = t_inmet.compute_itu(pl.lit(25.0), pl.lit(70.0))
    val = pl.select(out).item()
    expected = 0.8 * 25.0 + (70.0 * (25.0 - 14.3)) / 100 + 46.3
    assert abs(val - expected) < 1e-9


def test_qc_temperature_masks_out_of_range() -> None:
    df = pl.DataFrame({"t": [-20.0, -10.0, 25.0, 50.0, 55.0]})
    df = df.with_columns(t_inmet.qc_temperature(pl.col("t")).alias("t"))
    assert df["t"].to_list() == [None, -10.0, 25.0, 50.0, None]


def test_qc_humidity_masks_out_of_range() -> None:
    df = pl.DataFrame({"h": [-5.0, 0.0, 50.0, 100.0, 105.0]})
    df = df.with_columns(t_inmet.qc_humidity(pl.col("h")).alias("h"))
    assert df["h"].to_list() == [None, 0.0, 50.0, 100.0, None]


def test_stations_to_parquet(tmp_data_dir: Path, fixtures_dir: Path) -> None:
    payload = json.loads((fixtures_dir / "inmet_stations.json").read_text())
    out = t_inmet.stations_to_parquet(payload)
    assert out.exists()

    df = pl.read_parquet(out)
    assert df.height == 2  # null-coord row dropped
    assert "geometry" in df.columns
    geom = shapely_wkb.loads(df["geometry"][0])
    assert geom.geom_type == "Point"
    # Snake_case normalization applied
    for col in ("cd_estacao", "dc_nome", "vl_latitude", "vl_longitude", "sg_estado"):
        assert col in df.columns


def test_daily_from_live_payload_applies_qc_and_itu(fixtures_dir: Path) -> None:
    payload = json.loads((fixtures_dir / "inmet_daily.json").read_text())
    df = t_inmet.daily_from_live_payload(payload, "A001")
    assert df.height == 3
    # Day 2: temp_med=55 → masked; umid_min=-5 → masked
    row2 = df.filter(pl.col("date") == pl.lit("2023-01-02").str.to_date())
    assert row2["temp_med"].item() is None
    assert row2["umid_min"].item() is None
    # Day 1 ITU: 0.8*25 + 70*(25-14.3)/100 + 46.3 = 20 + 7.49 + 46.3 = 73.79
    row1 = df.filter(pl.col("date") == pl.lit("2023-01-01").str.to_date())
    assert abs(row1["itu_med"].item() - 73.79) < 1e-6


def test_daily_from_live_payload_empty() -> None:
    df = t_inmet.daily_from_live_payload([], "A001")
    assert df.height == 0
    assert set(df.columns) == {
        "date", "station_code", "temp_med", "temp_max", "umid_med", "umid_min", "itu_med", "itu_max",
    }


def test_parse_inmet_csv(fixtures_dir: Path) -> None:
    csv_path = fixtures_dir / "INMET_SE_MG_A001_TEST_01-01-2024_T_31-12-2024.CSV"
    df = t_inmet.parse_inmet_csv(csv_path)
    assert df is not None
    assert df.height == 7
    assert "cd_estacao" in df.columns
    assert df["cd_estacao"][0] == "A001"
    assert "data" in df.columns
    # Column normalization (lowercase, no accents/punct)
    assert "temperatura_do_ar_bulbo_seco_horaria_c" in df.columns
    assert "umidade_relativa_do_ar_horaria" in df.columns


def test_history_pipeline_end_to_end(tmp_data_dir: Path, fixtures_dir: Path) -> None:
    csv_path = fixtures_dir / "INMET_SE_MG_A001_TEST_01-01-2024_T_31-12-2024.CSV"
    rows = t_inmet.ingest_history_to_duckdb([csv_path], station_lookup={"A001"})
    assert rows == 7

    out = t_inmet.export_history_parquet()
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 7
    assert df["cd_estacao"].n_unique() == 1
    # Sort verified: data column ascending
    dates = df["data"].to_list()
    assert dates == sorted(dates)

    # Daily aggregation with QC + ITU
    daily_out = t_inmet.daily_from_history_parquet()
    daily = pl.read_parquet(daily_out)
    assert daily.height == 2  # two distinct dates
    # Day 2 has a temp=55 hour → masked; mean of remaining hours should still pass QC range
    day2 = daily.filter(pl.col("data") == pl.lit("2024-01-02").str.to_date())
    assert day2["temp_max"].item() <= 50  # masked 55 should not appear
