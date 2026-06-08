from __future__ import annotations

import json
from pathlib import Path

import polars as pl
from shapely import wkb as shapely_wkb

from src.transform import ibge as t_ibge


def test_clean_sidra_value() -> None:
    assert t_ibge.clean_sidra_value("-") == 0.0
    assert t_ibge.clean_sidra_value("..") is None
    assert t_ibge.clean_sidra_value("...") is None
    assert t_ibge.clean_sidra_value("X") is None
    assert t_ibge.clean_sidra_value("") is None
    assert t_ibge.clean_sidra_value(None) is None
    assert t_ibge.clean_sidra_value("1234") == 1234.0
    assert t_ibge.clean_sidra_value("not a number") is None


def test_validate_api_request_under_limit() -> None:
    assert t_ibge.validate_api_request(1, 10, 100) is True


def test_validate_api_request_over_limit_returns_message() -> None:
    msg = t_ibge.validate_api_request(2, 600, 100)  # 120000
    assert isinstance(msg, str)
    assert "limite de 100.000" in msg


def test_localidades_to_parquet(tmp_data_dir: Path, fixtures_dir: Path) -> None:
    states = json.loads((fixtures_dir / "ibge_states_se.json").read_text())
    out = t_ibge.localidades_to_parquet(states, "estados", parent_field="regiao.id")
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 4
    assert set(df.columns) == {"id", "nome", "parent_id"}
    assert df["parent_id"].to_list() == [3, 3, 3, 3]


def test_malhas_geojson_to_parquet(tmp_data_dir: Path, fixtures_dir: Path) -> None:
    gj = json.loads((fixtures_dir / "ibge_malha_estado_31.geojson").read_text())
    out = t_ibge.malhas_geojson_to_parquet(gj, "estados")
    df = pl.read_parquet(out)
    assert df.height == 1
    geom = shapely_wkb.loads(df["geometry"][0])
    assert geom.geom_type == "Polygon"
    assert df["code"][0] == "31"
    assert df["nivel"][0] == "estados"


def test_parse_milk_production(fixtures_dir: Path) -> None:
    payload = json.loads((fixtures_dir / "sidra_milk.json").read_text())
    df = t_ibge.parse_milk_production(payload, "N3")
    # MG: 2020, 2021, 2022(=0 from "-"). 2023 ".." dropped. SP: 2020, 2021. Total 5.
    assert df.height == 5
    mg_2020 = df.filter((pl.col("code") == 31) & (pl.col("year") == 2020))
    assert mg_2020["milk_production_liters"].item() == 9543210 * 1000
    mg_2022 = df.filter((pl.col("code") == 31) & (pl.col("year") == 2022))
    assert mg_2022["milk_production_liters"].item() == 0.0  # "-" → 0


def test_parse_milk_production_empty() -> None:
    df = t_ibge.parse_milk_production([], "N3")
    assert df.height == 0
    assert set(df.columns) == {"code", "nome", "year", "milk_production_liters", "geo_level"}
