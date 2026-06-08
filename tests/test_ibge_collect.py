from __future__ import annotations

from pathlib import Path

import responses

from src.collect import ibge as collect_ibge


def _load(fixtures_dir: Path, name: str) -> str:
    return (fixtures_dir / name).read_text()


def test_get_regions_static() -> None:
    regs = collect_ibge.get_regions()
    assert {r["nome"] for r in regs} == {"Norte", "Nordeste", "Sudeste", "Sul", "Centro-Oeste"}
    assert all(isinstance(r["id"], int) for r in regs)


@responses.activate
def test_get_states_for_region(fixtures_dir: Path) -> None:
    responses.add(
        responses.GET,
        f"{collect_ibge.LOC_BASE}/regioes/3/estados",
        body=_load(fixtures_dir, "ibge_states_se.json"),
        status=200,
        content_type="application/json",
    )
    out = collect_ibge.get_states(region_code=3)
    assert {s["sigla"] for s in out} == {"MG", "ES", "RJ", "SP"}


@responses.activate
def test_fetch_geojson_estado(fixtures_dir: Path) -> None:
    responses.add(
        responses.GET,
        f"{collect_ibge.MALHAS_BASE}/estados/31",
        body=_load(fixtures_dir, "ibge_malha_estado_31.geojson"),
        status=200,
        content_type="application/json",
        match=[responses.matchers.query_param_matcher({"formato": "application/vnd.geo+json"})],
    )
    gj = collect_ibge.fetch_geojson("estados", 31)
    assert gj["type"] == "FeatureCollection"


def test_fetch_geojson_invalid_level() -> None:
    try:
        collect_ibge.fetch_geojson("paises", 1)
    except ValueError as exc:
        assert "Invalid level" in str(exc)
    else:
        raise AssertionError("expected ValueError")


@responses.activate
def test_fetch_milk_production_raw(fixtures_dir: Path) -> None:
    responses.add(
        responses.GET,
        f"{collect_ibge.AGREGADOS_BASE}/74/periodos/2020|2021|2022|2023/variaveis/106",
        body=_load(fixtures_dir, "sidra_milk.json"),
        status=200,
        content_type="application/json",
    )
    payload = collect_ibge.fetch_milk_production_raw("N3", [31, 35], [2020, 2021, 2022, 2023])
    assert isinstance(payload, list)
    assert payload[0]["resultados"][0]["series"][0]["localidade"]["nome"] == "Minas Gerais"


def test_fetch_milk_production_invalid_geo_level() -> None:
    try:
        collect_ibge.fetch_milk_production_raw("ZZ", [1], [2020])
    except ValueError as exc:
        assert "Invalid geo_level" in str(exc)
    else:
        raise AssertionError("expected ValueError")
