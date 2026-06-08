"""Collectors for IBGE: Localidades v1, Malhas v3, Agregados (SIDRA) v3."""

from __future__ import annotations

from collections.abc import Iterable

import requests

from src.utils import get_logger, get_session

log = get_logger(__name__)

LOC_BASE = "https://servicodados.ibge.gov.br/api/v1/localidades"
MALHAS_BASE = "https://servicodados.ibge.gov.br/api/v3/malhas"
AGREGADOS_BASE = "https://servicodados.ibge.gov.br/api/v3/agregados"

REGIONS = {
    "Norte": 1,
    "Nordeste": 2,
    "Sudeste": 3,
    "Sul": 4,
    "Centro-Oeste": 5,
}

VALID_MALHA_LEVELS = ("regioes", "estados", "mesorregioes", "municipios")
INTRA_MAP = {
    "regioes": "regiao",
    "estados": "UF",
    "mesorregioes": "mesorregiao",
    "municipios": "municipio",
}
VALID_GEO_LEVELS = ("N1", "N2", "N3", "N6", "N8", "N9")


def _session() -> requests.Session:
    return get_session()


def _get_json(url: str, timeout: int = 60, params: dict | None = None) -> object:
    session = _session()
    resp = session.get(url, timeout=timeout, params=params)
    resp.raise_for_status()
    return resp.json()


def get_regions() -> list[dict]:
    return [{"id": code, "nome": name} for name, code in REGIONS.items()]


def get_states(region_code: int | None = None) -> list[dict]:
    if region_code is None:
        return _get_json(f"{LOC_BASE}/estados", params={"orderBy": "nome"})
    return _get_json(f"{LOC_BASE}/regioes/{region_code}/estados")


def get_mesoregions(state_code: int) -> list[dict]:
    return _get_json(f"{LOC_BASE}/estados/{state_code}/mesorregioes")


def get_microregions(state_code: int) -> list[dict]:
    return _get_json(f"{LOC_BASE}/estados/{state_code}/microrregioes")


def get_municipalities(state_code: int) -> list[dict]:
    return _get_json(f"{LOC_BASE}/estados/{state_code}/municipios", params={"orderBy": "nome"})


def fetch_geojson(level: str, code: int | str) -> dict:
    if level not in VALID_MALHA_LEVELS:
        raise ValueError(f"Invalid level {level!r}; must be one of {VALID_MALHA_LEVELS}")
    return _get_json(
        f"{MALHAS_BASE}/{level}/{code}",
        params={"formato": "application/vnd.geo+json"},
        timeout=120,
    )


def fetch_subdivisions(code: int | str, subdivision_level: str = "municipios") -> dict:
    """Fetch child geometries of a parent. Mirrors `fetch_subdivisions()` in R."""
    if subdivision_level not in VALID_MALHA_LEVELS:
        raise ValueError(f"Invalid subdivision_level {subdivision_level!r}")
    intra = INTRA_MAP[subdivision_level]
    code_str = str(code)
    ndig = len(code_str)
    if ndig <= 1:
        parent_level = "regioes"
    elif ndig <= 2:
        parent_level = "estados"
    elif ndig <= 4:
        parent_level = "mesorregioes"
    else:
        parent_level = "municipios"
    return _get_json(
        f"{MALHAS_BASE}/{parent_level}/{code}",
        params={
            "formato": "application/vnd.geo+json",
            "intrarregiao": intra,
            "qualidade": "intermediaria",
        },
        timeout=120,
    )


def fetch_milk_production_raw(
    geo_level: str,
    codes: Iterable[int],
    years: Iterable[int],
) -> object:
    """Hit IBGE Agregados (table 74, var 106, classif 80/2682)."""
    if geo_level not in VALID_GEO_LEVELS:
        raise ValueError(f"Invalid geo_level {geo_level!r}; must be one of {VALID_GEO_LEVELS}")
    codes_str = ",".join(str(c) for c in codes)
    years_str = "|".join(str(y) for y in years)
    url = (
        f"{AGREGADOS_BASE}/74/periodos/{years_str}/variaveis/106"
        f"?localidades={geo_level}[{codes_str}]&classificacao=80[2682]"
    )
    return _get_json(url, timeout=180)
