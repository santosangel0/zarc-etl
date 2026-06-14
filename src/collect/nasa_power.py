"""Coletor para o endpoint NASA POWER daily/point."""

from __future__ import annotations

import json

from src.utils import get_logger, get_session, raw_dir

log = get_logger(__name__)

POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
PARAMETERS = ("T2M", "RH2M", "PRECTOTCORR")

# Endpoint HORÁRIO (reanálise MERRA-2, grade ~0,5°x0,625°). Cobertura começa em
# 2001-01-01. T2M=temperatura a 2m (°C), RH2M=umidade relativa a 2m (%).
HOURLY_POWER_URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"
HOURLY_PARAMETERS = ("T2M", "RH2M")


def _to_yyyymmdd(date_str: str) -> str:
    return date_str.replace("-", "")


def fetch_point(
    lat: float,
    lon: float,
    start: str,
    end: str,
    use_cache: bool = True,
) -> dict:
    """Busca T2M/RH2M/PRECTOTCORR diário para um ponto.

    `start` / `end` aceitam ISO `YYYY-MM-DD`. Cache: `data/raw/nasa_power/`.
    """
    cache_path = raw_dir("nasa_power") / f"{lat}_{lon}_{start}_{end}.json"
    if use_cache and cache_path.exists():
        log.info("nasa-power cache hit: %s", cache_path)
        return json.loads(cache_path.read_text())

    params = {
        "parameters": ",".join(PARAMETERS),
        "community": "AG",
        "format": "JSON",
        "latitude": str(lat),
        "longitude": str(lon),
        "start": _to_yyyymmdd(start),
        "end": _to_yyyymmdd(end),
    }
    session = get_session()
    log.info("nasa-power fetch lat=%s lon=%s %s..%s", lat, lon, start, end)
    resp = session.get(POWER_URL, params=params, timeout=120)
    resp.raise_for_status()
    payload = resp.json()
    cache_path.write_text(json.dumps(payload))
    return payload


def fetch_point_hourly(
    lat: float,
    lon: float,
    start: str,
    end: str,
    parameters: tuple[str, ...] = HOURLY_PARAMETERS,
    use_cache: bool = True,
) -> dict:
    """Busca a série HORÁRIA de `parameters` para um ponto (lat/lon).

    Usa `temporal/hourly/point` com `time-standard=UTC` — as chaves `YYYYMMDDHH`
    ficam em UTC, alinhadas com a coluna `hora_utc` do INMET. `start`/`end` em
    ISO `YYYY-MM-DD`. Cache em `data/raw/nasa_power_hourly/`.

    Atenção ao limite do endpoint: requisições JSON acima de ~17 anos são
    recusadas (HTTP 422, "shorten your requested time extent"); quem chama deve
    fatiar o período (ver `_chunk_year_range` em main.py, chunk padrão 10 anos).
    """
    params_key = "_".join(parameters)
    cache_path = (
        raw_dir("nasa_power_hourly") / f"{lat}_{lon}_{start}_{end}_{params_key}.json"
    )
    if use_cache and cache_path.exists():
        log.info("nasa-power-hourly cache hit: %s", cache_path)
        return json.loads(cache_path.read_text())

    params = {
        "parameters": ",".join(parameters),
        "community": "AG",
        "format": "JSON",
        "latitude": str(lat),
        "longitude": str(lon),
        "start": _to_yyyymmdd(start),
        "end": _to_yyyymmdd(end),
        "time-standard": "UTC",
    }
    session = get_session()
    log.info("nasa-power-hourly fetch lat=%s lon=%s %s..%s", lat, lon, start, end)
    resp = session.get(HOURLY_POWER_URL, params=params, timeout=180)
    resp.raise_for_status()
    payload = resp.json()
    cache_path.write_text(json.dumps(payload))
    return payload
