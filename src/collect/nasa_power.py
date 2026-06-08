"""Coletor para o endpoint NASA POWER daily/point."""

from __future__ import annotations

import json

from src.utils import get_logger, get_session, raw_dir

log = get_logger(__name__)

POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
PARAMETERS = ("T2M", "RH2M", "PRECTOTCORR")


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
