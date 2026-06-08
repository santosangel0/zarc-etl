"""Transformador para payloads de ponto NASA POWER."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.utils import final_dir, get_logger

log = get_logger(__name__)

NA_SENTINEL = -999.0


def parse_point(payload: dict) -> pl.DataFrame:
    """Converte um payload NASA POWER em um DataFrame organizado.

    Schema: `date, lat, lon, t2m, rh2m, prectotcorr`. Sentinela -999 → null.
    """
    columns = {
        "date": pl.Date,
        "lat": pl.Float64,
        "lon": pl.Float64,
        "t2m": pl.Float64,
        "rh2m": pl.Float64,
        "prectotcorr": pl.Float64,
    }
    if not isinstance(payload, dict):
        return pl.DataFrame(schema=columns)

    geometry = payload.get("geometry") or {}
    coords = geometry.get("coordinates") or [None, None]
    lon = coords[0] if len(coords) > 0 else None
    lat = coords[1] if len(coords) > 1 else None

    parameters = ((payload.get("properties") or {}).get("parameter") or {})
    t2m = parameters.get("T2M") or {}
    rh2m = parameters.get("RH2M") or {}
    prec = parameters.get("PRECTOTCORR") or {}

    keys = sorted(set(t2m) | set(rh2m) | set(prec))

    def _norm(v: float | None) -> float | None:
        if v is None:
            return None
        try:
            v = float(v)
        except (TypeError, ValueError):
            return None
        if v <= NA_SENTINEL + 0.5:
            return None
        return v

    rows: list[dict] = []
    for key in keys:
        try:
            d: date = datetime.strptime(key, "%Y%m%d").date()
        except ValueError:
            continue
        rows.append(
            {
                "date": d,
                "lat": float(lat) if lat is not None else None,
                "lon": float(lon) if lon is not None else None,
                "t2m": _norm(t2m.get(key)),
                "rh2m": _norm(rh2m.get(key)),
                "prectotcorr": _norm(prec.get(key)),
            }
        )

    if not rows:
        return pl.DataFrame(schema=columns)
    return pl.DataFrame(rows, schema=columns).sort("date")


def write_parquet(
    df: pl.DataFrame,
    lat: float,
    lon: float,
    start: str,
    end: str,
    output_dir: Path | None = None,
) -> Path:
    out_dir = output_dir or final_dir()
    out = out_dir / f"nasa_power_{lat}_{lon}_{start}_{end}.parquet"
    df.write_parquet(out, compression="zstd", compression_level=3)
    log.info("wrote %s rows=%d", out, df.height)
    return out
