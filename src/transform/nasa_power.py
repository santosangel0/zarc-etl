"""Transformador para payloads de ponto NASA POWER."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.utils import final_dir, get_logger

log = get_logger(__name__)

NA_SENTINEL = -999.0
HOURLY_KEY_FMT = "%Y%m%d%H"  # chave horária do NASA POWER: YYYYMMDDHH


def _drop_sentinel(v: float | None) -> float | None:
    """NASA POWER usa -999 como NA; converte para null e garante float."""
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    if v <= NA_SENTINEL + 0.5:
        return None
    return v


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
                "t2m": _drop_sentinel(t2m.get(key)),
                "rh2m": _drop_sentinel(rh2m.get(key)),
                "prectotcorr": _drop_sentinel(prec.get(key)),
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


# ── Horário (para imputação do INMET) ─────────────────────────────────────────

# Colunas-base do parquet horário. Os parâmetros do NASA POWER (T2M, RH2M, …)
# viram colunas extras em minúsculas. Chave de join com `inmet_historico`:
# (cd_estacao, data, hora) — `data` "YYYY-MM-DD" (ISO, igual ao INMET) e `hora`
# inteiro 0..23 em UTC. Usamos hora INTEIRA de propósito: o `hora_utc` cru do
# INMET tem formatos inconsistentes ("0000 UTC", "00:00:00", "00:00") entre anos,
# então o join confiável é por hora numérica (ver receita em docs/wiki/05).
HOURLY_BASE_COLS: dict[str, pl.DataType] = {
    "cd_estacao": pl.Utf8,
    "datetime": pl.Datetime,
    "data": pl.Utf8,
    "hora": pl.Int64,
    "lat": pl.Float64,
    "lon": pl.Float64,
}


def parse_point_hourly(payload: dict, cd_estacao: str | None = None) -> pl.DataFrame:
    """Converte um payload HORÁRIO do NASA POWER em um DataFrame organizado.

    Schema: `cd_estacao, datetime, data, hora, lat, lon, <param>…`. Cada
    parâmetro presente no payload vira uma coluna Float64 em minúsculas (T2M→t2m,
    RH2M→rh2m). As chaves `YYYYMMDDHH` (UTC) são expandidas em `datetime`,
    `data` ("YYYY-MM-DD") e `hora` (inteiro 0..23). Sentinela -999 → null.
    `cd_estacao` é preenchido com o código da estação INMET quando a série é
    puxada por estação (senão fica null).
    """
    geometry = payload.get("geometry") if isinstance(payload, dict) else None
    geometry = geometry or {}
    coords = geometry.get("coordinates") or [None, None]
    lon = coords[0] if len(coords) > 0 else None
    lat = coords[1] if len(coords) > 1 else None

    properties = payload.get("properties") if isinstance(payload, dict) else None
    parameters = (properties or {}).get("parameter") or {}
    # nome-da-coluna (minúsculo) -> dict{ YYYYMMDDHH: valor }
    series = {name.lower(): vals for name, vals in parameters.items()}
    param_cols = sorted(series)
    schema = {**HOURLY_BASE_COLS, **{c: pl.Float64 for c in param_cols}}

    keys = sorted({k for vals in series.values() for k in vals})
    rows: list[dict] = []
    for key in keys:
        try:
            dt = datetime.strptime(key, HOURLY_KEY_FMT)
        except (ValueError, TypeError):
            continue
        row: dict = {
            "cd_estacao": cd_estacao,
            "datetime": dt,
            "data": dt.strftime("%Y-%m-%d"),
            "hora": dt.hour,
            "lat": float(lat) if lat is not None else None,
            "lon": float(lon) if lon is not None else None,
        }
        for col in param_cols:
            row[col] = _drop_sentinel(series[col].get(key))
        rows.append(row)

    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema).sort(["cd_estacao", "data", "hora"])


def write_hourly_parquet(df: pl.DataFrame, output_path: Path | None = None) -> Path:
    """Materializa o parquet HORÁRIO consolidado do NASA POWER.

    Saída: `nasa_power_hourly.parquet` (ZSTD, row_group 100k, ordenado por
    cd_estacao, data, hora — mesmo layout físico do `inmet_historico`, para
    predicate pushdown e join eficiente na imputação).
    """
    out = output_path or (final_dir() / "nasa_power_hourly.parquet")
    df = df.sort(["cd_estacao", "data", "hora"])
    df.write_parquet(out, compression="zstd", compression_level=3, row_group_size=100_000)
    log.info("wrote %s rows=%d", out, df.height)
    return out
