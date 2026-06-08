"""Transformadores para IBGE: localidades, malhas (WKB), produção leiteira."""

from __future__ import annotations

import math
from pathlib import Path

import polars as pl
from shapely import wkb as shapely_wkb
from shapely.geometry import shape

from src.utils import final_dir, get_logger

log = get_logger(__name__)


def localidades_to_parquet(
    rows: list[dict],
    nivel: str,
    output_dir: Path | None = None,
    parent_field: str | None = None,
) -> Path:
    """Escreve um parquet de localidades para um nível. Adiciona parent_id quando encontrado."""
    out_dir = output_dir or final_dir()
    if not rows:
        df = pl.DataFrame(schema={"id": pl.Int64, "nome": pl.Utf8})
    else:
        records = []
        for row in rows:
            entry = {"id": row.get("id"), "nome": row.get("nome")}
            if parent_field:
                parent = row
                for part in parent_field.split("."):
                    if isinstance(parent, dict):
                        parent = parent.get(part)
                    else:
                        parent = None
                        break
                entry["parent_id"] = parent
            records.append(entry)
        df = pl.from_dicts(records)

    out = out_dir / f"ibge_localidades_{nivel}.parquet"
    df.write_parquet(out, compression="zstd", compression_level=3)
    log.info("wrote %s rows=%d", out, df.height)
    return out


def malhas_geojson_to_parquet(
    geojson: dict,
    nivel: str,
    output_dir: Path | None = None,
) -> Path:
    """Converte uma FeatureCollection GeoJSON para parquet com geometria WKB."""
    out_dir = output_dir or final_dir()
    features = geojson.get("features", []) if isinstance(geojson, dict) else []
    rows: list[dict] = []
    for feat in features:
        props = feat.get("properties") or {}
        code = props.get("codarea") or props.get("CODAREA") or props.get("code")
        try:
            geometry = shape(feat["geometry"])
            wkb = shapely_wkb.dumps(geometry)
        except Exception as exc:
            log.warning("skipping feature without valid geometry: %s", exc)
            continue
        rows.append({"code": str(code) if code is not None else None, "nivel": nivel, "geometry": wkb})

    df = pl.DataFrame(
        rows,
        schema={"code": pl.Utf8, "nivel": pl.Utf8, "geometry": pl.Binary},
    )
    out = out_dir / f"ibge_malhas_{nivel}.parquet"
    df.write_parquet(out, compression="zstd", compression_level=3)
    log.info("wrote %s rows=%d", out, df.height)
    return out


# ── SIDRA ─────────────────────────────────────────────────────────────────────


def clean_sidra_value(raw: str | None) -> float | None:
    """Replica `clean_sidra_values()` (`-`→0, `..`/`...`/`X`→null)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if s == "-":
        return 0.0
    if s in ("..", "...", "X", ""):
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    if math.isnan(v):
        return None
    return v


def validate_api_request(n_categories: int, n_periods: int, n_locations: int) -> bool | str:
    """Limite de 100k do IBGE Agregados. Espelha `validate_api_request()` no R."""
    total = n_categories * n_periods * n_locations
    if total <= 100_000:
        return True
    formatted = f"{total:,}".replace(",", ".")
    return (
        "A requisição excede o limite de 100.000 valores "
        f"({formatted}). Reduza o intervalo de tempo ou o número de localidades."
    )


def parse_milk_production(payload: object, geo_level: str) -> pl.DataFrame:
    """Converte resposta dos agregados SIDRA em um DataFrame organizado.

    Schema: `code, nome, year, milk_production_liters, geo_level`. Valores são
    convertidos de "mil litros" para litros (×1000), igual ao esperado pelo
    consumidor em `app/logic/ibge.R:373-376`.
    """
    columns = {
        "code": pl.Int64,
        "nome": pl.Utf8,
        "year": pl.Int64,
        "milk_production_liters": pl.Float64,
        "geo_level": pl.Utf8,
    }
    if not isinstance(payload, list) or not payload:
        return pl.DataFrame(schema=columns)

    series = []
    try:
        series = payload[0]["resultados"][0]["series"]
    except (KeyError, IndexError, TypeError):
        return pl.DataFrame(schema=columns)

    rows: list[dict] = []
    for item in series:
        loc = item.get("localidade", {}) or {}
        try:
            code = int(loc.get("id"))
        except (TypeError, ValueError):
            continue
        nome = loc.get("nome")
        for year_str, raw_val in (item.get("serie") or {}).items():
            try:
                year = int(year_str)
            except ValueError:
                continue
            value = clean_sidra_value(raw_val)
            if value is None:
                continue
            rows.append(
                {
                    "code": code,
                    "nome": nome,
                    "year": year,
                    "milk_production_liters": value * 1000.0,
                    "geo_level": geo_level,
                }
            )
    df = pl.DataFrame(rows, schema=columns) if rows else pl.DataFrame(schema=columns)
    return df.sort(["geo_level", "year", "code"])


def write_milk_production_parquet(df: pl.DataFrame, output_path: Path | None = None) -> Path:
    out = output_path or (final_dir() / "milk_production.parquet")
    df.write_parquet(out, compression="zstd", compression_level=3, row_group_size=100_000)
    log.info("wrote %s rows=%d", out, df.height)
    return out
