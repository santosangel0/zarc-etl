"""Valida parquets de data/final contra os contratos de dados.

Confere, para cada arquivo: integridade (abre como parquet), compressão ZSTD,
row_group_size, schema (colunas + dtypes esperados), contagem de linhas e
amostra de geometria WKB quando aplicável. Detecta o contrato pelo nome do
arquivo. Sai com código != 0 se houver qualquer violação.

Uso:
    python scripts/validate_parquet.py data/final/*.parquet
    python scripts/validate_parquet.py /data/final/inmet_historico_diario.parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

EXPECTED_ROW_GROUP_SIZE = 100_000
# O DuckDB quantiza ROW_GROUP_SIZE para múltiplos do seu vector size (2048), então
# `COPY (... ROW_GROUP_SIZE 100000)` materializa grupos de 100352 (49*2048), não
# 100000. Aceitamos qualquer primeiro grupo em [EXPECTED, EXPECTED + VECTOR): cobre
# tanto o writer polars (exato 100000) quanto o writer DuckDB (100352).
DUCKDB_VECTOR_SIZE = 2048

# Colunas obrigatórias por contrato. Para schemas "raw" (todos Utf8) listamos só
# as colunas-chave; colunas extras são permitidas. dtype=None => não checa tipo.
# ── (coluna, dtype-polars-esperado-ou-None) ──
CONTRACTS: dict[str, dict] = {
    "estacoes": {
        "match": lambda n: n == "estacoes",
        "cols": {
            "cd_estacao": pl.Utf8,
            "dc_nome": pl.Utf8,
            "sg_estado": pl.Utf8,
            "vl_latitude": pl.Float64,
            "vl_longitude": pl.Float64,
            "geometry": pl.Binary,
        },
        "allow_extra": True,
        "wkb_col": "geometry",
    },
    "inmet_historico": {
        "match": lambda n: n == "inmet_historico",
        "cols": {
            "cd_estacao": pl.Utf8,
            "data": pl.Utf8,
            "hora_utc": pl.Utf8,
        },
        "allow_extra": True,
        "sort": ["cd_estacao", "data"],
    },
    "inmet_historico_diario": {
        "match": lambda n: n == "inmet_historico_diario",
        "cols": {
            "cd_estacao": pl.Utf8,
            "data": pl.Date,
            "temp_med": pl.Float64,
            "temp_max": pl.Float64,
            "umid_med": pl.Float64,
            "umid_min": pl.Float64,
            "itu_med": pl.Float64,
            "itu_max": pl.Float64,
        },
        "allow_extra": False,
        "sort": ["cd_estacao", "data"],
    },
    "ibge_localidades": {
        "match": lambda n: n.startswith("ibge_localidades_"),
        "cols": {"id": pl.Int64, "nome": pl.Utf8, "parent_id": pl.Int64},
        "allow_extra": False,
    },
    "ibge_malhas": {
        "match": lambda n: n.startswith("ibge_malhas_"),
        "cols": {"code": pl.Utf8, "nivel": pl.Utf8, "geometry": pl.Binary},
        "allow_extra": False,
        "wkb_col": "geometry",
    },
    "milk_production": {
        "match": lambda n: n == "milk_production",
        "cols": {
            "code": pl.Int64,
            "nome": pl.Utf8,
            "year": pl.Int64,
            "milk_production_liters": pl.Float64,
            "geo_level": pl.Utf8,
        },
        "allow_extra": False,
        "sort": ["geo_level", "year", "code"],
    },
    "nasa_power": {
        "match": lambda n: n.startswith("nasa_power_"),
        "cols": {
            "date": pl.Date,
            "lat": pl.Float64,
            "lon": pl.Float64,
            "t2m": pl.Float64,
            "rh2m": pl.Float64,
            "prectotcorr": pl.Float64,
        },
        "allow_extra": False,
        "sort": ["date"],
    },
}


def _find_contract(stem: str) -> tuple[str, dict] | tuple[None, None]:
    for name, spec in CONTRACTS.items():
        if spec["match"](stem):
            return name, spec
    return None, None


def validate(path: Path) -> list[str]:
    """Retorna lista de problemas (vazia => parquet ok)."""
    problems: list[str] = []

    # 1. Abre como parquet (integridade) + metadados de compressão/row group.
    try:
        meta = pq.ParquetFile(path).metadata
    except Exception as exc:  # noqa: BLE001
        return [f"não abre como parquet: {exc}"]

    n_rows = meta.num_rows
    compressions = {
        meta.row_group(rg).column(c).compression
        for rg in range(meta.num_row_groups)
        for c in range(meta.num_columns)
    }
    if compressions != {"ZSTD"}:
        problems.append(f"compressão esperada ZSTD, encontrada {sorted(compressions)}")

    if meta.num_row_groups > 1:
        first_rg = meta.row_group(0).num_rows
        upper = EXPECTED_ROW_GROUP_SIZE + DUCKDB_VECTOR_SIZE
        if not EXPECTED_ROW_GROUP_SIZE <= first_rg < upper:
            problems.append(
                f"row_group_size esperado ~{EXPECTED_ROW_GROUP_SIZE} "
                f"(aceita até {upper - 1} p/ quantização DuckDB), "
                f"primeiro grupo tem {first_rg}"
            )

    if n_rows == 0:
        problems.append("parquet vazio (0 linhas)")

    # 2. Schema + dtypes via polars (lazy, não carrega dados).
    schema = pl.scan_parquet(path).collect_schema()
    stem = path.stem
    contract_name, spec = _find_contract(stem)

    if spec is None:
        problems.append(
            f"nome '{stem}' não casa com nenhum contrato conhecido "
            "(checagem de schema pulada)"
        )
        return problems

    for col, want in spec["cols"].items():
        if col not in schema.names():
            problems.append(f"coluna obrigatória ausente: '{col}'")
            continue
        got = schema[col]
        if want is not None and got != want:
            problems.append(f"coluna '{col}': dtype esperado {want}, encontrado {got}")

    if not spec.get("allow_extra", True):
        extras = set(schema.names()) - set(spec["cols"])
        if extras:
            problems.append(f"colunas inesperadas: {sorted(extras)}")

    # 3. Geometria WKB: tenta decodificar 1 amostra.
    wkb_col = spec.get("wkb_col")
    if wkb_col and wkb_col in schema.names() and n_rows:
        try:
            from shapely import wkb as shp_wkb

            sample = pl.read_parquet(path, columns=[wkb_col], n_rows=1)[wkb_col][0]
            shp_wkb.loads(bytes(sample))
        except Exception as exc:  # noqa: BLE001
            problems.append(f"geometria WKB em '{wkb_col}' não decodifica: {exc}")

    # 4. Ordem física (sort) — checa amostra das primeiras 200k linhas.
    sort_cols = spec.get("sort")
    if sort_cols and all(c in schema.names() for c in sort_cols) and n_rows > 1:
        head = pl.read_parquet(path, columns=sort_cols, n_rows=200_000)
        if not head.equals(head.sort(sort_cols)):
            problems.append(f"não está fisicamente ordenado por {sort_cols}")

    return problems


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2

    rc = 0
    for arg in argv:
        path = Path(arg)
        if not path.exists():
            print(f"✗ {arg}: arquivo não existe")
            rc = 1
            continue

        problems = validate(path)
        contract_name, _ = _find_contract(path.stem)
        size_mb = path.stat().st_size / 1e6
        try:
            n_rows = pq.ParquetFile(path).metadata.num_rows
        except Exception:  # noqa: BLE001
            n_rows = "?"

        if problems:
            rc = 1
            print(f"✗ {path.name}  [{contract_name or 'desconhecido'}]  "
                  f"{size_mb:.1f} MB, {n_rows} linhas")
            for p in problems:
                print(f"    - {p}")
        else:
            print(f"✓ {path.name}  [{contract_name}]  "
                  f"{size_mb:.1f} MB, {n_rows} linhas — schema ok")

    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
