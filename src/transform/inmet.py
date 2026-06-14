"""Transformadores para dados INMET: estações, histórico horário raw, diário QC + ITU."""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

import duckdb
import polars as pl
from shapely import wkb as shapely_wkb
from shapely.geometry import Point

from src.utils import final_dir, get_logger, interim_dir, normalize_colname

log = get_logger(__name__)

STATION_FILENAME_RE = re.compile(r"[A-Z]\d{3}")
HEADER_RE = re.compile(r"^Data[;(]|^DATA \(", re.IGNORECASE)


def stations_to_parquet(payload: list[dict], output_path: Path | None = None) -> Path:
    """Constrói `estacoes.parquet` com geometria WKB (EPSG:4326).

    Espelha a limpeza em `app/logic/inmet.R:60-94`: remove linhas com coordenadas
    inválidas. Nomes de colunas normalizados para snake_case via `normalize_colname`.
    """
    if not payload:
        raise ValueError("Empty stations payload")

    df = pl.from_dicts(payload, infer_schema_length=None)
    df = df.rename({c: normalize_colname(c) for c in df.columns})

    df = df.with_columns(
        pl.col("vl_latitude").cast(pl.Utf8).str.replace_all(",", ".").cast(pl.Float64, strict=False),
        pl.col("vl_longitude").cast(pl.Utf8).str.replace_all(",", ".").cast(pl.Float64, strict=False),
    )
    df = df.filter(pl.col("vl_latitude").is_not_null() & pl.col("vl_longitude").is_not_null())

    geometry = [
        shapely_wkb.dumps(Point(lon, lat))
        for lat, lon in zip(df["vl_latitude"].to_list(), df["vl_longitude"].to_list(), strict=True)
    ]
    df = df.with_columns(pl.Series(name="geometry", values=geometry, dtype=pl.Binary))

    string_cols = [c for c in df.columns if df.schema[c] == pl.Object]
    if string_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in string_cols])

    out = output_path or (final_dir() / "estacoes.parquet")
    df.write_parquet(out, compression="zstd", compression_level=3)
    log.info("wrote %s rows=%d", out, df.height)
    return out


def _decode_inmet_csv(raw_bytes: bytes) -> str:
    """latin1 → utf-8 com backslash-replace, estratégia idêntica ao qmd."""
    return raw_bytes.decode("latin-1", errors="backslashreplace")


def _detect_header_line(text: str) -> int | None:
    for idx, line in enumerate(text.splitlines()[:15]):
        if HEADER_RE.match(line):
            return idx
    return None


def parse_inmet_csv(csv_path: Path) -> pl.DataFrame | None:
    r"""Faz parse de um CSV horario INMET. Retorna None em caso de falha.

    Replica `read_inmet_csv()` em `tests/test_pipeline_inmet.R:210-276`:
    - Extrai codigo da estacao do nome do arquivo via regex `[A-Z]\d{3}`.
    - Decodifica bytes como latin1.
    - Detecta linha de cabecalho por `^Data[;(]|^DATA \(`.
    - Parse com separador `;`, decimal `,`, NA = ['', 'NA', '-9999', '-9999,0'].
    - Normaliza nomes de colunas; harmoniza `data_yyyy_mm_dd` -> `data`.
    - Remove colunas fantasmas (nomes vazios/so numericos, `unnamed_*`).
    - Forca todas as colunas para Utf8 para tolerar drift de schema entre anos.
    """
    name = csv_path.name
    match = STATION_FILENAME_RE.search(name)
    if not match:
        log.warning("station code not found in %s", name)
        return None
    station_code = match.group(0)

    raw = csv_path.read_bytes()
    text = _decode_inmet_csv(raw)
    header_idx = _detect_header_line(text)
    if header_idx is None:
        log.warning("header not found in %s", name)
        return None

    lines = text.splitlines()
    body = "\n".join(lines[header_idx:])

    try:
        df = pl.read_csv(
            body.encode("utf-8"),
            separator=";",
            decimal_comma=True,
            null_values=["", "NA", "-9999", "-9999,0"],
            try_parse_dates=False,
            ignore_errors=True,
            infer_schema_length=0,  # everything as string
            truncate_ragged_lines=True,
        )
    except Exception as exc:
        log.warning("polars read_csv failed for %s: %s", name, exc)
        return None

    if df.is_empty():
        return None

    rename_map: dict[str, str] = {}
    for col in df.columns:
        norm = normalize_colname(col)
        if norm == "data_yyyy_mm_dd":
            norm = "data"
        rename_map[col] = norm
    df = df.rename(rename_map)

    keep = []
    seen = set()
    for col in df.columns:
        if not col:
            continue
        if col.startswith("unnamed"):
            continue
        if re.fullmatch(r"_*\d+_*", col):
            continue
        if col in seen:
            continue
        seen.add(col)
        keep.append(col)
    df = df.select(keep)

    df = df.with_columns(pl.lit(station_code).alias("cd_estacao"))
    return df


def ingest_history_to_duckdb(
    csv_paths: Iterable[Path],
    db_path: Path | None = None,
    station_lookup: set[str] | None = None,
) -> int:
    """Acrescenta CSVs parseados na `tabela_clima` em um DuckDB persistente.

    Estratégia espelha qmd §6: DB persistente em disco, INSERT BY NAME para colunas
    poderem variar entre anos, todos os valores armazenados como VARCHAR (DuckDB
    faz cast na leitura). Retorna total de linhas inseridas.
    """
    target_db = db_path or (interim_dir() / "inmet_temp.duckdb")
    con = duckdb.connect(str(target_db))
    total = 0
    try:
        for csv_path in csv_paths:
            df = parse_inmet_csv(csv_path)
            if df is None or df.is_empty():
                continue
            if station_lookup is not None and df["cd_estacao"][0] not in station_lookup:
                continue

            df_str = df.with_columns([pl.col(c).cast(pl.Utf8) for c in df.columns])
            arrow_tbl = df_str.to_arrow()  # noqa: F841 — used by DuckDB via reference

            con.register("year_batch", df_str)
            existing = {
                row[0]
                for row in con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                ).fetchall()
            }
            if "tabela_clima" not in existing:
                con.execute("CREATE TABLE tabela_clima AS SELECT * FROM year_batch")
            else:
                # Add any new columns introduced by this batch
                existing_cols = {
                    row[0]
                    for row in con.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name='tabela_clima'"
                    ).fetchall()
                }
                for col in df_str.columns:
                    if col not in existing_cols:
                        con.execute(f'ALTER TABLE tabela_clima ADD COLUMN "{col}" VARCHAR')
                con.execute("INSERT INTO tabela_clima BY NAME SELECT * FROM year_batch")

            con.unregister("year_batch")
            total += df_str.height
    finally:
        con.close()
    log.info("DuckDB total rows in tabela_clima: %d", total)
    return total


def export_history_parquet(
    db_path: Path | None = None,
    output_path: Path | None = None,
    cleanup_db: bool = True,
) -> Path:
    """COPY tabela_clima → parquet (ZSTD) ordenado por (cd_estacao, data)."""
    target_db = db_path or (interim_dir() / "inmet_temp.duckdb")
    out = output_path or (final_dir() / "inmet_historico.parquet")
    if out.exists():
        out.unlink()
    con = duckdb.connect(str(target_db))
    try:
        con.execute(
            f"""
            COPY (
                SELECT * FROM tabela_clima ORDER BY cd_estacao, data
            ) TO '{out.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
            """
        )
    finally:
        con.close()
    log.info("wrote %s (%.1f MB)", out, out.stat().st_size / 1e6)

    if cleanup_db:
        for p in target_db.parent.glob(f"{target_db.name}*"):
            try:
                p.unlink()
            except OSError:
                pass
        log.info("removed temp DuckDB at %s", target_db)
    return out


# ── QC + ITU (Buffington 1977) ────────────────────────────────────────────────


def compute_itu(temp: pl.Expr, humidity: pl.Expr) -> pl.Expr:
    """ITU = 0.8*T + (RH*(T-14.3))/100 + 46.3 (espelha `inmet.R:272-284`)."""
    return 0.8 * temp + (humidity * (temp - 14.3)) / 100 + 46.3


def qc_temperature(expr: pl.Expr) -> pl.Expr:
    """Mascara valores fora de [-10, 50] °C como null."""
    return pl.when((expr < -10) | (expr > 50)).then(None).otherwise(expr)


def qc_humidity(expr: pl.Expr) -> pl.Expr:
    """Mascara valores fora de [0, 100] % como null."""
    return pl.when((expr < 0) | (expr > 100)).then(None).otherwise(expr)


def daily_from_live_payload(payload: list[dict], station_code: str) -> pl.DataFrame:
    """Constrói o DataFrame diário a partir do payload da API INMET ao vivo.

    Espelha `fetch_climate_data()` em `app/logic/inmet.R:160-287`: constrói o
    frame com faixas QC e colunas ITU. Retorna frame vazio se payload vazio.
    """
    columns = ["date", "station_code", "temp_med", "temp_max", "umid_med", "umid_min", "itu_med", "itu_max"]
    if not payload:
        return pl.DataFrame(schema={c: pl.Date if c == "date" else (pl.Utf8 if c == "station_code" else pl.Float64) for c in columns})

    df = pl.from_dicts(payload, infer_schema_length=None)
    rename_map = {c: c.lower() for c in df.columns}
    df = df.rename(rename_map)
    for col in ("temp_med", "temp_max", "umid_med", "umid_min"):
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    df = df.with_columns(
        pl.col("dt_medicao").str.strptime(pl.Date, format="%Y-%m-%d", strict=False).alias("date"),
        pl.lit(station_code).alias("station_code"),
        pl.col("temp_med").cast(pl.Float64, strict=False),
        pl.col("temp_max").cast(pl.Float64, strict=False),
        pl.col("umid_med").cast(pl.Float64, strict=False),
        pl.col("umid_min").cast(pl.Float64, strict=False),
    )
    df = df.with_columns(
        qc_temperature(pl.col("temp_med")).alias("temp_med"),
        qc_temperature(pl.col("temp_max")).alias("temp_max"),
        qc_humidity(pl.col("umid_med")).alias("umid_med"),
        qc_humidity(pl.col("umid_min")).alias("umid_min"),
    )
    df = df.with_columns(
        compute_itu(pl.col("temp_med"), pl.col("umid_med")).alias("itu_med"),
        compute_itu(pl.col("temp_max"), pl.col("umid_min")).alias("itu_max"),
    )
    return df.select(columns)


def daily_from_history_parquet(
    history_path: Path | None = None,
    output_path: Path | None = None,
    temp_col: str = "temperatura_do_ar_bulbo_seco_horaria_c",
    rh_col: str = "umidade_relativa_do_ar_horaria",
) -> Path:
    """Agrega parquet histórico horário raw para diário com QC + ITU."""
    src = history_path or (final_dir() / "inmet_historico.parquet")
    out = output_path or (final_dir() / "inmet_historico_diario.parquet")

    lf = pl.scan_parquet(src)
    schema_cols = lf.collect_schema().names()
    if temp_col not in schema_cols or rh_col not in schema_cols:
        raise RuntimeError(
            f"expected columns missing from {src}: temp={temp_col!r} rh={rh_col!r}; "
            f"available={schema_cols[:8]}..."
        )

    lf = lf.with_columns(
        pl.col("data").str.strptime(pl.Date, format="%Y-%m-%d", strict=False).alias("data"),
        pl.col(temp_col).cast(pl.Utf8).str.replace_all(",", ".").cast(pl.Float64, strict=False).alias("_temp"),
        pl.col(rh_col).cast(pl.Utf8).str.replace_all(",", ".").cast(pl.Float64, strict=False).alias("_rh"),
    )
    lf = lf.with_columns(
        qc_temperature(pl.col("_temp")).alias("_temp"),
        qc_humidity(pl.col("_rh")).alias("_rh"),
    )
    daily = (
        lf.group_by(["cd_estacao", "data"])
        .agg(
            pl.col("_temp").mean().alias("temp_med"),
            pl.col("_temp").max().alias("temp_max"),
            pl.col("_rh").mean().alias("umid_med"),
            pl.col("_rh").min().alias("umid_min"),
        )
        .with_columns(
            compute_itu(pl.col("temp_med"), pl.col("umid_med")).alias("itu_med"),
            compute_itu(pl.col("temp_max"), pl.col("umid_min")).alias("itu_max"),
        )
        .sort(["cd_estacao", "data"])
        .collect()
    )
    daily.write_parquet(out, compression="zstd", compression_level=3, row_group_size=100_000)
    log.info("wrote %s rows=%d", out, daily.height)
    return out
