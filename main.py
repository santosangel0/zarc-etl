"""Ponto de entrada CLI. Subcomandos mapeiam 1:1 para passos do pipeline em src/."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable

from src.utils import configure_logging, get_logger

log = get_logger("zarc-etl")


def _parse_year_range(spec: str) -> list[int]:
    if "-" in spec:
        a, b = spec.split("-", 1)
        start, end = int(a), int(b)
        if end < start:
            raise ValueError(f"invalid year range: {spec}")
        return list(range(start, end + 1))
    return [int(spec)]


def _parse_codes(spec: str) -> list[int]:
    return [int(x) for x in spec.split(",") if x.strip()]


def _chunk_year_range(years: list[int], size: int) -> list[tuple[str, str]]:
    """Quebra um range de anos em pedaços ISO (start, end) de até `size` anos.

    O endpoint horário JSON do NASA POWER recusa extensões acima de ~17 anos
    (HTTP 422, "shorten your requested time extent"), então puxamos em blocos.
    """
    chunks: list[tuple[str, str]] = []
    for i in range(0, len(years), size):
        block = years[i : i + size]
        chunks.append((f"{block[0]}-01-01", f"{block[-1]}-12-31"))
    return chunks


# ── Subcomandos INMET ────────────────────────────────────────────────────────


def cmd_inmet_stations(_args: argparse.Namespace) -> int:
    from src.collect.inmet import fetch_stations
    from src.transform.inmet import stations_to_parquet

    payload = fetch_stations()
    stations_to_parquet(payload)
    return 0


def cmd_inmet_history(args: argparse.Namespace) -> int:
    import duckdb

    from src.collect.inmet import download_history_zip, extract_history_zip
    from src.transform.inmet import (
        export_history_parquet,
        ingest_history_to_duckdb,
    )
    from src.utils import final_dir

    years = _parse_year_range(args.years)
    estacoes_path = final_dir() / "estacoes.parquet"
    station_lookup: set[str] | None = None
    if estacoes_path.exists():
        con = duckdb.connect()
        try:
            rows = con.execute(
                f"SELECT DISTINCT cd_estacao FROM read_parquet('{estacoes_path.as_posix()}')"
            ).fetchall()
        finally:
            con.close()
        station_lookup = {r[0] for r in rows if r[0]}
        log.info("loaded station lookup: %d codes", len(station_lookup))
    else:
        log.warning("estacoes.parquet missing; ingestion will keep all stations")

    for year in years:
        zip_path = download_history_zip(year)
        csv_paths = extract_history_zip(zip_path)
        ingest_history_to_duckdb(csv_paths, station_lookup=station_lookup)

    export_history_parquet()
    return 0


def cmd_inmet_history_daily(_args: argparse.Namespace) -> int:
    from src.transform.inmet import daily_from_history_parquet

    daily_from_history_parquet()
    return 0


def cmd_inmet_live(args: argparse.Namespace) -> int:
    from src.collect.inmet import fetch_daily
    from src.transform.inmet import daily_from_live_payload

    token = os.environ.get("INMET_TOKEN") or ""
    payload = fetch_daily(args.code, args.start, args.end, token)
    df = daily_from_live_payload(payload, args.code)
    print(df)
    return 0


def cmd_inmet_impute(args: argparse.Namespace) -> int:
    """Imputa lacunas horárias do INMET com NASA POWER (corrigido) → diário + relatório."""
    from src.transform.impute import (
        coverage_summary,
        format_report,
        impute_all,
        impute_daily,
        sample_codes,
        validate,
        validate_sample,
    )
    from src.utils import final_dir

    test_years = _parse_year_range(args.test_years)
    yrs = _parse_year_range(args.years)
    years = (yrs[0], yrs[-1])

    if args.all_stations:
        # Escala completa: imputa em lotes (RAM-safe, retomável); valida numa amostra.
        log.info("imputação ALL-STATIONS: método=%s, anos=%s, lote=%d", args.method, years, args.batch_size)
        from src.transform.impute import station_coords

        _out, daily = impute_all(method=args.method, years=years, batch_size=args.batch_size)
        all_codes = daily["cd_estacao"].unique().sort().to_list()
        want = set(station_coords()["cd_estacao"].to_list())
        excluded = sorted(want - set(all_codes))  # sem dado observável no período
        sample = sample_codes(all_codes, args.sample)
        log.info("validando amostra de %d estações: %s", len(sample), sample)
        val = validate_sample(sample, test_years, args.method, years=years)
        cov = coverage_summary(daily)
        report = format_report(val, cov, sample)
        completeness = (
            f"\n## Completude\n\n- Estações com dados imputados: **{len(all_codes)}/{len(want)}**."
            + (
                ""
                if not excluded
                else f"\n- {len(excluded)} excluídas por não terem dados INMET no período "
                f"(estações que começaram a operar depois): {excluded}"
            )
        )
        report += completeness
    else:
        codes = [c.strip() for c in (args.codes or "").split(",") if c.strip()]
        if not codes:
            print("informe --codes A001,A101 ou --all-stations", file=sys.stderr)
            return 2
        log.info("imputação: %d estações, método=%s, hold-out=%s", len(codes), args.method, test_years)
        val = validate(codes, test_years, args.method)
        _out, daily = impute_daily(codes, method=args.method)
        cov = coverage_summary(daily)
        report = format_report(val, cov, codes)

    # Composição por tipo (observado / gap-fill / backfill pré-existência).
    total = daily.height
    tipo = daily.group_by("tipo").len().sort("len", descending=True)
    comp = "\n## Composição por tipo\n\n| tipo | dias-estação | % |\n|---|---|---|\n"
    for r in tipo.iter_rows(named=True):
        comp += f"| {r['tipo']} | {r['len']:,} | {r['len'] / total * 100:.1f}% |\n"
    report += comp

    report_path = final_dir() / args.report
    report_path.write_text(report)
    log.info("relatório escrito em %s", report_path)
    print(report)
    return 0


# ── Subcomandos IBGE ──────────────────────────────────────────────────────────


def cmd_ibge_localidades(_args: argparse.Namespace) -> int:
    from src.collect.ibge import (
        get_mesoregions,
        get_microregions,
        get_municipalities,
        get_regions,
        get_states,
    )
    from src.transform.ibge import localidades_to_parquet

    localidades_to_parquet(get_regions(), "regioes")
    states = get_states()
    localidades_to_parquet(states, "estados", parent_field="regiao.id")

    all_meso: list[dict] = []
    all_micro: list[dict] = []
    all_munic: list[dict] = []
    for state in states:
        sid = state["id"]
        all_meso.extend(get_mesoregions(sid))
        all_micro.extend(get_microregions(sid))
        all_munic.extend(get_municipalities(sid))
    localidades_to_parquet(all_meso, "mesorregioes", parent_field="UF.id")
    localidades_to_parquet(all_micro, "microrregioes", parent_field="mesorregiao.UF.id")
    localidades_to_parquet(all_munic, "municipios", parent_field="microrregiao.id")
    return 0


def cmd_ibge_malhas(args: argparse.Namespace) -> int:
    from src.collect.ibge import fetch_geojson, fetch_subdivisions
    from src.transform.ibge import malhas_geojson_to_parquet

    if args.code:
        gj = fetch_subdivisions(args.code, args.level)
    else:
        gj = fetch_geojson(args.level, "BR")
    malhas_geojson_to_parquet(gj, args.level)
    return 0


def cmd_ibge_milk(args: argparse.Namespace) -> int:
    from src.collect.ibge import fetch_milk_production_raw
    from src.transform.ibge import (
        parse_milk_production,
        validate_api_request,
        write_milk_production_parquet,
    )

    codes = _parse_codes(args.codes)
    years = _parse_year_range(args.years)
    check = validate_api_request(1, len(years), len(codes))
    if check is not True:
        print(check, file=sys.stderr)
        return 2
    payload = fetch_milk_production_raw(args.geo_level, codes, years)
    df = parse_milk_production(payload, args.geo_level)
    write_milk_production_parquet(df)
    return 0


# ── NASA POWER ───────────────────────────────────────────────────────────────


def cmd_nasa_power(args: argparse.Namespace) -> int:
    from src.collect.nasa_power import fetch_point
    from src.transform.nasa_power import parse_point, write_parquet

    payload = fetch_point(args.lat, args.lon, args.start, args.end)
    df = parse_point(payload)
    write_parquet(df, args.lat, args.lon, args.start, args.end)
    return 0


def cmd_nasa_power_hourly(args: argparse.Namespace) -> int:
    """Série HORÁRIA do NASA POWER (T2M/RH2M…) para imputar buracos do INMET.

    Dois modos:
      • ponto único  : `--lat --lon --start --end` → parquet ad-hoc por ponto.
      • por estações : `--codes A001,A002` ou `--all-stations` (+ `--years`) →
        lê estacoes.parquet, puxa a série de cada estação e consolida em
        `nasa_power_hourly.parquet`, com cd_estacao/data/hora_utc para join
        direto com `inmet_historico`.
    """
    import time

    import polars as pl

    from src.collect.nasa_power import fetch_point_hourly
    from src.transform.nasa_power import (
        parse_point_hourly,
        write_hourly_parquet,
    )
    from src.utils import final_dir

    parameters = tuple(p.strip().upper() for p in args.parameters.split(",") if p.strip())

    # ── Modo ponto único: pull ad-hoc, não toca o parquet consolidado. ──────────
    if args.lat is not None and args.lon is not None:
        payload = fetch_point_hourly(args.lat, args.lon, args.start, args.end, parameters)
        df = parse_point_hourly(payload)
        out = (
            final_dir()
            / f"nasa_power_hourly_{args.lat}_{args.lon}_{args.start}_{args.end}.parquet"
        )
        df.write_parquet(out, compression="zstd", compression_level=3, row_group_size=100_000)
        log.info("wrote %s rows=%d", out, df.height)
        return 0

    # ── Modo por estações: coordenadas vêm de estacoes.parquet. ─────────────────
    estacoes_path = final_dir() / "estacoes.parquet"
    if not estacoes_path.exists():
        print("estacoes.parquet ausente — rode `inmet-stations` primeiro", file=sys.stderr)
        return 2

    est = pl.read_parquet(
        estacoes_path, columns=["cd_estacao", "vl_latitude", "vl_longitude"]
    ).drop_nulls()

    if not args.all_stations:
        codes = {c.strip() for c in (args.codes or "").split(",") if c.strip()}
        if not codes:
            print("informe --codes A001,A002 ou --all-stations", file=sys.stderr)
            return 2
        est = est.filter(pl.col("cd_estacao").is_in(list(codes)))

    if est.height == 0:
        print("nenhuma estação selecionada", file=sys.stderr)
        return 2

    years = _parse_year_range(args.years)
    chunks = _chunk_year_range(years, args.chunk_years)

    frames: list[pl.DataFrame] = []
    total = est.height
    for i, row in enumerate(est.iter_rows(named=True), start=1):
        code, lat, lon = row["cd_estacao"], row["vl_latitude"], row["vl_longitude"]
        log.info("[%d/%d] nasa-power-hourly %s (%.4f, %.4f)", i, total, code, lat, lon)
        for c_start, c_end in chunks:
            payload = fetch_point_hourly(lat, lon, c_start, c_end, parameters)
            frames.append(parse_point_hourly(payload, cd_estacao=code))
            time.sleep(args.sleep)

    df = pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame()
    write_hourly_parquet(df)
    return 0


# ── all (tudo) ────────────────────────────────────────────────────────────────


def cmd_all(args: argparse.Namespace) -> int:
    rc = 0
    rc |= cmd_inmet_stations(args)
    rc |= cmd_inmet_history(argparse.Namespace(years=args.history_years))
    rc |= cmd_inmet_history_daily(args)
    rc |= cmd_ibge_localidades(args)
    for level in args.malha_levels.split(","):
        rc |= cmd_ibge_malhas(argparse.Namespace(level=level.strip(), code=None))
    rc |= cmd_ibge_milk(
        argparse.Namespace(geo_level=args.milk_geo_level, codes=args.milk_codes, years=args.milk_years)
    )
    return rc


# ── main ─────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="zarc-etl", description="Pipeline zarc-etl")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("inmet-stations", help="Busca estações INMET para estacoes.parquet")
    sp.set_defaults(func=cmd_inmet_stations)

    sp = sub.add_parser("inmet-history", help="Download + parse dos ZIPs históricos INMET")
    sp.add_argument("--years", default="2000-2025", help="ex: 2000-2025 ou 2024")
    sp.set_defaults(func=cmd_inmet_history)

    sp = sub.add_parser("inmet-history-daily", help="Agrega histórico horário raw → diário com QC + ITU")
    sp.set_defaults(func=cmd_inmet_history_daily)

    sp = sub.add_parser("inmet-live", help="Consulta API INMET diária ao vivo para uma estação")
    sp.add_argument("--code", required=True)
    sp.add_argument("--start", required=True)
    sp.add_argument("--end", required=True)
    sp.set_defaults(func=cmd_inmet_live)

    sp = sub.add_parser("inmet-impute", help="Imputa buracos horários do INMET com NASA POWER (corrigido) → diário")
    sp.add_argument("--codes", default=None, help="códigos INMET ex: A001,A101")
    sp.add_argument("--all-stations", action="store_true", help="todas as estações (em lotes, retomável)")
    sp.add_argument("--method", default="scaling", choices=("scaling", "linear"), help="correção de viés")
    sp.add_argument("--years", default="2001-2024", help="período a imputar (NASA horário começa em 2001)")
    sp.add_argument("--test-years", default="2022-2024", help="anos de hold-out p/ validação")
    sp.add_argument("--batch-size", type=int, default=20, help="estações por lote (modo --all-stations)")
    sp.add_argument("--sample", type=int, default=12, help="estações na amostra de validação (modo --all-stations)")
    sp.add_argument("--report", default="impute_report.md", help="nome do relatório em data/final/")
    sp.set_defaults(func=cmd_inmet_impute)

    sp = sub.add_parser("ibge-localidades", help="Busca localidades IBGE para todos os níveis")
    sp.set_defaults(func=cmd_ibge_localidades)

    sp = sub.add_parser("ibge-malhas", help="Busca geometria IBGE Malhas para parquet")
    sp.add_argument("--level", required=True, choices=("regioes", "estados", "mesorregioes", "municipios"))
    sp.add_argument("--code", default=None, help="IBGE parent code (defaults to BR)")
    sp.set_defaults(func=cmd_ibge_malhas)

    sp = sub.add_parser("ibge-milk", help="Busca produção leiteira SIDRA para parquet")
    sp.add_argument("--geo-level", default="N3", choices=("N1", "N2", "N3", "N6", "N8", "N9"))
    sp.add_argument("--codes", default="31", help="comma-separated IBGE codes")
    sp.add_argument("--years", default="2000-2023")
    sp.set_defaults(func=cmd_ibge_milk)

    sp = sub.add_parser("nasa-power", help="Busca NASA POWER diário/ponto")
    sp.add_argument("--lat", type=float, required=True)
    sp.add_argument("--lon", type=float, required=True)
    sp.add_argument("--start", required=True, help="YYYY-MM-DD")
    sp.add_argument("--end", required=True, help="YYYY-MM-DD")
    sp.set_defaults(func=cmd_nasa_power)

    sp = sub.add_parser(
        "nasa-power-hourly",
        help="NASA POWER horário (T2M/RH2M) p/ imputação — ponto único ou por estações",
    )
    sp.add_argument("--lat", type=float, default=None, help="ponto único (com --lon/--start/--end)")
    sp.add_argument("--lon", type=float, default=None)
    sp.add_argument("--start", default=None, help="YYYY-MM-DD (modo ponto único)")
    sp.add_argument("--end", default=None, help="YYYY-MM-DD (modo ponto único)")
    sp.add_argument("--codes", default=None, help="códigos INMET ex: A001,A521 (modo estações)")
    sp.add_argument("--all-stations", action="store_true", help="todas as estações de estacoes.parquet")
    sp.add_argument("--years", default="2001-2025", help="range de anos (modo estações)")
    sp.add_argument("--chunk-years", type=int, default=10, help="anos por request (limite JSON ~17)")
    sp.add_argument("--parameters", default="T2M,RH2M", help="parâmetros POWER, vírgula-separados")
    sp.add_argument("--sleep", type=float, default=0.5, help="pausa entre requests (s)")
    sp.set_defaults(func=cmd_nasa_power_hourly)

    sp = sub.add_parser("all", help="Executa o pipeline batch (sem nasa-power, sem inmet-live)")
    sp.add_argument("--history-years", default="2000-2025")
    sp.add_argument("--malha-levels", default="regioes,estados")
    sp.add_argument("--milk-geo-level", default="N3")
    sp.add_argument("--milk-codes", default="31")
    sp.add_argument("--milk-years", default="2000-2023")
    sp.set_defaults(func=cmd_all)

    return p


def main(argv: Iterable[str] | None = None) -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
