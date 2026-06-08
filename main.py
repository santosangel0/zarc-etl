"""CLI entry point. Subcommands map 1:1 to pipeline steps in src/."""

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


# ── INMET subcommands ────────────────────────────────────────────────────────


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


# ── IBGE subcommands ─────────────────────────────────────────────────────────


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


# ── all ──────────────────────────────────────────────────────────────────────


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
    p = argparse.ArgumentParser(prog="zarc-etl", description="zarc-etl pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("inmet-stations", help="Fetch INMET stations to estacoes.parquet")
    sp.set_defaults(func=cmd_inmet_stations)

    sp = sub.add_parser("inmet-history", help="Download + parse INMET historical ZIPs")
    sp.add_argument("--years", default="2000-2025", help="e.g. 2000-2025 or 2024")
    sp.set_defaults(func=cmd_inmet_history)

    sp = sub.add_parser("inmet-history-daily", help="Aggregate raw hourly history → daily QC + ITU")
    sp.set_defaults(func=cmd_inmet_history_daily)

    sp = sub.add_parser("inmet-live", help="Hit live INMET daily API for one station")
    sp.add_argument("--code", required=True)
    sp.add_argument("--start", required=True)
    sp.add_argument("--end", required=True)
    sp.set_defaults(func=cmd_inmet_live)

    sp = sub.add_parser("ibge-localidades", help="Fetch IBGE localidades for all levels")
    sp.set_defaults(func=cmd_ibge_localidades)

    sp = sub.add_parser("ibge-malhas", help="Fetch IBGE Malhas geometry to parquet")
    sp.add_argument("--level", required=True, choices=("regioes", "estados", "mesorregioes", "municipios"))
    sp.add_argument("--code", default=None, help="IBGE parent code (defaults to BR)")
    sp.set_defaults(func=cmd_ibge_malhas)

    sp = sub.add_parser("ibge-milk", help="Fetch SIDRA milk production to parquet")
    sp.add_argument("--geo-level", default="N3", choices=("N1", "N2", "N3", "N6", "N8", "N9"))
    sp.add_argument("--codes", default="31", help="comma-separated IBGE codes")
    sp.add_argument("--years", default="2000-2023")
    sp.set_defaults(func=cmd_ibge_milk)

    sp = sub.add_parser("nasa-power", help="Fetch NASA POWER daily/point")
    sp.add_argument("--lat", type=float, required=True)
    sp.add_argument("--lon", type=float, required=True)
    sp.add_argument("--start", required=True, help="YYYY-MM-DD")
    sp.add_argument("--end", required=True, help="YYYY-MM-DD")
    sp.set_defaults(func=cmd_nasa_power)

    sp = sub.add_parser("all", help="Run the batch pipeline (no nasa-power, no inmet-live)")
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
