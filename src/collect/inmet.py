"""Collectors for INMET data (live API + historical ZIPs)."""

from __future__ import annotations

import json
import zipfile
from collections.abc import Iterable
from pathlib import Path

import requests

from src.utils import (
    INMET_USER_AGENT,
    get_logger,
    get_session,
    interim_dir,
    raw_dir,
)

log = get_logger(__name__)

STATIONS_URL = "https://apitempo.inmet.gov.br/estacoes/T"
DAILY_URL_TMPL = "https://apitempo.inmet.gov.br/token/estacao/diaria/{start}/{end}/{code}/{token}"
HIST_ZIP_URL_TMPL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"


def _inmet_session() -> requests.Session:
    return get_session(user_agent=INMET_USER_AGENT)


def fetch_stations() -> list[dict]:
    """Hit /estacoes/T and return the raw JSON list. Mirrors `get_stations()`."""
    session = _inmet_session()
    resp = session.get(STATIONS_URL, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    log.info("INMET stations fetched: %d", len(payload))
    return payload


def fetch_daily(code: str, start: str, end: str, token: str) -> list[dict]:
    """Hit the authenticated daily endpoint for one station/period."""
    if not token:
        raise ValueError("INMET_TOKEN is required for fetch_daily()")
    url = DAILY_URL_TMPL.format(start=start, end=end, code=code, token=token)
    session = _inmet_session()
    resp = session.get(url, timeout=120)
    if resp.status_code >= 400:
        log.warning("INMET daily HTTP %d for %s (%s..%s)", resp.status_code, code, start, end)
        return []
    text = resp.text
    if len(text) < 3:
        return []
    try:
        return resp.json()
    except json.JSONDecodeError:
        return []


def download_history_zip(year: int, dest_dir: Path | None = None) -> Path:
    """Download `{year}.zip` from the INMET historical portal. Skip if exists."""
    target_dir = dest_dir or raw_dir("inmet")
    target = target_dir / f"{year}.zip"
    if target.exists():
        log.info("zip already present: %s", target)
        return target

    url = HIST_ZIP_URL_TMPL.format(year=year)
    session = _inmet_session()
    log.info("downloading %s", url)
    resp = session.get(url, timeout=600, stream=True)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code} downloading {url}")
    with target.open("wb") as fh:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            if chunk:
                fh.write(chunk)
    size_mb = target.stat().st_size / 1e6
    log.info("saved %s (%.1f MB)", target, size_mb)
    return target


def extract_history_zip(zip_path: Path, target_dir: Path | None = None) -> list[Path]:
    """Extract all CSV entries of an INMET yearly zip. Returns list of CSV paths."""
    year = zip_path.stem
    out_dir = target_dir or interim_dir(f"inmet/{year}")
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            if not member.lower().endswith(".csv"):
                continue
            base = Path(member).name
            if not base:
                continue
            dest = out_dir / base
            with zf.open(member) as src, dest.open("wb") as dst:
                dst.write(src.read())
            extracted.append(dest)
    log.info("extracted %d CSVs from %s", len(extracted), zip_path.name)
    return extracted


def download_history_years(years: Iterable[int]) -> list[Path]:
    return [download_history_zip(y) for y in years]
