from __future__ import annotations

import logging
import os
import re
import unicodedata
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

INMET_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def data_dir() -> Path:
    raw = os.environ.get("DATA_DIR") or "./data"
    return Path(raw).resolve()


def raw_dir(subpath: str = "") -> Path:
    p = data_dir() / "raw"
    if subpath:
        p = p / subpath
    p.mkdir(parents=True, exist_ok=True)
    return p


def interim_dir(subpath: str = "") -> Path:
    p = data_dir() / "interim"
    if subpath:
        p = p / subpath
    p.mkdir(parents=True, exist_ok=True)
    return p


def final_dir() -> Path:
    p = data_dir() / "final"
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_session(user_agent: str | None = None) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    headers = {"Accept": "application/json"}
    if user_agent:
        headers["User-Agent"] = user_agent
    session.headers.update(headers)
    return session


def configure_logging(level: str | None = None) -> None:
    level_name = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def normalize_colname(name: str) -> str:
    """Latin-ASCII fold + lowercase + collapse non-alnum to '_'.

    Mirrors `normalize_colnames()` in `tests/test_pipeline_inmet.R` and the
    `qmd` (`stri_trans_general("Latin-ASCII")`).
    """
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = decomposed.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_only.lower()
    collapsed = _NON_ALNUM.sub("_", lowered).strip("_")
    return collapsed
