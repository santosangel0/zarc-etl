from __future__ import annotations

import io
import zipfile
from pathlib import Path

import responses

from src.collect import inmet as collect_inmet


def _load(fixtures_dir: Path, name: str) -> str:
    return (fixtures_dir / name).read_text()


@responses.activate
def test_fetch_stations_returns_list(fixtures_dir: Path) -> None:
    responses.add(
        responses.GET,
        collect_inmet.STATIONS_URL,
        body=_load(fixtures_dir, "inmet_stations.json"),
        status=200,
        content_type="application/json",
    )
    payload = collect_inmet.fetch_stations()
    assert isinstance(payload, list)
    assert len(payload) == 3
    assert payload[0]["CD_ESTACAO"] == "A001"


@responses.activate
def test_fetch_daily_returns_list(fixtures_dir: Path) -> None:
    url = collect_inmet.DAILY_URL_TMPL.format(
        start="2023-01-01", end="2023-01-03", code="A001", token="TEST_TOKEN"
    )
    responses.add(
        responses.GET,
        url,
        body=_load(fixtures_dir, "inmet_daily.json"),
        status=200,
        content_type="application/json",
    )
    payload = collect_inmet.fetch_daily("A001", "2023-01-01", "2023-01-03", "TEST_TOKEN")
    assert len(payload) == 3
    assert payload[0]["DT_MEDICAO"] == "2023-01-01"


@responses.activate
def test_fetch_daily_handles_http_error() -> None:
    url = collect_inmet.DAILY_URL_TMPL.format(
        start="2023-06-01", end="2023-06-30", code="A999", token="TEST_TOKEN"
    )
    responses.add(responses.GET, url, status=500)
    out = collect_inmet.fetch_daily("A999", "2023-06-01", "2023-06-30", "TEST_TOKEN")
    assert out == []


@responses.activate
def test_fetch_daily_handles_empty_body() -> None:
    url = collect_inmet.DAILY_URL_TMPL.format(
        start="2023-06-01", end="2023-06-30", code="A999", token="TEST_TOKEN"
    )
    responses.add(responses.GET, url, body="", status=200)
    out = collect_inmet.fetch_daily("A999", "2023-06-01", "2023-06-30", "TEST_TOKEN")
    assert out == []


@responses.activate
def test_download_history_zip(tmp_data_dir: Path) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("INMET_SE_MG_A001_TEST_01-01-2024_T_31-12-2024.CSV", "Data;Hora UTC\n2024-01-01;0000 UTC\n")
    body = buf.getvalue()

    url = collect_inmet.HIST_ZIP_URL_TMPL.format(year=2024)
    responses.add(responses.GET, url, body=body, status=200, content_type="application/zip")

    out = collect_inmet.download_history_zip(2024)
    assert out.exists()
    assert out.stat().st_size == len(body)

    # Idempotent: second call returns same path without HTTP
    out2 = collect_inmet.download_history_zip(2024)
    assert out2 == out


@responses.activate
def test_download_history_zip_raises_on_404(tmp_data_dir: Path) -> None:
    url = collect_inmet.HIST_ZIP_URL_TMPL.format(year=1999)
    responses.add(responses.GET, url, status=404)
    try:
        collect_inmet.download_history_zip(1999)
    except RuntimeError as exc:
        assert "404" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
