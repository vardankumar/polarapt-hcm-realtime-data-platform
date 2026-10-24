"""Pure parser tests: no AWS, no network. Run first when a source changes shape."""
import json
import random

import pytest

from polarapt.config import FIXTURES, SOURCES
from polarapt.processors import bls, fx, usajobs
from polarapt.contract import build, envelope as _envelope, processed_key, raw_key, s3_metadata, source_from_key, utcnow


def envelope(source, payload, meta, ts):
    return _envelope(build(source, ts, 0, 200), payload)


def _env(source, payload):
    return envelope(source, payload, {}, utcnow())


# ---------- raw zone contract ----------
def test_raw_key_matches_pdf_layout():
    from datetime import datetime, timezone
    ts = datetime(2026, 10, 25, 1, 7, tzinfo=timezone.utc)
    assert raw_key("jobs", ts) == "raw/jobs/2026/10/25/0107.json"
    assert processed_key("raw/benchmarks/2026/10/25/0107.json") == "processed/benchmarks/2026/10/25/0107.json"
    for s in SOURCES:
        assert source_from_key(raw_key(s, ts)) == s


def test_contract_metadata_is_strings():
    md = s3_metadata(build("fx", utcnow(), 6, 200))
    assert md["source"] == "fx" and md["record-count"] == "6" and md["response-status"] == "200"
    assert all(isinstance(v, str) for v in md.values())


def test_source_from_key_rejects_foreign_prefix():
    with pytest.raises(ValueError):
        source_from_key("curated/fx/x.json")


# ---------- USAJOBS ----------
def test_usajobs_parse_dirty_rows():
    rows = usajobs.parse(_env("usajobs", json.loads((FIXTURES / "usajobs_sample.json").read_text())))
    by_id = {r["control_number"]: r for r in rows}
    assert len(rows) == 4                                   # Q2 dropped the broken row
    assert by_id["900000001"]["pay_min_annual"] == 117962   # PA passthrough
    assert by_id["900000002"]["pay_min_annual"] == 51132    # 24.50/h * 2087 (T1)
    assert by_id["900000002"]["location_count"] == 2
    assert "pay_min" not in by_id["900000003"]              # empty remuneration -> attr omitted
    assert "pay_min_annual" not in by_id["900000004"]       # WC -> no annual figure
    assert by_id["900000001"]["close_date"] == "2026-11-20T23:59:59Z"


def test_usajobs_q1_missing_result():
    with pytest.raises(ValueError, match="Q1"):
        usajobs.parse(_env("usajobs", {"oops": 1}))


# ---------- BLS ----------
def _bls_html():
    return (FIXTURES / "bls_sample.html").read_text()


def test_bls_top10_sorted_and_year():
    rows = bls.parse(_env("bls", {"html": _bls_html()}), top_n=10)
    assert [r["rank"] for r in rows] == list(range(1, 11))
    assert rows[0]["occupation"] == "Pediatric surgeons" and rows[0]["median_annual_pay"] == 559030
    assert rows[-1]["occupation"] == "Physicians, pathologists"
    assert all(r["pay_year"] == 2025 for r in rows)
    assert rows[0]["profile_url"].startswith("https://www.bls.gov/ooh/")


def test_bls_does_not_trust_page_order():
    html = _bls_html()
    head, body = html.split("<tbody>")
    trs, tail = body.split("</tbody>")
    lines = [ln for ln in trs.strip().splitlines()]
    random.Random(7).shuffle(lines)
    shuffled = head + "<tbody>\n" + "\n".join(lines) + "\n</tbody>" + tail
    rows = bls.parse(_env("bls", {"html": shuffled}), top_n=10)
    assert rows[0]["median_annual_pay"] == 559030


def test_bls_capped_and_hourly_variants():
    html = _bls_html().replace("$559,030 per year", "$559,030 or more per year", 1)
    rows = bls.parse(_env("bls", {"html": html}), top_n=10)
    assert rows[0]["pay_capped"] is True and rows[0]["median_annual_pay"] == 559030
    assert not any(r["pay_capped"] for r in rows[1:])
    row = bls.parse(_env("bls", {"html": _bls_html().replace("$496,010 per year", "$300.00 per hour")}), top_n=20)
    cardio = next(r for r in row if r["occupation"] == "Cardiologists")
    assert cardio["median_annual_pay"] == 624000 and cardio["pay_source_unit"] == "hour"


def test_bls_q3_too_few_rows():
    with pytest.raises(ValueError, match="Q3"):
        bls.parse(_env("bls", {"html": _bls_html()}), top_n=25)


def test_bls_q3_no_table():
    with pytest.raises(ValueError, match="Q3"):
        bls.parse(_env("bls", {"html": "<html><body>Access Denied</body></html>"}))


# ---------- FX ----------
SYMS = ("PKR", "SAR", "AED", "GBP", "EUR", "INR")


def test_fx_parse():
    rows = fx.parse(_env("fx", json.loads((FIXTURES / "fx_sample.json").read_text())), SYMS)
    assert {r["currency"] for r in rows} == set(SYMS)
    assert all(r["rate"] > 0 and r["base"] == "USD" for r in rows)


def test_fx_missing_symbol_is_warning_not_error():
    p = json.loads((FIXTURES / "fx_sample.json").read_text())
    del p["rates"]["PKR"]
    rows = fx.parse(_env("fx", p), SYMS)
    assert len(rows) == 5


def test_fx_q4_bad_base_and_rate():
    p = json.loads((FIXTURES / "fx_sample.json").read_text())
    with pytest.raises(ValueError, match="Q4"):
        fx.parse(_env("fx", {**p, "base": "EUR"}), SYMS)
    with pytest.raises(ValueError, match="Q4"):
        fx.parse(_env("fx", {**p, "rates": {**p["rates"], "PKR": 0}}), SYMS)


def test_glue_parser_matches_lambda_rules():
    import importlib.util, sys
    sys.argv = ["x", "--QUEUE_NAME", "q", "--SECRET_NAME", "s", "--REGION", "us-east-1"]
    spec = importlib.util.spec_from_file_location("g", "src/polarapt/glue/jobs_to_snowflake.py")
    g = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(g)
    except ImportError:
        return  # boto3 missing locally
    rows = g.parse(_env("jobs", json.loads((FIXTURES / "usajobs_sample.json").read_text())))
    by = {r[0]: r for r in rows}
    assert len(rows) == 4 and by["900000002"][13] == 51132 and by["900000004"][13] is None
