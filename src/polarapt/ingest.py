"""Task 1 Lambdas: lambda_jobs_ingest, lambda_benchmarks_scraper, lambda_fx_ingest.
Each is triggered by its own EventBridge rule every minute and writes one contract file to S3.
"""
from __future__ import annotations

import time

from .config import cfg
from .contract import put_raw
from .log import log
from .processors import bls as bls_parser
from .producers import bls, fx, usajobs


def _count_jobs(p) -> int:
    return len(((p or {}).get("SearchResult") or {}).get("SearchResultItems") or [])


def _count_benchmarks(p) -> int:
    from .contract import envelope, build, utcnow
    try:
        return len(bls_parser.parse(envelope(build("benchmarks", utcnow(), 0, 200), p), top_n=cfg().bls_top_n))
    except ValueError:
        return 0


def _count_fx(p) -> int:
    rates = (p or {}).get("rates") or {}
    return sum(1 for s in cfg().fx_symbols if s in rates)


_FETCH = {"jobs": (usajobs.fetch, _count_jobs), "benchmarks": (bls.fetch, _count_benchmarks), "fx": (fx.fetch, _count_fx)}


def ingest(source: str) -> dict:
    t0 = time.monotonic()
    fetch, count = _FETCH[source]
    payload, meta = fetch()
    status = int(meta.get("http_status", 200))
    key, contract = put_raw(source, payload, count(payload), status, {"mock": bool(meta.get("mock", False))})
    log(f"ingest-{source}", "landed", key=key, records=contract["record_count"], status=status,
        ms=int((time.monotonic() - t0) * 1000))
    return {"key": key, **contract}


def jobs_handler(event=None, context=None):
    return ingest("jobs")


def benchmarks_handler(event=None, context=None):
    return ingest("benchmarks")


def fx_handler(event=None, context=None):
    return ingest("fx")


if __name__ == "__main__":  # local smoke: python -m polarapt.ingest jobs
    import sys

    print(ingest(sys.argv[1] if len(sys.argv) > 1 else "fx"))
