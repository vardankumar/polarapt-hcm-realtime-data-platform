"""S3 processor: OXR latest.json -> fx_rates rows (SPEC §7.5, T3, Q4)."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from ..log import log


def parse(env: dict, symbols: tuple[str, ...]) -> list[dict]:
    p = env["payload"]
    if p.get("base") != "USD":
        raise ValueError(f"Q4: base must be USD, got {p.get('base')}")
    ts = int(p["timestamp"])
    rates = p.get("rates") or {}
    missing = [s for s in symbols if s not in rates]
    if missing:
        log("proc-fx", "t3_missing_symbols", missing=missing)
    as_of = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []
    for s in symbols:
        if s not in rates:
            continue
        rate = Decimal(str(rates[s]))
        if rate <= 0:
            raise ValueError(f"Q4: non-positive rate {s}={rate}")
        rows.append({"currency": s, "rate_ts": ts, "rate": rate, "base": "USD", "as_of": as_of,
                     "fetched_at": env["fetched_at"]})
    if not rows:
        raise ValueError("Q4: no requested symbols in payload")
    return rows

