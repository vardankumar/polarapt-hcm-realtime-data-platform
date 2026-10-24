"""S2 BLS Highest Paying Occupations page. Raw HTML is landed; parsing happens in the processor."""
from __future__ import annotations

import hashlib

import requests

from ..config import FIXTURES, cfg



def fetch() -> tuple[dict, dict]:
    c = cfg()
    if c.mock:
        html = (FIXTURES / "bls_sample.html").read_text()
        return {"html": html}, {"mock": True, "sha256": hashlib.sha256(html.encode()).hexdigest()}
    # BLS returns 403 to the default python-requests User-Agent.
    headers = {"User-Agent": c.bls_ua, "Accept": "text/html", "Accept-Language": "en-US,en;q=0.9"}
    r = requests.get(c.bls_url, headers=headers, timeout=30)
    r.raise_for_status()
    html = r.text
    return {"html": html}, {"url": c.bls_url, "http_status": r.status_code,
                            "sha256": hashlib.sha256(html.encode()).hexdigest()}
