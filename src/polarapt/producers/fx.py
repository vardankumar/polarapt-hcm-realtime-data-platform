"""S3 Open Exchange Rates latest.json. Free plan: USD base only, hourly updates, 1,000 req/month."""
from __future__ import annotations

import json

import requests

from ..config import FIXTURES, cfg

URL = "https://openexchangerates.org/api/latest.json"



def fetch() -> tuple[dict, dict]:
    c = cfg()
    if c.mock:
        return json.loads((FIXTURES / "fx_sample.json").read_text()), {"mock": True}
    if not c.oxr_app_id:
        raise RuntimeError("OXR_APP_ID is required")
    # base omitted on purpose: free plan only allows USD and rejects an explicit base.
    params = {"app_id": c.oxr_app_id, "symbols": ",".join(c.fx_symbols)}
    r = requests.get(URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json(), {"symbols": list(c.fx_symbols), "http_status": r.status_code}
