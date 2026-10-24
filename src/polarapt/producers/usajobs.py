"""S1 USAJOBS Search API. Docs: developer.usajobs.gov/api-reference/get-api-search"""
from __future__ import annotations

import json

import requests

from ..config import FIXTURES, cfg

URL = "https://data.usajobs.gov/api/Search"



def fetch() -> tuple[dict, dict]:
    c = cfg()
    if c.mock:
        return json.loads((FIXTURES / "usajobs_sample.json").read_text()), {"mock": True}
    if not (c.usajobs_key and c.usajobs_ua):
        raise RuntimeError("USAJOBS_API_KEY and USAJOBS_USER_AGENT (your email) are required")
    params = {"ResultsPerPage": c.usajobs_per_page, "Page": 1, "SortField": "opendate", "SortDirection": "desc"}
    if c.usajobs_keyword:
        params["Keyword"] = c.usajobs_keyword
    headers = {"Host": "data.usajobs.gov", "User-Agent": c.usajobs_ua, "Authorization-Key": c.usajobs_key}
    r = requests.get(URL, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    body = r.json()
    sr = body.get("SearchResult", {})
    return body, {"params": params, "http_status": r.status_code,
                  "count": sr.get("SearchResultCount"), "count_all": sr.get("SearchResultCountAll")}
