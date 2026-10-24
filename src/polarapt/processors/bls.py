"""S2 processor: BLS HTML -> top-N pay_benchmarks rows (SPEC §7.4, T2, Q3)."""
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

BASE = "https://www.bls.gov"
PAY_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")
YEAR_RE = re.compile(r"(\d{4})\s*MEDIAN\s*PAY", re.I)


def _find_table(soup: BeautifulSoup):
    for tbl in soup.find_all("table"):
        if "MEDIAN PAY" in tbl.get_text(" ", strip=True).upper():
            return tbl
    raise ValueError("Q3: no table with a MEDIAN PAY header")


def parse(env: dict, top_n: int = 10) -> list[dict]:
    soup = BeautifulSoup(env["payload"]["html"], "html.parser")
    tbl = _find_table(soup)
    header = " ".join(th.get_text(" ", strip=True) for th in tbl.find_all("th")[:6])
    m = YEAR_RE.search(header) or YEAR_RE.search(tbl.get_text(" ", strip=True))
    if not m:
        raise ValueError("Q3: pay year not found in header")
    pay_year = int(m.group(1))

    parsed = []
    for tr in tbl.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        pay_text = cells[-1].get_text(" ", strip=True)
        pm = PAY_RE.search(pay_text)
        if not pm:
            continue  # header or junk row
        name = cells[0].get_text(" ", strip=True)
        a = cells[0].find("a")
        hourly = "per hour" in pay_text.lower()
        amount = float(pm.group(1).replace(",", ""))
        parsed.append({
            "occupation": re.sub(r"\s+", " ", name),
            "median_annual_pay": int(round(amount * 2080 if hourly else amount)),
            "pay_capped": "or more" in pay_text.lower(),
            "pay_source_unit": "hour" if hourly else "year",
            "pay_year": pay_year,
            "profile_url": urljoin(BASE, a["href"]) if a and a.get("href") else None,
        })
    # T2: never trust page order; sort, then cut
    parsed.sort(key=lambda r: r["median_annual_pay"], reverse=True)
    top = parsed[:top_n]
    if len(top) < top_n or any(r["median_annual_pay"] <= 0 for r in top):
        raise ValueError(f"Q3: expected {top_n} valid rows, got {len(top)}")
    for i, r in enumerate(top, 1):
        r["rank"] = i
    return [{k: v for k, v in r.items() if v is not None} for r in top]

