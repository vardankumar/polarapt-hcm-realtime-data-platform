"""S1 processor: USAJOBS envelope -> job_postings rows (SPEC §7.3, T1, Q1-Q2)."""
from __future__ import annotations

from decimal import Decimal

from ..log import log

ANNUALIZE = {"PA": 1, "PH": 2087, "PD": 260, "PW": 52, "BW": 26, "SM": 24, "PM": 12}


def _num(v) -> Decimal | None:
    try:
        return None if v in (None, "") else Decimal(str(v).replace(",", ""))
    except Exception:  # noqa: BLE001
        return None


def _annual(v: Decimal | None, code: str | None) -> Decimal | None:
    f = ANNUALIZE.get((code or "").upper())
    return None if v is None or f is None else (v * f).quantize(Decimal("1"))


def _iso(v: str | None) -> str | None:
    if not v:
        return None
    return v if v.endswith("Z") else v[:19] + "Z"  # API sends ...T00:00:00.0000 or ...Z


def parse(env: dict) -> list[dict]:
    try:
        items = env["payload"]["SearchResult"]["SearchResultItems"]
    except (KeyError, TypeError) as exc:
        raise ValueError("Q1: payload missing SearchResult.SearchResultItems") from exc
    rows, dropped = [], 0
    for it in items:
        d = it.get("MatchedObjectDescriptor") or {}
        cn, title = str(it.get("MatchedObjectId") or "").strip(), (d.get("PositionTitle") or "").strip()
        if not cn or not title:
            dropped += 1
            continue
        rem = (d.get("PositionRemuneration") or [{}])[0] or {}
        code = rem.get("RateIntervalCode")
        pmin, pmax = _num(rem.get("MinimumRange")), _num(rem.get("MaximumRange"))
        locs = d.get("PositionLocation") or []
        loc = locs[0] if locs else {}
        apply = d.get("ApplyURI") or []
        row = {
            "control_number": cn,
            "position_id": d.get("PositionID"),
            "title": title,
            "organization": d.get("OrganizationName"),
            "department": d.get("DepartmentName"),
            "location_display": d.get("PositionLocationDisplay"),
            "city": loc.get("CityName"),
            "state": loc.get("CountrySubDivisionCode"),
            "country": loc.get("CountryCode"),
            "lat": _num(loc.get("Latitude")),
            "lon": _num(loc.get("Longitude")),
            "location_count": len(locs),
            "pay_min": pmin,
            "pay_max": pmax,
            "pay_interval": code,
            "pay_min_annual": _annual(pmin, code),
            "pay_max_annual": _annual(pmax, code),
            "open_date": _iso(d.get("PublicationStartDate")),
            "close_date": _iso(d.get("ApplicationCloseDate")),
            "position_url": d.get("PositionURI"),
            "apply_url": apply[0] if apply else None,
        }
        rows.append({k: v for k, v in row.items() if v not in (None, "")})
    if dropped:
        log("proc-usajobs", "q2_dropped", dropped=dropped)
    return rows

