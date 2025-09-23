from __future__ import annotations

import math
import re
from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests

# ---------------------------------------------------------------------
# Minimal client: documented DSIRE endpoint (HTTP + JSON)
# ---------------------------------------------------------------------

class DSIREClient:
    """
    Exactly the documented endpoint:
      http://programs.dsireusa.org/api/v1/getprogramsbydate/[YYYYMMDD]/[YYYYMMDD]/json
    """

    def __init__(self, base_url: str = "http://programs.dsireusa.org/api/v1") -> None:
        self.base_url = base_url
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "DER-RevEngine/0.1 (dsire-client)"})

    def get_programs_by_date(self, yyyymmdd_start: str, yyyymmdd_end: str, timeout: int = 60) -> list[dict]:
        url = f"{self.base_url}/getprogramsbydate/{yyyymmdd_start}/{yyyymmdd_end}/json"
        resp = self._session.get(url, timeout=timeout)
        resp.raise_for_status()
        obj = resp.json()
        return _unwrap_programs(obj)


# ---------------------------------------------------------------------
# Month chunking helpers
# ---------------------------------------------------------------------

def month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    assert start <= end
    chunks: list[tuple[date, date]] = []
    cur = date(start.year, start.month, 1)
    chunk_start = start
    while cur <= end:
        if cur.month == 12:
            month_end = date(cur.year, 12, 31)
        else:
            month_end = date(cur.year, cur.month + 1, 1) - timedelta(days=1)
        chunk_end = min(month_end, end)
        chunks.append((chunk_start, chunk_end))
        cur = month_end + timedelta(days=1)
        chunk_start = cur
    return chunks


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


# ---------------------------------------------------------------------
# State normalization
# ---------------------------------------------------------------------

_US_STATE_CODE_TO_NAME: dict[str, str] = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado",
    "CT":"Connecticut","DE":"Delaware","DC":"District of Columbia","FL":"Florida","GA":"Georgia",
    "HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky",
    "LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota",
    "MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota",
    "OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina",
    "SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia",
    "WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
}
_US_STATE_NAME_TO_CODE: dict[str, str] = {v.upper(): k for k, v in _US_STATE_CODE_TO_NAME.items()}

def normalize_state(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if len(s) == 2 and s.upper() in _US_STATE_CODE_TO_NAME:
        return s.upper()
    code = _US_STATE_NAME_TO_CODE.get(s.upper(), "")
    return code


# ---------------------------------------------------------------------
# Record unwrapping and normalization (modeling-friendly)
# ---------------------------------------------------------------------

def _unwrap_programs(obj: Any) -> list[dict]:
    if obj is None:
        return []
    if isinstance(obj, list):
        return [r for r in obj if isinstance(r, dict)]
    if isinstance(obj, dict):
        for k in ("programs", "Programs", "results", "data", "items"):
            v = obj.get(k)
            if isinstance(v, list):
                return [r for r in v if isinstance(r, dict)]
        return [obj] if obj else []
    return []


def _get_any(d: Any, keys: Iterable[str], default=None):
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d:
            return d[k]
        kl = k.lower()
        for kk in d.keys():
            if isinstance(kk, str) and kk.lower() == kl:
                return d[kk]
    return default


def _join_names(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    if isinstance(val, list):
        parts = []
        for x in val:
            if isinstance(x, str):
                parts.append(x)
            elif isinstance(x, dict):
                nm = _get_any(x, ["Name", "name", "Technology", "Utility", "Sector"], default=None)
                if nm:
                    parts.append(str(nm))
        return ", ".join(sorted(set(p.strip() for p in parts if p)))
    return str(val)


def extract_program_id(r: dict) -> str:
    pid = _get_any(r, ["ProgramId", "programId", "id", "Program ID"], default=None)
    if pid is not None:
        return str(pid)
    url = _get_any(r, ["Website", "URL", "url", "Website URL", "WebsiteUrl", "ProgramURL"], default="")
    return str(url).strip()


def dedupe_records_by_program_id(records: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for r in records:
        if not isinstance(r, dict):
            continue
        key = extract_program_id(r)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def normalize_programs(records: list[dict]) -> pd.DataFrame:
    rows = []
    for r in records:
        if not isinstance(r, dict):
            continue
        pid = extract_program_id(r)
        state_raw = _get_any(r, ["State", "state"], default="")
        state = normalize_state(state_raw) or str(state_raw).strip()  # prefer code; fall back to raw
        name = _get_any(r, ["Program Name", "ProgramName", "Name", "name"], default="")
        admin = _get_any(r, ["Administrator", "AdministratorName", "admin"], default="")
        ptype = _get_any(r, ["Type", "Program Type", "type", "TypeName"], default="")
        category = _get_any(r, ["Category", "Program Category", "category", "CategoryName"], default="")
        url = _get_any(r, ["Website", "URL", "url", "Website URL", "WebsiteUrl", "ProgramURL"], default="")
        updated = _get_any(r, ["Last Update", "LastUpdated", "last_update", "lastUpdate"], default="")
        status = _get_any(r, ["Status", "status"], default="")
        techs = _join_names(_get_any(r, ["Technologies", "Technology", "tech"], default=[]))
        sectors = _join_names(_get_any(r, ["Sectors", "Sector"], default=[]))
        utilities = _join_names(_get_any(r, ["Utilities", "Utility"], default=[]))

        rows.append(
            {
                "program_id": pid,
                "state": state,
                "program_name": str(name).strip(),
                "administrator": str(admin),
                "type": str(ptype),
                "category": str(category),
                "website_url": str(url),
                "status": str(status),
                "last_update": str(updated),
                "technologies": techs,
                "sectors": sectors,
                "utilities": utilities,
                "raw_json": r,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------
# Parameter extraction (structured + derived from Details)
# ---------------------------------------------------------------------

_AMT_KW = re.compile(r"\$([\d,]+(?:\.\d+)?)\s*/\s*kW\b", re.I)
_AMT_KWH = re.compile(r"\$([\d,]+(?:\.\d+)?)\s*/\s*kWh\b", re.I)
_AMT_W = re.compile(r"\$([\d,]+(?:\.\d+)?)\s*/\s*W\b", re.I)
_AMT_PCT = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*%\b", re.I)
_AMT_CAP = re.compile(r"(?:up to|maximum(?: incentive)?|cap)\s*\$([\d,]+(?:\.\d+)?)", re.I)

def _strip_html(s: str | None) -> str | None:
    if not isinstance(s, str) or not s.strip():
        return None
    s = s.replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    return s.strip() or None

def _extract_amounts_any(text: str | None) -> list[dict[str, Any]]:
    if not text:
        return []
    hits: list[dict[str, Any]] = []
    for pat, units in ((_AMT_KW, "$/kW"), (_AMT_KWH, "$/kWh"), (_AMT_W, "$/W")):
        for m in pat.finditer(text):
            hits.append({"amount": float(m.group(1).replace(",", "")), "units": units})
    for m in _AMT_PCT.finditer(text):
        hits.append({"amount": float(m.group(1)), "units": "%"})
    for m in _AMT_CAP.finditer(text):
        hits.append({"amount": float(m.group(1).replace(",", "")), "units": "USD", "qualifier": "cap"})
    return hits

def parameters_long(records: list[dict]) -> pd.DataFrame:
    out = []
    for r in records:
        if not isinstance(r, dict):
            continue
        pid = extract_program_id(r)

        # Structured parameters if present
        param_sets = _get_any(r, ["ProgramParameters", "Parameters", "Incentives"], default=[])
        if isinstance(param_sets, list):
            for p in param_sets:
                if not isinstance(p, dict):
                    continue
                label = _get_any(p, ["Label", "label", "Name", "name"], default="")
                units = _get_any(p, ["Units", "Unit", "units"], default="")
                amount = _get_any(p, ["Amount", "Value", "value", "amount"], default=None)
                minimum = _get_any(p, ["Min", "Minimum", "min"], default=None)
                maximum = _get_any(p, ["Max", "Maximum", "max"], default=None)
                tech = _get_any(p, ["Technology", "technology"], default=None)
                sector = _get_any(p, ["Sector", "sector"], default=None)
                source = _get_any(p, ["Source", "source"], default="ProgramParameters")

                def _to_float(x):
                    try:
                        if isinstance(x, str):
                            x2 = x.replace("$", "").replace(",", "").strip()
                            return float(x2)
                        return float(x)
                    except Exception:
                        return math.nan

                for qualifier, val in (("amount", amount), ("min", minimum), ("max", maximum)):
                    if val is None:
                        continue
                    out.append(
                        {
                            "program_id": pid,
                            "tech": str(tech) if tech is not None else None,
                            "sector": str(sector) if sector is not None else None,
                            "source": str(source),
                            "qualifier": qualifier,
                            "amount": _to_float(val),
                            "units": str(units),
                            "raw_label": str(label),
                            "raw_value": val,
                        }
                    )

        # Derived from Details (narrative)
        details = _get_any(r, ["Details"], default=[])
        if isinstance(details, list):
            det_map: dict[str, str] = {}
            for d in details:
                if not isinstance(d, dict):
                    continue
                label = str(_get_any(d, ["label", "Label"], default="")).strip()
                txt = _strip_html(_get_any(d, ["value", "Value"], default=None))
                if label and txt:
                    det_map[label] = txt

            incentive_text = det_map.get("Incentive Amount") or det_map.get("Incentive") or det_map.get("Benefit Details")
            max_incentive_tx = det_map.get("Maximum Incentive")

            for hit in _extract_amounts_any(incentive_text):
                out.append(
                    {
                        "program_id": pid,
                        "tech": None,
                        "sector": None,
                        "source": "DerivedFromDetails",
                        "qualifier": hit.get("qualifier"),
                        "amount": float(hit["amount"]),
                        "units": str(hit["units"]),
                        "raw_label": "Incentive Amount",
                        "raw_value": incentive_text,
                    }
                )
            for hit in _extract_amounts_any(max_incentive_tx):
                out.append(
                    {
                        "program_id": pid,
                        "tech": None,
                        "sector": None,
                        "source": "DerivedFromDetails",
                        "qualifier": hit.get("qualifier", "cap"),
                        "amount": float(hit["amount"]),
                        "units": str(hit.get("units", "USD")),
                        "raw_label": "Maximum Incentive",
                        "raw_value": max_incentive_tx,
                    }
                )

    return pd.DataFrame(out)