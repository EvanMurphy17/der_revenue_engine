from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from dre.clients.openei import OpenEIClient, OpenEIResult
from dre.config import project_root
from dre.ops.pudl_fetch import ensure_tables


@dataclass
class IsoInference:
    utility_name: str | None
    utility_id_eia: int | None
    balancing_authority_id_eia: int | None
    balancing_authority_name: str | None
    iso_rto: str | None
    method: str  # "pudl", "openei_only", "state_heuristic", "unknown"
    state: str | None
    provenance: dict | None = None


# Simple fallback by state → ISO/RTO
_STATE_TO_ISO: dict[str, str] = {
    "CA": "CAISO", "NY": "NYISO", "TX": "ERCOT",
    "ME": "ISONE", "NH": "ISONE", "VT": "ISONE", "MA": "ISONE", "RI": "ISONE", "CT": "ISONE",
    "ND": "MISO", "SD": "MISO", "MN": "MISO", "WI": "MISO", "IA": "MISO", "MI": "MISO",
    "IL": "PJM", "IN": "PJM", "OH": "PJM", "PA": "PJM", "NJ": "PJM", "MD": "PJM", "DE": "PJM",
    "DC": "PJM", "VA": "PJM", "WV": "PJM", "NC": "PJM",
    "KS": "SPP", "OK": "SPP", "NE": "SPP",
    "FL": "FRCC",
    "AL": "SERC", "GA": "SERC", "SC": "SERC", "TN": "SERC",
    "AZ": "WECC", "NV": "WECC", "UT": "WECC", "CO": "WECC", "NM": "WECC",
    "OR": "WECC", "WA": "WECC", "ID": "WECC", "MT": "WECC", "WY": "WECC",
}


def _parse_state(address: str) -> str | None:
    m = re.findall(r"\b([A-Z]{2})\b", (address or "").upper())
    return m[-1] if m else None


def _paths() -> dict[str, Path]:
    base = project_root() / "data" / "external" / "pudl"
    return {
        "misc": base / "core_eia861__yearly_utility_data_misc.parquet",
        "ba": base / "core_eia861__assn_balancing_authority.parquet",
        "rto": base / "core_eia861__yearly_utility_data_rto.parquet",
    }


def _load_parquet(p: Path) -> pd.DataFrame | None:
    try:
        return pd.read_parquet(p)  # type: ignore[no-any-return]
    except Exception:
        return None


def _derive_iso_from_columns(df_row: pd.Series, state: str | None) -> str | None:
    # Try explicit RTO/ISO columns first
    for col in ("rto", "rto_iso", "rto_iso_code", "rto_name"):
        if col in df_row.index and isinstance(df_row[col], str) and df_row[col].strip():
            return df_row[col].strip().upper()
    # Fallback: use NERC to approximate via state (coarse)
    if "nerc_region" in df_row.index and isinstance(df_row["nerc_region"], str):
        # leave final decision to state fallback; too coarse otherwise
        return _STATE_TO_ISO.get(state) if state else None
    return _STATE_TO_ISO.get(state) if state else None


def _pick_latest(df: pd.DataFrame, util_id: int, year: int | None) -> pd.Series | None:
    d = df[df.get("utility_id_eia") == util_id].copy()
    if "report_date" in d.columns:
        d["__yr"] = pd.to_datetime(d["report_date"], errors="coerce").dt.year
        if year is not None:
            d = d[d["__yr"] <= int(year)]
        d = d.sort_values("report_date")
    return d.iloc[-1] if len(d) else None


def infer_iso_from_address(
    address: str,
    year: int | None = None,
    openei_api_key: str | None = None,
    *,
    require_pudl: bool = False,
    want_trace: bool = False,
) -> tuple[IsoInference, pd.DataFrame | None]:
    """
    1) OpenEI -> utility name (+ try to get utility_id_eia)
    2) If utility_id_eia found: use PUDL EIA-861 parquets to get BA & ISO/RTO.
    3) If require_pudl=True and no PUDL-based mapping, raise RuntimeError.
       Else fallback to state heuristic.
    Returns IsoInference and optional trace dataframe (rows used).
    """
    # Ensure parquets exist (no-op if already present)
    ensure_tables(force=False)

    state_guess = _parse_state(address)

    oe = OpenEIClient(api_key=openei_api_key)
    results: list[OpenEIResult] = oe.get_by_address(address)
    top: OpenEIResult | None = results[0] if results else None

    util_name: str | None = top.utility_name if top else None
    util_id: int | None = top.utility_id_eia if top else None
    state_guess = top.state if (top and top.state) else state_guess

    # If EIA ID missing, try alias search
    if util_id is None and util_name:
        aliases: list[dict[str, Any]] = oe.get_utility_aliases(util_name)
        for a in aliases:
            for key in ("eiaid", "eia_id", "utility_id_eia"):
                val = a.get(key)
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    util_id = int(val)
                    break
                if isinstance(val, str) and val.isdigit():
                    util_id = int(val)
                    break
            if util_id is not None:
                break

    paths = _paths()
    misc = _load_parquet(paths["misc"])
    ba = _load_parquet(paths["ba"])
    rto = _load_parquet(paths["rto"])

    # Require PUDL: fail early if files missing or EIA ID missing
    if require_pudl and (util_id is None or misc is None or ba is None):
        missing: list[str] = []
        if util_id is None:
            missing.append("utility_id_eia (OpenEI)")
        if misc is None:
            missing.append(paths["misc"].name)
        if ba is None:
            missing.append(paths["ba"].name)
        raise RuntimeError("PUDL mapping required but not available: " + ", ".join(missing))

    trace_rows: list[pd.Series] = []
    ba_row: pd.Series | None = None
    rto_row: pd.Series | None = None
    misc_row: pd.Series | None = None

    if util_id is not None and any(x is not None for x in (misc, ba, rto)):
        if ba is not None:
            ba_row = _pick_latest(ba, util_id, year)
            if ba_row is not None:
                trace_rows.append(ba_row)
        if rto is not None:
            rto_row = _pick_latest(rto, util_id, year)
            if rto_row is not None:
                trace_rows.append(rto_row)
        if misc is not None:
            misc_row = _pick_latest(misc, util_id, year)
            if misc_row is not None:
                trace_rows.append(misc_row)

    if util_id is not None and (ba_row is not None or rto_row is not None or misc_row is not None):
        ba_id = int(ba_row["balancing_authority_id_eia"]) if (ba_row is not None and "balancing_authority_id_eia" in ba_row) else None
        ba_name = str(ba_row["balancing_authority_name_eia"]) if (ba_row is not None and "balancing_authority_name_eia" in ba_row) else None

        iso = None
        if rto_row is not None:
            iso = _derive_iso_from_columns(rto_row, state_guess)
        if iso is None and misc_row is not None:
            iso = _derive_iso_from_columns(misc_row, state_guess)
        if iso is None and ba_name:
            s = ba_name.lower()
            if "pjm" in s:
                iso = "PJM"
            elif "miso" in s:
                iso = "MISO"
            elif "caiso" in s:
                iso = "CAISO"
            elif "nyiso" in s:
                iso = "NYISO"
            elif "iso new england" in s or "isone" in s:
                iso = "ISONE"
            elif "spp" in s or "southwest power pool" in s:
                iso = "SPP"

        info = IsoInference(
            utility_name=util_name,
            utility_id_eia=util_id,
            balancing_authority_id_eia=ba_id,
            balancing_authority_name=ba_name,
            iso_rto=iso or (_STATE_TO_ISO.get(state_guess) if state_guess else None),
            method="pudl",
            state=state_guess,
            provenance={
                "sources": {
                    "misc": str(paths["misc"]),
                    "ba": str(paths["ba"]),
                    "rto": str(paths["rto"]),
                },
                "year": int(year) if year is not None else None,
            },
        )
        trace_df = pd.DataFrame([sr for sr in trace_rows]) if (want_trace and trace_rows) else None
        return info, trace_df

    # If require PUDL and nothing usable found:
    if require_pudl:
        raise RuntimeError("PUDL mapping required but could not determine BA/ISO for the utility.")

    # Fallbacks:
    if util_name or util_id:
        return IsoInference(
            utility_name=util_name,
            utility_id_eia=util_id,
            balancing_authority_id_eia=None,
            balancing_authority_name=None,
            iso_rto=_STATE_TO_ISO.get(state_guess) if state_guess else None,
            method="openei_only",
            state=state_guess,
            provenance={"reason": "no PUDL row matched"},
        ), None

    return IsoInference(
        utility_name=None,
        utility_id_eia=None,
        balancing_authority_id_eia=None,
        balancing_authority_name=None,
        iso_rto=_STATE_TO_ISO.get(state_guess) if state_guess else None,
        method=("state_heuristic" if state_guess else "unknown"),
        state=state_guess,
        provenance={"reason": "no OpenEI match"},
    ), None


def pudl_available() -> tuple[bool, str | None]:
    """
    Return whether all required PUDL parquets exist locally, and the directory path.
    """
    base = project_root() / "data" / "external" / "pudl"
    req = [
        "core_eia861__yearly_utility_data_misc.parquet",
        "core_eia861__assn_balancing_authority.parquet",
        "core_eia861__yearly_utility_data_rto.parquet",
    ]
    ok = all((base / f).exists() and (base / f).stat().st_size > 0 for f in req)
    return ok, str(base)


def is_pudl_based(info: IsoInference) -> bool:
    return info.method == "pudl"
