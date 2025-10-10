from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from dre.config import project_root
from dre.ops.pudl_fetch import ensure_tables


# --------------------------------------------------------------------------------------
# Local parquet location helpers
# --------------------------------------------------------------------------------------
def _dr_path(root: Path | None = None) -> Path:
    base = (root or project_root()) / "data" / "external" / "pudl"
    return base / "core_eia861__yearly_demand_response.parquet"

def dr_table_available(root: Path | None = None) -> bool:
    p = _dr_path(root)
    return p.exists() and p.stat().st_size > 0

def load_dr_table(root: Path | None = None) -> pd.DataFrame | None:
    """Load the PUDL EIA-861 Demand Response table if present."""
    p = _dr_path(root)
    try:
        return pd.read_parquet(p)  # type: ignore[no-any-return]
    except Exception:
        return None

# --------------------------------------------------------------------------------------
# Normalization
# --------------------------------------------------------------------------------------
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize common columns across PUDL DR vintages."""
    df = df.copy()
    if "customer_class" not in df.columns and "sector" in df.columns:
        df["customer_class"] = df["sector"]
    for col in ["customer_class", "state", "utility_name_eia", "balancing_authority_name_eia"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df

# --------------------------------------------------------------------------------------
# Sector mapping from project inputs
# --------------------------------------------------------------------------------------
_SECTOR_MAP: dict[str, str] = {
    "residential": "Residential",
    "commercial": "Commercial",
    "industrial": "Industrial",
    "gov": "Commercial",
    "government": "Commercial",
    "institutional": "Commercial",
    "ag": "Industrial",
    "agricultural": "Industrial",
    "muni": "Commercial",
    "municipal": "Commercial",
    "other": "Commercial",
}

def sector_from_project_customer_type(customer_type: str | None) -> str | None:
    if not customer_type:
        return None
    key = str(customer_type).strip().lower()
    return _SECTOR_MAP.get(key) or _SECTOR_MAP.get(key.split("/")[0]) or None

# --------------------------------------------------------------------------------------
# Filtering
# --------------------------------------------------------------------------------------
def filter_dr_for_entity(
    df: pd.DataFrame,
    *,
    utility_id_eia: int | None,
    ba_id_eia: int | None,
    state: str | None,
    sector: str | None = None,
) -> pd.DataFrame:
    """
    Preference order:
      1) utility_id_eia (if available in table)
      2) balancing_authority_id_eia
      3) state
    Then optionally filter by sector / customer_class if provided.
    """
    cols = set(df.columns)
    out = df.copy()

    if utility_id_eia is not None and "utility_id_eia" in cols:
        out = out[out["utility_id_eia"] == utility_id_eia]
    elif ba_id_eia is not None and "balancing_authority_id_eia" in cols:
        out = out[out["balancing_authority_id_eia"] == ba_id_eia]
    elif state and "state" in cols:
        out = out[out["state"].str.upper() == str(state).upper()]

    if sector and "customer_class" in out.columns:
        out = out[out["customer_class"].str.contains(sector, case=False, na=False)]

    if "report_date" in out.columns:
        out = out.sort_values("report_date")

    return out.reset_index(drop=True)

# --------------------------------------------------------------------------------------
# Robust parsing helpers (typing-friendly)
# --------------------------------------------------------------------------------------
def _safe_year_from_report_date(val: Any) -> int | None:
    """Extract a year from report_date robustly without tripping strict stubs."""
    if val is None:
        return None
    # Try pandas Timestamp first
    try:
        ts = pd.Timestamp(val)  # accepts str/dt/np datetime
        if not pd.isna(ts):
            return int(ts.year)
    except Exception:
        pass
    # Fallback: regex first 4-digit year
    try:
        m = re.search(r"(\d{4})", str(val))
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None

def _to_float(val: Any) -> float | None:
    """Best-effort float coercion that accepts Any (avoids Pylance complaints)."""
    if val is None:
        return None
    # Handle pandas NA / NaT
    try:
        if pd.isna(val):  # type: ignore[arg-type]
            return None
    except Exception:
        pass
    if isinstance(val, (int, float)):
        return float(val)
    try:
        s = str(val).strip()
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return None
        s = s.replace(",", "")
        return float(s)
    except Exception:
        return None

# --------------------------------------------------------------------------------------
# Summaries + Estimator
# --------------------------------------------------------------------------------------
def summarize_dr(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compact summary for the most recent year available.
    Returns numeric values (page can format as strings to avoid Arrow issues).
    """
    if df.empty:
        return pd.DataFrame({"metric": [], "value": []})
    latest = df.iloc[-1]
    year_val = _safe_year_from_report_date(latest.get("report_date"))

    fields = [
        ("Year", year_val),
        ("Customers enrolled", latest.get("customers_enrolled")),
        ("Potential peak reduction (MW)", latest.get("potential_peak_reduction_mw") or latest.get("potential_peak_savings_mw")),
        ("Actual peak reduction (MW)", latest.get("actual_peak_reduction_mw") or latest.get("actual_peak_savings_mw")),
        ("Program expenditures (USD)", latest.get("expenditures") or latest.get("total_cost_usd")),
        ("Customer class", latest.get("customer_class")),
        ("Utility", latest.get("utility_name_eia")),
        ("BA", latest.get("balancing_authority_name_eia")),
        ("State", latest.get("state")),
    ]
    rows = [{"metric": k, "value": v} for k, v in fields if k is not None]
    return pd.DataFrame(rows)

@dataclass
class DREstimate:
    year: int | None
    expenditures_usd: float | None
    actual_reduction_mw: float | None
    usd_per_kw_year: float | None

def latest_dr_estimate(df: pd.DataFrame) -> DREstimate:
    """
    Estimate DR $/kW-year from latest row:
      $/kW ≈ expenditures_usd / (actual_peak_reduction_mw * 1000)
    """
    if df.empty:
        return DREstimate(None, None, None, None)
    latest = df.iloc[-1]
    year_val = _safe_year_from_report_date(latest.get("report_date"))
    expenditures = _to_float(latest.get("expenditures") or latest.get("total_cost_usd"))
    actual_mw = _to_float(latest.get("actual_peak_reduction_mw") or latest.get("actual_peak_savings_mw"))
    per_kw = None
    if expenditures is not None and actual_mw is not None and actual_mw > 0:
        per_kw = expenditures / (actual_mw * 1000.0)
    return DREstimate(year_val, expenditures, actual_mw, per_kw)

def ensure_dr_available() -> None:
    """Make sure the DR parquet exists locally. Will download if missing."""
    if not dr_table_available():
        ensure_tables(force=False, only=["core_eia861__yearly_demand_response"])
