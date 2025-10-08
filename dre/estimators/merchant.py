from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def estimate_plc_nspl_savings(
    *,
    current_plc_kw: float,
    current_nspl_kw: float,
    capacity_rate_per_kw_year: float,
    transmission_rate_per_kw_year: float,
    avg_reduction_kw: float,
    coverage_fraction_capacity: float,
    coverage_fraction_transmission: float,
) -> dict[str, float]:
    """
    Screening calculator for PLC/NSPL savings.

    New PLC = max(PLC - avg_reduction_kw * coverage_cap, 0)
    New NSPL = max(NSPL - avg_reduction_kw * coverage_tx, 0)

    Savings = (PLC_old - PLC_new) * cap_rate + (NSPL_old - NSPL_new) * tx_rate
    """
    plc_red = max(min(avg_reduction_kw * coverage_fraction_capacity, current_plc_kw), 0.0)
    nspl_red = max(min(avg_reduction_kw * coverage_fraction_transmission, current_nspl_kw), 0.0)

    new_plc = max(current_plc_kw - plc_red, 0.0)
    new_nspl = max(current_nspl_kw - nspl_red, 0.0)

    cap_save = plc_red * float(capacity_rate_per_kw_year)
    tx_save = nspl_red * float(transmission_rate_per_kw_year)

    return {
        "plc_reduction_kw": plc_red,
        "new_plc_kw": new_plc,
        "capacity_savings_usd_yr": cap_save,
        "nspl_reduction_kw": nspl_red,
        "new_nspl_kw": new_nspl,
        "transmission_savings_usd_yr": tx_save,
        "total_savings_usd_yr": cap_save + tx_save,
    }


def _to_dict(x: Any) -> dict[str, Any]:
    """Support pydantic BaseModel, dataclass, or dict for monthly billing rows."""
    if x is None:
        return {}
    if hasattr(x, "model_dump"):
        # pydantic v2 BaseModel
        return x.model_dump()
    if hasattr(x, "dict"):
        # pydantic v1
        try:
            return x.dict()
        except Exception:
            pass
    if isinstance(x, dict):
        return x
    # last resort
    return {}


def infer_kw_rates_from_monthly_billing(
    monthly_rows: Iterable[Any],
    current_plc_kw: float,
    current_nspl_kw: float,
) -> dict[str, float | None]:
    """
    Infer $/kW-year rates from BillingMonth rows on the active project.

    capacity_rate ≈ sum(capacity_usd) / PLC
    transmission_rate ≈ sum(transmission_usd) / NSPL

    Returns:
      {"capacity_rate_per_kw_year": float|None, "transmission_rate_per_kw_year": float|None}
    """
    total_cap = 0.0
    total_tx = 0.0
    count_cap = 0
    count_tx = 0

    for row in monthly_rows or []:
        d = _to_dict(row)
        c = d.get("capacity_usd")
        t = d.get("transmission_usd")
        if isinstance(c, (int, float)):
            total_cap += float(c)
            count_cap += 1
        if isinstance(t, (int, float)):
            total_tx += float(t)
            count_tx += 1

    cap_rate = (total_cap / current_plc_kw) if current_plc_kw and count_cap > 0 else None
    tx_rate = (total_tx / current_nspl_kw) if current_nspl_kw and count_tx > 0 else None

    return {
        "capacity_rate_per_kw_year": cap_rate,
        "transmission_rate_per_kw_year": tx_rate,
    }
