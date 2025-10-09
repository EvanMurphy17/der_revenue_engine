from __future__ import annotations

import io
import os
import re
import sys
from collections.abc import Iterable, Sequence
from datetime import date
from pathlib import Path
from typing import Any, cast

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from dre.clients.pjm import PJMClient
from dre.config import project_root
from dre.estimators.merchant import estimate_plc_nspl_savings, infer_kw_rates_from_monthly_billing
from dre.markets.pjm.cache import (
    cache_root,
    load_or_fetch_window_report,
    rolling_12_full_months,
)
from dre.markets.pjm.estimate_frequency import (
    BESSParams,
    Ranking,
    compute_n_hours_from_cycles,
    estimate_reg_revenue_top_n,
)

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# Load env so PJM keys are visible
try:
    load_dotenv(project_root() / ".env", override=False)
except Exception:
    pass

# --------- small helpers ----------
def _to_dict(x: Any) -> dict[str, Any]:
    if x is None:
        return {}
    for meth in ("model_dump", "dict"):
        if hasattr(x, meth):
            try:
                return getattr(x, meth)()
            except Exception:
                pass
    if isinstance(x, dict):
        return x
    if hasattr(x, "__dict__"):
        return dict(x.__dict__)
    return {}


def _rows_iter(rows: Iterable[Any]) -> Iterable[dict[str, Any]]:
    for r in rows or []:
        yield _to_dict(r)


def _derive_bess(bundle_bess: Any) -> tuple[float, float, float | None]:
    b = _to_dict(bundle_bess)
    rows = list(_rows_iter(b.get("rows", [])))
    total_kw = float(sum(float(r.get("power_kw") or 0.0) for r in rows))
    total_kwh = float(sum(float(r.get("energy_kwh") or 0.0) for r in rows))
    duration = (total_kwh / total_kw) if (total_kw > 0 and total_kwh > 0) else None
    return total_kw, total_kwh, duration


def _to_excel_bytes(sheets: dict[str, pd.DataFrame]) -> bytes | None:
    try:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            for name, df in sheets.items():
                df.to_excel(writer, index=False, sheet_name=name[:31])
        buf.seek(0)
        return buf.read()
    except Exception:
        return None


def _normalize_asof(asof_input: Any) -> date:
    if isinstance(asof_input, date):
        return asof_input
    if isinstance(asof_input, (list, tuple, Sequence)):
        if len(asof_input) >= 1 and isinstance(asof_input[0], date):
            return asof_input[0]
    return date.today()


# --------- page ----------
st.set_page_config(page_title="Merchant overlay", layout="wide")
st.title("Merchant Overlay")
st.caption("Estimate PLC/NSPL savings and PJM frequency regulation revenue using the active project.")

if "site_bundle" not in st.session_state:
    st.warning("No active project. Open **Home and projects** and activate a project.")
    st.page_link("pages/01_Home_and_projects.py", label="Go to Home and projects", icon="🏠")
    st.stop()

bundle = st.session_state["site_bundle"]
identity = _to_dict(bundle.get("identity"))
bess_spec = bundle.get("bess")
pv_spec = _to_dict(bundle.get("pv"))
tariff = bundle.get("tariff")

proj_name = identity.get("name", "Untitled project")
addr = (identity.get("site_address") or "").strip()


def _guess_state(address: str) -> str:
    m = re.findall(r"\b([A-Z]{2})\b", address.upper())
    return m[-1] if m else "PA"


state_code = _guess_state(addr)
total_bess_kw, total_bess_kwh, bess_duration = _derive_bess(bess_spec)
nameplate_mw = total_bess_kw / 1000.0 if total_bess_kw > 0 else 0.0

with st.expander("Active project summary (derived)", expanded=True):
    st.write(
        {
            "project": proj_name,
            "address": addr,
            "state": state_code,
            "BESS total power (kW)": total_bess_kw,
            "BESS total energy (kWh)": total_bess_kwh,
            "BESS duration (hours)": bess_duration,
            "PV DC (kWdc)": pv_spec.get("dc_nameplate_kw"),
            "PV AC (kWac)": pv_spec.get("ac_nameplate_kw"),
        }
    )

if total_bess_kw <= 0 or not bess_duration or bess_duration <= 0:
    st.error("Active project must have BESS power (kW) and energy (kWh) to proceed.")
    st.stop()

# ---------- PLC/NSPL ----------
st.header("1) PLC / NSPL savings")

left_col, right_col = st.columns([1, 1])
with left_col:
    cur_plc_kw = st.number_input("Current PLC (kW)", min_value=0.0, value=1000.0, step=10.0)
    cur_nspl_kw = st.number_input("Current NSPL (kW)", min_value=0.0, value=1000.0, step=10.0)

    monthly_rows = []
    try:
        monthly_rows = (
            tariff.monthly_billing or []
            if hasattr(tariff, "monthly_billing")
            else (_to_dict(tariff).get("monthly_billing", []) or [])
        )
    except Exception:
        monthly_rows = []

    inferred = infer_kw_rates_from_monthly_billing(monthly_rows, cur_plc_kw, cur_nspl_kw)
    cap_rate_default = float(inferred.get("capacity_rate_per_kw_year") or 110.0)
    tx_rate_default = float(inferred.get("transmission_rate_per_kw_year") or 80.0)

    cap_rate = st.number_input("Capacity rate ($/kW-year)", min_value=0.0, value=cap_rate_default, step=5.0)
    tx_rate = st.number_input("Transmission rate ($/kW-year)", min_value=0.0, value=tx_rate_default, step=5.0)

with right_col:
    st.caption("Average sustained kW reduction reflects PV+BESS dispatch during CP/NP events.")
    avg_kW_reduction = st.number_input(
        "Average kW reduction during CP/NP events", min_value=0.0, value=min(total_bess_kw, 500.0), step=10.0
    )
    coverage_cap = st.slider("Capacity tag coverage fraction (0-1)", 0.0, 1.0, 0.7, 0.05)
    coverage_tx = st.slider("Transmission tag coverage fraction (0-1)", 0.0, 1.0, 0.7, 0.05)

plc_results = estimate_plc_nspl_savings(
    current_plc_kw=cur_plc_kw,
    current_nspl_kw=cur_nspl_kw,
    capacity_rate_per_kw_year=cap_rate,
    transmission_rate_per_kw_year=tx_rate,
    avg_reduction_kw=avg_kW_reduction,
    coverage_fraction_capacity=coverage_cap,
    coverage_fraction_transmission=coverage_tx,
)

st.subheader("Estimated annual savings")
st.write(
    {
        "PLC reduced (kW)": plc_results["plc_reduction_kw"],
        "New PLC (kW)": plc_results["new_plc_kw"],
        "Capacity savings ($/yr)": plc_results["capacity_savings_usd_yr"],
        "NSPL reduced (kW)": plc_results["nspl_reduction_kw"],
        "New NSPL (kW)": plc_results["new_nspl_kw"],
        "Transmission savings ($/yr)": plc_results["transmission_savings_usd_yr"],
        "Total PLC+NSPL savings ($/yr)": plc_results["total_savings_usd_yr"],
    }
)

# ---------- Frequency regulation ----------
st.header("2) Frequency regulation revenue (Top-N)")

# As-of month → previous 12 full months (exclusive end)
asof_input = st.date_input(
    "As-of month (we’ll use the 12 full months ending the month before this)",
    value=date.today(),
)
asof_date = _normalize_asof(asof_input)
win_start, win_end = rolling_12_full_months(asof_date)
st.caption(f"Window (exclusive end) = {win_start:%Y-%m-%d %H:%M} → {win_end:%Y-%m-%d %H:%M}")

# Fetch/cache switch
root = project_root()
base = cache_root(root)
env_key_detected = bool(
    os.getenv("PJM_API_PRIMARY_KEY") or os.getenv("PJM_API_KEY") or os.getenv("PJM_PRIMARY_KEY") or os.getenv("PJM_KEY")
)
use_api = st.toggle(
    "Fetch any missing months from PJM Data Miner (and cache locally)",
    value=env_key_detected,
)
if use_api and env_key_detected:
    st.caption(f"Environment key detected. Cache root: `{base}`")

client = PJMClient() if use_api else None
hourly, report = load_or_fetch_window_report(client, root, win_start, win_end, fetch_missing=use_api)

with st.expander("Cache / Fetch report by month", expanded=hourly.empty):
    st.dataframe(pd.DataFrame(report), use_container_width=True, hide_index=True)

if hourly.empty:
    st.error(
        "No hourly data available for the selected window. "
        "Enable the fetch toggle (with API keys) to populate the cache."
    )
    st.stop()

# Inputs for cycles + throughput (new)
c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    annual_cycles = st.number_input("Total annual regulation cycles", min_value=1, value=365, step=1)
with c2:
    throughput_ratio = st.number_input(
        "Throughput ratio (MWh per hour, 0–1]",
        min_value=0.01,
        max_value=1.0,
        value=0.50,
        step=0.01,
        format="%.2f",
        help="Average energy used per regulation operating hour. Example: 0.50 = 0.5 MWh per hour.",
    )
with c3:
    ranking_choice = st.selectbox(
        "Ranking metric",
        options=[
            "Full hourly payment (RMCCP + RMPCP × Mileage)",
            "RMCCP (capability price)",
            "RMPCP (performance price)",
        ],
        index=0,
        help="Consecutive-hours constraint still applies (limited by BESS duration).",
    )
rank_map: dict[str, Ranking] = {
    "Full hourly payment (RMCCP + RMPCP × Mileage)": "full",
    "RMCCP (capability price)": "rmccp",
    "RMPCP (performance price)": "rmpcp",
}
ranking: Ranking = cast(Ranking, rank_map[ranking_choice])

perf_score = st.number_input(
    "Assumed performance score (0–1)", min_value=0.0, max_value=1.0, value=0.90, step=0.05,
    help="Used in Capability and Performance credits.",
)

# Build BESS params and hours budget from cycles
bess = BESSParams(nameplate_mw=nameplate_mw, duration_hours=float(bess_duration))
window_hours = int(len(hourly))
n_hours = compute_n_hours_from_cycles(
    bess=bess,
    annual_cycles=int(annual_cycles),
    throughput_ratio_mwh_per_hour=float(throughput_ratio),
    window_hours=window_hours,
)

# Show the derived hours and the formula
with st.expander("Hours budget from cycles (how we compute N)", expanded=True):
    st.markdown(
        f"""
**Inputs**  
- Duration = `{bess.duration_hours:.2f}` hours  
- Throughput ratio = `{throughput_ratio:.2f}` MWh/hour  
- Annual cycles = `{annual_cycles}`  

**Formula**  
Hours per cycle ≈ `duration / throughput_ratio`  
Total hours ≈ `(duration / throughput_ratio) × annual_cycles`  

**Computed**  
- Hours per cycle ≈ `{bess.duration_hours/throughput_ratio:.2f}`  
- Total hours (before clamp) ≈ `{(bess.duration_hours/throughput_ratio)*annual_cycles:.2f}`  
- Available window hours = `{window_hours}`  
- **Hours used for Top-N** = `{n_hours}`  
"""
    )

# Run Top-N selection + payments
top_hours, summary = estimate_reg_revenue_top_n(
    hourly_df=hourly,
    bess=bess,
    n_hours=int(n_hours),
    ranking=ranking,
    performance_score=float(perf_score),
)

# Enrich / display summary — keep Arrow happy by casting values to string for the UI table
extra = pd.DataFrame(
    {
        "metric": [
            "annual_cycles",
            "throughput_ratio_mwh_per_hour",
            "derived_hours_budget",
            "bess_duration_hours",
        ],
        "value": [
            int(annual_cycles),
            float(throughput_ratio),
            int(n_hours),
            float(bess.duration_hours),
        ],
    }
)
summary_full = pd.concat([summary, extra], ignore_index=True)

summary_display = summary_full.copy()
# Convert to string to avoid Arrow dtype issues across mixed types
summary_display["value"] = summary_display["value"].apply(lambda v: "" if v is None else str(v))

st.subheader("Top-N summary")
st.dataframe(summary_display, use_container_width=True, hide_index=True)

with st.expander("Selected hours", expanded=False):
    st.dataframe(top_hours, use_container_width=True, hide_index=True)

# Downloads
@st.cache_data(show_spinner=False)
def _csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

st.download_button(
    "Download Top-N hours (CSV)",
    data=_csv_bytes(top_hours),
    file_name=f"pjm_topN_hours_{win_start:%Y%m}_{win_end:%Y%m}.csv",
    mime="text/csv",
)
st.download_button(
    "Download summary (CSV)",
    data=_csv_bytes(summary_full),
    file_name=f"pjm_topN_summary_{win_start:%Y%m}_{win_end:%Y%m}.csv",
    mime="text/csv",
)

xls = _to_excel_bytes({"TopN_Hours": top_hours, "Summary": summary_full})
if xls:
    st.download_button(
        "Download Top-N workbook (Excel)",
        data=xls,
        file_name=f"pjm_topN_{win_start:%Y%m}_{win_end:%Y%m}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

with st.expander("Cache location and files"):
    base = cache_root(project_root())
    prices_files = sorted((base / "prices").glob("*.parquet"))
    market_files = sorted((base / "market").glob("*.parquet"))
    combined_files = sorted((base / "combined").glob("*.parquet"))
    st.write(
        {
            "project_root()": str(project_root()),
            "cache_root": str(base),
            "prices_files": [p.name for p in prices_files],
            "market_files": [p.name for p in market_files],
            "combined_files": [p.name for p in combined_files],
        }
    )
