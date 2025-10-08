from __future__ import annotations

import io
import json
import os
import re
import sys
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from dre.clients.pjm import PJMClient
from dre.config import project_root
from dre.estimators.merchant import (
    estimate_plc_nspl_savings,
    infer_kw_rates_from_monthly_billing,
)
from dre.markets.pjm.estimate_frequency import (
    BESSParams,
    estimate_reg_revenue_top_n,
)
from dre.markets.pjm.io import load_reg_prices_local

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# Load .env so env keys are visible to this Streamlit process
try:
    load_dotenv(project_root() / ".env", override=False)
except Exception:
    pass


# ---------- helpers ----------
def _to_dict(x: Any) -> dict[str, Any]:
    if x is None:
        return {}
    if hasattr(x, "model_dump"):
        try:
            return x.model_dump()
        except Exception:
            pass
    if hasattr(x, "dict"):
        try:
            return x.dict()
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


# ---------- page ----------
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

c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    n_hours = st.number_input("Total hours to commit (Top-N over the period)", min_value=1, value=500, step=10)
with c2:
    start_year = st.number_input(
        "Historical start year", min_value=2018, max_value=date.today().year, value=max(2023, date.today().year - 2)
    )
with c3:
    end_year = st.number_input(
        "Historical end year", min_value=start_year, max_value=date.today().year, value=date.today().year
    )

rank_choice = st.selectbox(
    "Ranking metric",
    options=[
        "Full hourly payment (RMCCP + RMPCP × Mileage)",
        "RMCCP (capability price)",
        "RMPCP (performance price)",
    ],
    index=0,
    help="Controls which hourly score is used to pick Top-N hours; consecutive hours are still limited by BESS duration.",
)
rank_map = {
    "Full hourly payment (RMCCP + RMPCP × Mileage)": "full",
    "RMCCP (capability price)": "rmccp",
    "RMPCP (performance price)": "rmpcp",
}
ranking = rank_map[rank_choice]

perf_score = st.number_input(
    "Assumed performance score (0–1)", min_value=0.0, max_value=1.0, value=0.90, step=0.05,
    help="Used in Capability and Performance credits.",
)

root = project_root()
price_df = load_reg_prices_local(root, start_year, end_year)

use_api = st.toggle("Use PJM API (Data Miner) if local cache missing", value=False)
env_key_detected = bool(
    os.getenv("PJM_API_PRIMARY_KEY") or os.getenv("PJM_API_KEY") or os.getenv("PJM_PRIMARY_KEY") or os.getenv("PJM_KEY")
)

if use_api:
    if env_key_detected:
        st.caption("Environment key detected: using PJM_API_PRIMARY_KEY from .env")
        api_primary = api_secondary = None
    else:
        api_primary = st.text_input("PJM API Primary Key", type="password")
        api_secondary = st.text_input("PJM API Secondary Key (optional)", type="password")
else:
    api_primary = api_secondary = None

if price_df is None or price_df.empty:
    st.info(
        "No cached PJM regulation price data found at `data/market/pjm/reg_prices.parquet`. "
        "With PJM API enabled, we will fetch hourly RMCCP/RMPCP and mileage ratio.",
    )
    if use_api:
        try:
            client = PJMClient(primary_key=api_primary or None, secondary_key=api_secondary or None)
            start_dt, end_dt = datetime(start_year, 1, 1), datetime(end_year, 12, 31)
            bess = BESSParams(nameplate_mw=nameplate_mw, duration_hours=float(bess_duration))
            top_hours, summary = estimate_reg_revenue_top_n(
                client,
                start_dt,
                end_dt,
                bess=bess,
                n_hours=int(n_hours),
                ranking=ranking,  # <-- pass user choice
                performance_score=float(perf_score),
            )

            # Arrow-safe display of summary (stringified values)
            summary_display = summary.copy()
            summary_display["value"] = summary_display["value"].apply(lambda x: "" if x is None else str(x))
            st.subheader("Top-N summary (API results, PJM_RTO)")
            st.dataframe(summary_display, use_container_width=True, hide_index=True)

            with st.expander("Selected hours (API)", expanded=False):
                st.dataframe(top_hours, use_container_width=True, hide_index=True)

            # Downloads (CSV + Excel)
            @st.cache_data(show_spinner=False)
            def _csv_bytes(df: pd.DataFrame) -> bytes:
                return df.to_csv(index=False).encode("utf-8")

            st.download_button(
                "Download Top-N hours (CSV)",
                data=_csv_bytes(top_hours),
                file_name=f"pjm_topN_hours_{start_year}-{end_year}.csv",
                mime="text/csv",
            )
            st.download_button(
                "Download summary (CSV)",
                data=_csv_bytes(summary),
                file_name=f"pjm_topN_summary_{start_year}-{end_year}.csv",
                mime="text/csv",
            )

            xls = _to_excel_bytes({"TopN_Hours": top_hours, "Summary": summary})
            if xls:
                st.download_button(
                    "Download Top-N workbook (Excel)",
                    data=xls,
                    file_name=f"pjm_topN_{start_year}-{end_year}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            # Raw API responses (both datasets)
            with st.expander("Raw API responses", expanded=False):
                raw_prices = client.last_response_json("reg_zone_prelim_bill")
                raw_market = client.last_response_json("reg_market_results")
                st.markdown("**reg_zone_prelim_bill**")
                st.json(raw_prices if isinstance(raw_prices, dict) else {"items": raw_prices} if raw_prices is not None else {"status": "no capture"})
                st.download_button(
                    "Download reg_zone_prelim_bill JSON",
                    data=json.dumps(raw_prices).encode("utf-8") if raw_prices is not None else b"{}",
                    file_name=f"pjm_raw_reg_zone_prelim_bill_{start_year}-{end_year}.json",
                    mime="application/json",
                )
                st.markdown("---\n**reg_market_results**")
                st.json(raw_market if isinstance(raw_market, dict) else {"items": raw_market} if raw_market is not None else {"status": "no capture"})
                st.download_button(
                    "Download reg_market_results JSON",
                    data=json.dumps(raw_market).encode("utf-8") if raw_market is not None else b"{}",
                    file_name=f"pjm_raw_reg_market_results_{start_year}-{end_year}.json",
                    mime="application/json",
                )

            st.stop()
        except Exception as e:
            st.error(f"PJM API fetch failed: {e}")
            st.stop()
    else:
        st.warning("Enable the toggle above and ensure keys, or add a local Parquet cache.")
        st.stop()

# Cached averages path (non-API path)
price_df = price_df.copy()
price_df["ts"] = pd.to_datetime(price_df["ts"])
price_df = price_df[(price_df["ts"].dt.year >= start_year) & (price_df["ts"].dt.year <= end_year)]
if price_df.empty:
    st.warning("Cached file has no rows for the selected years; enable API or change years.")
    st.stop()

avg_rmccp = float(price_df["rmccp"].mean())
avg_rmpcp = float(price_df["rmpcp"].mean())
avg_mileage_ratio = float(price_df["mileage_ratio"].mean())

st.subheader("Cached PJM price stats (selected period)")
st.write(
    {
        "rows": len(price_df),
        "avg_rmccp ($/MW-h)": round(avg_rmccp, 3),
        "avg_rmpcp ($/MW-h)": round(avg_rmpcp, 3),
        "avg_mileage_ratio": round(avg_mileage_ratio, 3),
    }
)

hours_per_year = st.number_input(
    "Hours per year participating (screening with cached averages)", min_value=0, value=3000, step=100
)
est_cap = nameplate_mw * hours_per_year * avg_rmccp * float(perf_score)
est_perf = nameplate_mw * hours_per_year * avg_rmpcp * avg_mileage_ratio * float(perf_score)
st.subheader("Estimated regulation revenue (screening w/ cached averages, PJM_RTO)")
st.write(
    {
        "BESS nameplate (MW)": round(nameplate_mw, 3),
        "BESS duration (h)": round(float(bess_duration), 3),
        "Capability credit ($/yr)": round(est_cap, 2),
        "Performance credit ($/yr)": round(est_perf, 2),
        "Total ($/yr)": round(est_cap + est_perf, 2),
    }
)
