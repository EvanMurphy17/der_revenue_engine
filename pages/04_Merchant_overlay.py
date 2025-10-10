from __future__ import annotations

import io
import os
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
from dre.markets.catalog import programs_for_iso
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
from dre.ops.iso_locator import infer_iso_from_address, is_pudl_based

# --- repo imports ---
_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# ---------- env ----------
try:
    load_dotenv(project_root() / ".env", override=False)
except Exception:
    pass


# ---------- small helpers ----------
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


# ---------- page ----------
st.set_page_config(page_title="Merchant Overlay", layout="wide")
st.title("Merchant Overlay")
st.caption("Identify ISO/RTO via PUDL and estimate wholesale market revenues (PJM Regulation implemented).")

# ---- Active project guard ----
if "site_bundle" not in st.session_state:
    st.warning("No active project. Open **Home and projects** and activate a project.")
    st.page_link("pages/01_Home_and_projects.py", label="Go to Home and projects", icon="🏠")
    st.stop()

bundle = st.session_state["site_bundle"]
identity = _to_dict(bundle.get("identity"))
bess_spec = bundle.get("bess")
pv_spec = _to_dict(bundle.get("pv"))

proj_name = identity.get("name", "Untitled project")
addr = (identity.get("site_address") or "").strip()

total_bess_kw, total_bess_kwh, bess_duration = _derive_bess(bess_spec)
nameplate_mw = total_bess_kw / 1000.0 if total_bess_kw > 0 else 0.0

with st.expander("Active project summary (derived)", expanded=True):
    st.write(
        {
            "Project": proj_name,
            "Address": addr,
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


# ======================================================================
# 1) ISO / RTO (PUDL REQUIRED)
# ======================================================================
st.header("1) ISO / RTO identification (PUDL required)")

# Try OpenEI for utility, then PUDL parquets for BA/ISO mapping.
openei_key = (
    os.getenv("NREL_OPENEI_API_KEY")
    or os.getenv("OPEN_EI_API_KEY")
    or os.getenv("OPENEI_API_KEY")
    or None
)
year_for_mapping = st.number_input(
    "Mapping year (for EIA-861 association)", min_value=2015, value=2024, step=1
)

try:
    iso_info, trace = infer_iso_from_address(
        addr,
        year=int(year_for_mapping),
        openei_api_key=openei_key,
        require_pudl=True,          # <<< always require PUDL mapping
        want_trace=True,
    )
except RuntimeError as e:
    st.error(f"ISO inference failed: {e}")
    st.info(
        "Make sure the PUDL parquets exist under `data/external/pudl/`:\n"
        "- core_eia861__yearly_utility_data_misc.parquet\n"
        "- core_eia861__assn_balancing_authority.parquet\n"
        "- core_eia861__yearly_utility_data_rto.parquet\n\n"
        "Use: `python -m dre.ops.pudl_fetch download` to fetch them."
    )
    st.stop()

# Tidy, compact metrics row
mc1, mc2, mc3, mc4 = st.columns([1, 1, 1, 1])
with mc1:
    st.metric("Utility", iso_info.utility_name or "Unknown")
with mc2:
    st.metric("Balancing Authority", iso_info.balancing_authority_name or "Unknown")
with mc3:
    st.metric("ISO / RTO", iso_info.iso_rto or "Unknown")
with mc4:
    st.metric("Method", iso_info.method)

if not is_pudl_based(iso_info):
    st.error("Expected PUDL-based mapping, but a fallback was used. Please check local PUDL parquets.")
    st.stop()

with st.expander("QA Trace (PUDL rows used)", expanded=False):
    if trace is not None and len(trace) > 0:
        show_cols = [
            c
            for c in [
                "utility_id_eia",
                "balancing_authority_id_eia",
                "balancing_authority_name_eia",
                "rto",
                "rto_iso",
                "rto_iso_code",
                "rto_name",
                "nerc_region",
                "state",
                "report_date",
            ]
            if c in trace.columns
        ]
        st.dataframe(trace[show_cols] if show_cols else trace, use_container_width=True, hide_index=True)

        @st.cache_data(show_spinner=False)
        def _csv_bytes(df: pd.DataFrame) -> bytes:
            return df.to_csv(index=False).encode("utf-8")

        st.download_button(
            "Download PUDL trace (CSV)",
            data=_csv_bytes(trace[show_cols] if show_cols else trace),
            file_name="pudl_ba_trace.csv",
            mime="text/csv",
        )
    else:
        st.info("No PUDL trace rows available (unexpected for required PUDL).")


# ======================================================================
# 2) Programs available in ISO (catalog view)
# ======================================================================
iso = iso_info.iso_rto or "PJM"
st.header(f"2) Programs in {iso}")

programs = programs_for_iso(iso)
if not programs:
    st.info("No programs cataloged yet for this ISO. (Edit `dre/markets/catalog.py` to extend.)")
else:
    st.dataframe(
        pd.DataFrame(
            [{"program_id": p.program_id, "name": p.name, "implemented": p.implemented, "description": p.description} for p in programs]
        ),
        use_container_width=True,
        hide_index=True,
    )


# ======================================================================
# 3) PJM Regulation estimator (Top-N) – only shown if ISO is PJM
# ======================================================================
if iso == "PJM":
    st.header("3) PJM Regulation – Top-N revenue estimator")

    # Asof → rolling 12 full months (exclusive end)
    asof_input = st.date_input(
        "As-of month (we’ll use the 12 full months ending the month before this)",
        value=date.today(),
    )
    asof_date = _normalize_asof(asof_input)
    win_start, win_end = rolling_12_full_months(asof_date)
    st.caption(f"Window (exclusive end) = {win_start:%Y-%m-%d %H:%M} → {win_end:%Y-%m-%d %H:%M}")

    # Fetch/cache behavior
    root = project_root()
    base = cache_root(root)
    env_key_detected = bool(
        os.getenv("PJM_API_PRIMARY_KEY")
        or os.getenv("PJM_API_KEY")
        or os.getenv("PJM_PRIMARY_KEY")
        or os.getenv("PJM_KEY")
    )
    use_api = st.toggle(
        "Fetch any missing months from PJM Data Miner (cache locally)",
        value=env_key_detected,
        help="If disabled, only cached months will be used.",
    )
    if use_api and env_key_detected:
        st.caption(f"Environment key detected. Cache: `{base}`")

    client = PJMClient() if use_api else None
    hourly, report = load_or_fetch_window_report(client, root, win_start, win_end, fetch_missing=use_api)

    with st.expander("Cache / Fetch report by month", expanded=hourly.empty):
        st.dataframe(pd.DataFrame(report), use_container_width=True, hide_index=True)

    if hourly.empty:
        st.error(
            "No hourly data available for the selected window. "
            "Enable fetching (and set PJM API keys in .env) to populate the cache."
        )
        st.stop()

    # Inputs panel
    st.subheader("Inputs")
    ic1, ic2, ic3, ic4 = st.columns([1, 1, 1, 1])
    with ic1:
        annual_cycles = st.number_input("Annual regulation cycles", min_value=1, value=365, step=1)
    with ic2:
        throughput_ratio = st.number_input(
            "Throughput ratio (MWh per hour, 0–1]",
            min_value=0.01,
            max_value=1.0,
            value=0.50,
            step=0.01,
            format="%.2f",
            help="Average MWh used per operating hour (e.g., 0.50 means 0.5 MWh used per hour).",
        )
    with ic3:
        ranking_choice = st.selectbox(
            "Ranking metric (consecutive-hours constraint enforced by duration)",
            options=[
                "Full hourly payment (RMCCP + RMPCP × Mileage)",
                "RMCCP (capability price)",
                "RMPCP (performance price)",
            ],
            index=0,
        )
    with ic4:
        perf_score = st.number_input(
            "Performance score (0–1)",
            min_value=0.0,
            max_value=1.0,
            value=0.90,
            step=0.05,
            help="Used in Capability and Performance credits.",
        )

    rank_map: dict[str, Ranking] = {
        "Full hourly payment (RMCCP + RMPCP × Mileage)": "full",
        "RMCCP (capability price)": "rmccp",
        "RMPCP (performance price)": "rmpcp",
    }
    ranking: Ranking = cast(Ranking, rank_map[ranking_choice])

    # Build BESS params from active project
    if not bess_duration or nameplate_mw <= 0:
        st.error("Active project BESS must specify power and energy.")
        st.stop()
    bess = BESSParams(nameplate_mw=nameplate_mw, duration_hours=float(bess_duration))

    # Top-N hours budget from cycles
    window_hours = int(len(hourly))
    n_hours = compute_n_hours_from_cycles(
        bess=bess,
        annual_cycles=int(annual_cycles),
        throughput_ratio_mwh_per_hour=float(throughput_ratio),
        window_hours=window_hours,
    )

    with st.expander("Hours budget from cycles (how N is derived)", expanded=False):
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

    # Make summary Arrow-friendly for Streamlit table
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
    summary_display["value"] = summary_display["value"].apply(lambda v: "" if v is None else str(v))

    st.subheader("Results")
    rc1, rc2 = st.columns([1.2, 1])
    with rc1:
        st.markdown("**Top-N summary**")
        st.dataframe(summary_display, use_container_width=True, hide_index=True)
    with rc2:
        st.markdown("**Selected hours (count)**")
        st.metric("Hours selected", len(top_hours))

    with st.expander("Selected hours (detailed table)", expanded=False):
        st.dataframe(top_hours, use_container_width=True, hide_index=True)

    # Downloads
    @st.cache_data(show_spinner=False)
    def _csv_bytes(df: pd.DataFrame) -> bytes:
        return df.to_csv(index=False).encode("utf-8")

    st.subheader("Downloads")
    d1, d2, d3 = st.columns([1, 1, 1])
    with d1:
        st.download_button(
            "Top-N hours (CSV)",
            data=_csv_bytes(top_hours),
            file_name=f"pjm_topN_hours_{win_start:%Y%m}_{win_end:%Y%m}.csv",
            mime="text/csv",
        )
    with d2:
        st.download_button(
            "Summary (CSV)",
            data=_csv_bytes(summary_full),
            file_name=f"pjm_topN_summary_{win_start:%Y%m}_{win_end:%Y%m}.csv",
            mime="text/csv",
        )
    with d3:
        xls = _to_excel_bytes({"TopN_Hours": top_hours, "Summary": summary_full})
        if xls:
            st.download_button(
                "Workbook (Excel)",
                data=xls,
                file_name=f"pjm_topN_{win_start:%Y%m}_{win_end:%Y%m}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    with st.expander("QA: Hourly window sample"):
        st.dataframe(hourly.head(200), use_container_width=True, hide_index=True)
        st.download_button(
            "Download hourly window (CSV)",
            data=_csv_bytes(hourly),
            file_name=f"pjm_hourly_window_{win_start:%Y%m}_{win_end:%Y%m}.csv",
            mime="text/csv",
        )

else:
    st.info(f"Estimators for {iso} coming soon. (We’ll reuse this UI pattern per product.)")
