from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from dre.catalog.dsire_catalog import (
    catalog_exists,
    get_parameters_for_program,
    query_programs_by_state,
)
from dre.config import project_root
from dre.markets.demand_response import (
    _normalize_columns,
    ensure_dr_available,
    filter_dr_for_entity,
    latest_dr_estimate,
    load_dr_table,
    sector_from_project_customer_type,
    summarize_dr,
)

# ISO panel + DR helpers (logic lives in module, not in UI)
from dre.ui.iso_panel import render_iso_panel

# -----------------------------------------------------------------------------
# Make the repo importable when running this page directly in Streamlit
# -----------------------------------------------------------------------------
_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# -----------------------------------------------------------------------------
# Page setup
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Services and programs", layout="wide")
st.title("Services & Programs")
st.caption(
    "Query locally-stored DSIRE programs for the active project’s state. "
    "Fast, offline, and modeling-ready."
)

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
_US_STATES: dict[str, str] = {
    "AL": "Alabama","AK": "Alaska","AZ": "Arizona","AR": "Arkansas","CA": "California","CO": "Colorado",
    "CT": "Connecticut","DE": "Delaware","DC": "District of Columbia","FL": "Florida","GA": "Georgia",
    "HI": "Hawaii","ID": "Idaho","IL": "Illinois","IN": "Indiana","IA": "Iowa","KS": "Kansas","KY": "Kentucky",
    "LA": "Louisiana","ME": "Maine","MD": "Maryland","MA": "Massachusetts","MI": "Michigan","MN": "Minnesota",
    "MS": "Mississippi","MO": "Missouri","MT": "Montana","NE": "Nebraska","NV": "Nevada","NH": "New Hampshire",
    "NJ": "New Jersey","NM": "New Mexico","NY": "New York","NC": "North Carolina","ND": "North Dakota",
    "OH": "Ohio","OK": "Oklahoma","OR": "Oregon","PA": "Pennsylvania","RI": "Rhode Island","SC": "South Carolina",
    "SD": "South Dakota","TN": "Tennessee","TX": "Texas","UT": "Utah","VT": "Vermont","VA": "Virginia",
    "WA": "Washington","WV": "West Virginia","WI": "Wisconsin","WY": "Wyoming",
}

# -----------------------------------------------------------------------------
# Active project guard + state guess
# -----------------------------------------------------------------------------
if "site_bundle" not in st.session_state:
    st.warning("No active project. Open **Home and projects** and activate a project.")
    st.page_link("pages/01_Home_and_projects.py", label="Go to Home and projects", icon="🏠")
    st.stop()

bundle = st.session_state["site_bundle"]
addr = (bundle["identity"].get("site_address") or "").strip()
customer_type = (bundle["identity"].get("customer_type") or "").strip()

def _guess_state_from_address(address: str) -> str:
    m = re.findall(r"\b([A-Z]{2})\b", address.upper())
    return m[-1] if m else "CA"

state_guess = _guess_state_from_address(addr)

# -----------------------------------------------------------------------------
# ISO / BA / Utility via PUDL (reusable panel, used across pages)
# -----------------------------------------------------------------------------
iso_info, _trace = render_iso_panel(
    address=addr,
    mapping_year=date.today().year,
    openei_api_key=None,  # from .env if present
)

util_id_eia = iso_info.utility_id_eia
ba_id_eia = iso_info.balancing_authority_id_eia
state_from_iso = iso_info.state or state_guess

st.markdown("---")

# -----------------------------------------------------------------------------
# Demand Response (EIA-861 via PUDL) — latest year & $/kW estimator
# -----------------------------------------------------------------------------
st.subheader("Demand Response (latest year) — EIA-861 via PUDL")

ensure_dr_available()
dr = load_dr_table()
if dr is None or dr.empty:
    st.error(
        "Demand Response table not available. Try:\n\n"
        "`python -m dre.ops.pudl_fetch download --table core_eia861__yearly_demand_response`"
    )
else:
    dr = _normalize_columns(dr)

    inferred_sector = sector_from_project_customer_type(customer_type)
    sector_choices = ["(All)", "Residential", "Commercial", "Industrial"]
    default_idx = sector_choices.index(inferred_sector) if inferred_sector in sector_choices else 0
    sector_pick = st.selectbox(
        "Customer class (inferred from project; override if needed)",
        options=sector_choices,
        index=default_idx,
    )
    sector_filter: str | None = None if sector_pick == "(All)" else sector_pick

    fdf = filter_dr_for_entity(
        dr,
        utility_id_eia=util_id_eia,
        ba_id_eia=ba_id_eia,
        state=state_from_iso,
        sector=sector_filter,
    )

    if fdf.empty:
        st.info("No demand response records found for this utility / BA / state selection.")
    else:
        # Latest-year summary table
        summ = summarize_dr(fdf).copy()
        # Render-friendly string conversion to avoid Arrow warnings
        summ["value"] = summ["value"].apply(
            lambda v: "" if v is None else (f"{v:,.2f}" if isinstance(v, (int, float)) else str(v))
        )
        st.dataframe(summ, use_container_width=True, hide_index=True)

        # $/kW estimator (centralized in module)
        est = latest_dr_estimate(fdf)
        m1, m2, m3 = st.columns([1, 1, 1])
        with m1:
            st.metric("Latest year expenditures (USD)", "N/A" if est.expenditures_usd is None else f"{est.expenditures_usd:,.0f}")
        with m2:
            st.metric("Actual peak reduction (MW)", "N/A" if est.actual_reduction_mw is None else f"{est.actual_reduction_mw:,.2f}")
        with m3:
            st.metric("Estimated DR $/kW (annual)", "N/A" if est.usd_per_kw_year is None else f"{est.usd_per_kw_year:,.2f}")

st.markdown("---")

# -----------------------------------------------------------------------------
# Require local catalog (SQLite)
# -----------------------------------------------------------------------------
root = project_root()
if not catalog_exists(root):
    st.warning(
        "DSIRE catalog not found at `data/catalog/dsire.db`.\n\n"
        "Build it once, then this page will be instant:\n\n"
        "1) `dre-dsire-db build-api --start 2020-01-01 --end 2025-09-23`\n"
        "   or\n"
        "2) `dre-dsire-db import-raw --path data/raw/dsire --tag local-<date>`\n\n"
        "After building, click **Rerun**."
    )
    st.stop()

# -----------------------------------------------------------------------------
# Controls
# -----------------------------------------------------------------------------
ctrl_left, ctrl_right = st.columns([1, 2])

with ctrl_left:
    state_codes = list(_US_STATES.keys())
    default_idx = state_codes.index(state_guess) if state_guess in state_codes else state_codes.index("CA")
    state = st.selectbox(
        "State (from project address)",
        options=state_codes,
        index=default_idx,
        format_func=lambda s: f"{s} — {_US_STATES[s]}",
    )
    year_start = st.number_input(
        "Updated since (year)",
        min_value=2010,
        max_value=date.today().year,
        value=max(2020, date.today().year - 5),
        step=1,
    )
    year_end = st.number_input(
        "Updated through (year)",
        min_value=year_start,
        max_value=date.today().year,
        value=date.today().year,
        step=1,
    )

with ctrl_right:
    st.caption("Filters")
    program_types_placeholder = st.empty()
    program_categories_placeholder = st.empty()
    tech_contains = st.text_input("Technology contains", placeholder="Solar, Storage, Demand Response")
    global_search = st.text_input("Global contains (name/admin/url/text)", placeholder="free text filter")
    if st.button("Rescan database"):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

# -----------------------------------------------------------------------------
# Load programs for the chosen state from the catalog
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=True, ttl=3600)
def _load_programs_by_state(repo_root: str, state_code: str) -> pd.DataFrame:
    return query_programs_by_state(Path(repo_root), state_code)

prog_df = _load_programs_by_state(str(root), state)

if prog_df.empty:
    st.info("No programs found in the catalog for this state.")
    st.stop()

# Populate dynamic filters
type_options = sorted([t for t in prog_df["type"].dropna().unique().tolist() if str(t).strip()])
cat_options = sorted([c for c in prog_df["category"].dropna().unique().tolist() if str(c).strip()])

program_types = program_types_placeholder.multiselect("Program type", type_options)
program_categories = program_categories_placeholder.multiselect("Program category", cat_options)

# -----------------------------------------------------------------------------
# Filtering helpers
# -----------------------------------------------------------------------------
def _contains(s: pd.Series, needle: str) -> pd.Series:
    return s.fillna("").str.contains(needle, case=False, regex=False)

def _year_window_filter(df: pd.DataFrame, col: str, y0: int, y1: int) -> pd.DataFrame:
    if col not in df.columns:
        return df
    years = df[col].fillna("").astype(str).str.extract(r"(\d{4})", expand=False)
    with pd.option_context("mode.chained_assignment", None):
        years = pd.to_numeric(years, errors="coerce")
    return df[(years.fillna(0).astype(int) >= int(y0)) & (years.fillna(9999).astype(int) <= int(y1))]

# -----------------------------------------------------------------------------
# Apply filters
# -----------------------------------------------------------------------------
filtered = prog_df.copy()
filtered = _year_window_filter(filtered, "last_update", year_start, year_end)

if program_types:
    filtered = filtered[filtered["type"].fillna("").isin(program_types)]

if program_categories:
    filtered = filtered[filtered["category"].fillna("").isin(program_categories)]

if tech_contains.strip():
    filtered = filtered[_contains(filtered["technologies"], tech_contains.strip())]

if global_search.strip():
    needle = global_search.strip()
    mask = (
        _contains(filtered["program_name"], needle)
        | _contains(filtered["administrator"], needle)
        | _contains(filtered["website_url"], needle)
        | _contains(filtered["technologies"], needle)
        | _contains(filtered["category"], needle)
        | _contains(filtered["utilities"], needle)
        | _contains(filtered["sectors"], needle)
        | _contains(filtered["status"], needle)
    )
    filtered = filtered[mask]

# -----------------------------------------------------------------------------
# Table + selection
# -----------------------------------------------------------------------------
left, right = st.columns([2, 1])

with left:
    st.subheader("Programs")
    if filtered.empty:
        st.warning("No programs matched your filters.")
        selected_pid = None
    else:
        show_cols = [
            "state", "program_name", "type", "category", "administrator",
            "technologies", "sectors", "utilities", "last_update", "website_url"
        ]
        show_cols = [c for c in show_cols if c in filtered.columns]
        table = filtered[show_cols].reset_index(drop=True)
        st.dataframe(table, use_container_width=True, hide_index=True)

        # Build a stable selection list by program_id
        options: list[tuple[str, str]] = []
        for _, row in filtered.iterrows():
            pid = str(row["program_id"])
            label = f'{row.get("program_name","")} — {row.get("administrator","")}'.strip(" —")
            label = f"{label}  [{pid}]"
            options.append((pid, label))

        labels = [lbl for _, lbl in options]
        default_idx = 0 if labels else None
        selected_label = st.selectbox("Inspect a program", labels, index=default_idx) if labels else None
        selected_pid = None
        if selected_label:
            for pid, lbl in options:
                if lbl == selected_label:
                    selected_pid = pid
                    break

with right:
    st.subheader("Downloads")
    drop_cols = ["raw_json"]
    to_csv = filtered.drop(columns=[c for c in drop_cols if c in filtered.columns], errors="ignore")
    st.download_button(
        "Download filtered Programs CSV",
        to_csv.to_csv(index=False),
        file_name=f"dsire_programs_{state}.csv",
    )
    if selected_pid:
        psub = get_parameters_for_program(root, selected_pid)
        if not psub.empty:
            st.download_button(
                "Download selected program Parameters CSV",
                psub.to_csv(index=False),
                file_name=f"dsire_parameters_{selected_pid}.csv",
            )

st.markdown("---")

# -----------------------------------------------------------------------------
# Details & raw JSON
# -----------------------------------------------------------------------------
if selected_pid:
    sel = filtered[filtered["program_id"].astype(str) == str(selected_pid)]
    if not sel.empty:
        row = sel.iloc[0]
        st.subheader("Record details")
        colA, colB = st.columns([1.2, 1])

        with colA:
            st.write(
                {
                    "program_id": row.get("program_id"),
                    "state": row.get("state"),
                    "program_name": row.get("program_name"),
                    "administrator": row.get("administrator"),
                    "type": row.get("type"),
                    "category": row.get("category"),
                    "technologies": row.get("technologies"),
                    "sectors": row.get("sectors"),
                    "utilities": row.get("utilities"),
                    "website_url": row.get("website_url"),
                    "last_update": row.get("last_update"),
                    "status": row.get("status"),
                    "source_tag": row.get("source_tag"),
                    "updated_ts": row.get("updated_ts"),
                }
            )

        with colB:
            url = (row.get("website_url") or "").strip()
            if url:
                st.link_button("Open website", url)
            psub = get_parameters_for_program(root, str(row.get("program_id")))
            if not psub.empty:
                money_like = psub[
                    psub["units"].fillna("").str.contains(r"\$|USD", na=False)
                    | psub["raw_label"].fillna("").str.contains(r"\$|USD", na=False)
                ]
                st.write("**Extracted incentive parameters (preview)**")
                showp = [c for c in ["qualifier", "amount", "units", "source", "raw_label"] if c in money_like.columns]
                st.dataframe(money_like.head(12)[showp], use_container_width=True, hide_index=True)

        with st.expander("Raw JSON"):
            raw = row.get("raw_json")
            try:
                obj = json.loads(raw) if isinstance(raw, str) else (raw or {})
            except Exception:
                obj = {"_raw": str(raw)}
            st.json(obj)

st.markdown("---")

# -----------------------------------------------------------------------------
# Operational considerations (kept in session for now)
# -----------------------------------------------------------------------------
st.subheader("Operational considerations for the active project")

ops_cols = st.columns(3)
with ops_cols[0]:
    st.checkbox("Telemetry & dispatch API available", value=True)
    st.checkbox("Baseline methodology documented", value=True)
    st.checkbox("M&V rules clear", value=True)
    st.checkbox("Performance penalties understood", value=False)
with ops_cols[1]:
    st.checkbox("Export allowed per interconnection", value=True)
    st.checkbox("Netting/settlement interval compatible", value=True)
    st.checkbox("Coincident peak rules mapped", value=False)
    st.checkbox("Standby charges evaluated", value=False)
with ops_cols[2]:
    st.checkbox("BESS warranty aligns with use", value=True)
    st.checkbox("Cycle/throughput limits respected", value=True)
    st.checkbox("DR telemetry capable (yes/no)", value=True)

st.text_area(
    "Notes",
    placeholder="Site-specific operational constraints, enrollment nuances, telemetry vendors, etc.",
)
st.caption("These notes are kept in-session for now. We’ll attach them to Underwriting and Reporting next.")
