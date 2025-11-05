from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

import pandas as pd
import streamlit as st

from dre.clients.pjm import PJMClient
from dre.markets.pjm.cache import (
    load_energy_cached,
    load_regulation_cached,
    load_reserves_cached,
)
from dre.markets.pjm.estimate_energy import EnergyArbParams, estimate_energy_arbitrage
from dre.markets.pjm.estimate_frequency import (
    BESSParams,
    estimate_reg_revenue_top_n,
)
from dre.markets.pjm.estimate_reserves import ReserveParams, estimate_reserve_revenue

st.set_page_config(page_title="Merchant overlay", layout="wide")


# ---------------- utils ----------------

def _first_of_next_month(d: datetime) -> datetime:
    return (d.replace(day=28) + timedelta(days=4)).replace(day=1)


def _normalize_asof(x: Any) -> tuple[datetime, datetime]:
    """
    Accept Streamlit's date_input return:
      - date
      - (date,)
      - (date, date)
    Return timezone-aware UTC (start, end_exclusive) on month boundaries.
    Never raises on odd shapes; falls back to trailing 12 months.
    """
    try:
        # tuple of dates
        if isinstance(x, tuple):
            if len(x) == 2 and all(isinstance(v, date) for v in x):
                s = datetime(x[0].year, x[0].month, 1, tzinfo=UTC)
                e_month = datetime(x[1].year, x[1].month, 1, tzinfo=UTC)
                return s, _first_of_next_month(e_month)
            if len(x) == 1 and isinstance(x[0], date):
                s = datetime(x[0].year, x[0].month, 1, tzinfo=UTC)
                return s, _first_of_next_month(s)
        # single date
        if isinstance(x, date):
            s = datetime(x.year, x.month, 1, tzinfo=UTC)
            return s, _first_of_next_month(s)
    except Exception:
        pass

    # fallback: trailing 12 months
    today_m1 = datetime.now(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_excl = datetime(today_m1.year, today_m1.month, 1, tzinfo=UTC)
    start = datetime(end_excl.year - 1, end_excl.month, 1, tzinfo=UTC)
    return start, end_excl


def _sum_or_zero(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _safe_metric(label: str, value: Any, fmt: str = "${:,.0f}") -> None:
    st.metric(label, fmt.format(_sum_or_zero(value)))


def _have_any_cached_reserves(market: str, services: Iterable[str], start: datetime, end_excl: datetime) -> bool:
    for svc in services:
        df = load_reserves_cached(market, svc, start, end_excl)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return True
    return False


# ---------------- UI ----------------

st.title("Merchant overlay")

colA, colB = st.columns([2, 1])
with colA:
    project = st.selectbox("Project", options=["My Test Project"], index=0)
with colB:
    asof_input = st.date_input(
        "Analysis window",
        value=(date.today().replace(day=1) - timedelta(days=365), date.today()),
        format="YYYY-MM-DD",
    )

start, end_excl = _normalize_asof(asof_input)

# For now, we pin to PJM. Remove this caption or wire up your inference later.
iso_name = "PJM"
st.caption(f"ISO inference: {iso_name}")

client = PJMClient()

st.subheader("BESS parameters")
c1, c2, c3, c4 = st.columns(4)
with c1:
    total_kw = float(st.number_input("Power kW", value=1000.0, min_value=0.0, step=100.0))
with c2:
    total_kwh = float(st.number_input("Energy kWh", value=4000.0, min_value=0.0, step=100.0))
with c3:
    rte = float(st.number_input("Round trip eff", value=0.90, min_value=0.5, max_value=1.0, step=0.01))
with c4:
    annual_cycles = int(st.number_input("Annual cycles", value=365, min_value=1, step=1))

duration_hr = 0.0 if total_kw <= 0 else total_kwh / total_kw

st.divider()

# =========================
# Regulation — cache-first
# =========================
st.markdown("### Regulation revenue")

reg_cached = load_regulation_cached(start, end_excl)
if not isinstance(reg_cached, pd.DataFrame) or reg_cached.empty:
    st.warning("No cached regulation price data available in the selected window.")
else:
    perf_score = float(st.number_input("Performance score", value=0.80, min_value=0.0, max_value=1.0, step=0.01))
    rank_choice = cast(str, st.selectbox("Ranking metric", options=["full", "rmccp", "rmpcp"], index=0))

    reg_params = BESSParams(
        nameplate_mw=total_kw / 1000.0,
        duration_hours=duration_hr,
        annual_cycles=annual_cycles,
        throughput_ratio=1.0,
        round_trip_eff=rte,
    )

    try:
        reg_res = estimate_reg_revenue_top_n(
            start=start,
            end_exclusive=end_excl,
            bess=reg_params,
            performance_score=perf_score,
            ranking=rank_choice,
            client=client,
        )
        reg_sum = reg_res.get("summary")
        reg_top = reg_res.get("topn")

        c1, c2 = st.columns([1, 2])
        with c1:
            gross = 0.0
            if isinstance(reg_sum, pd.DataFrame) and not reg_sum.empty and "gross_usd" in reg_sum.columns:
                gross = _sum_or_zero(reg_sum.iloc[0]["gross_usd"])
            _safe_metric("Gross revenue", gross)
        with c2:
            if isinstance(reg_top, pd.DataFrame) and not reg_top.empty:
                st.dataframe(reg_top, use_container_width=True, hide_index=True)
            else:
                st.info("No top hours available in cache window.")
    except Exception as e:
        st.error(f"PJM API fetch or calc failed (regulation): {e}")

st.divider()

# =========================
# Energy — cache-only page
# =========================
st.markdown("### Energy arbitrage")

da_cached = load_energy_cached("DA", start, end_excl)
rt_cached = load_energy_cached("RT", start, end_excl)

if (not isinstance(da_cached, pd.DataFrame) or da_cached.empty) and (
    not isinstance(rt_cached, pd.DataFrame) or rt_cached.empty
):
    st.warning("No cached DA or RT energy prices found in the selected window.")
else:
    market_choice = cast(str, st.selectbox("Market", options=["DA", "RT"], index=0))

    eparams = EnergyArbParams(
        market=market_choice,
        bess_power_kw=total_kw,
        bess_energy_kwh=total_kwh,
        duration_hr=duration_hr,
        round_trip_eff=rte,
    )

    try:
        eres = estimate_energy_arbitrage(start, end_excl, eparams)
        gross = _sum_or_zero(eres.get("gross_usd"))
        avg_spread = _sum_or_zero(eres.get("avg_spread"))
        raw = eres.get("raw")

        c1, c2 = st.columns([1, 2])
        with c1:
            _safe_metric("Gross revenue", gross)
            st.caption(f"Avg daily spread: ${avg_spread:,.2f}/MWh")
        with c2:
            if isinstance(raw, pd.DataFrame) and not raw.empty:
                st.dataframe(raw.tail(200), use_container_width=True, hide_index=True)
            else:
                st.info("No price rows available to display.")
    except Exception as e:
        st.error(f"PJM API fetch or calc failed (energy): {e}")

st.divider()

# =========================
# Reserves — cache-only page
# =========================
st.markdown("### Operating reserves")

svc_map_da = (
    "PJM RTO Synchronized Reserve",
    "PJM RTO Primary Reserve",
    "PJM RTO Thirty Minutes Reserve",
)
svc_map_rt = (
    "RTO Synchronized Reserve",
    "RTO Secondary Reserve",
    "RTO Non-Synchronized Reserve",
)

tab_da, tab_rt = st.tabs(["DA reserves", "RT reserves"])

with tab_da:
    if not _have_any_cached_reserves("DA", svc_map_da, start, end_excl):
        st.warning("No cached DA reserves found in the selected window.")
    else:
        picks_da = st.multiselect("DA ancillary services", options=list(svc_map_da), default=list(svc_map_da))
        offered_mw_da = float(st.number_input("Offered MW", value=1.0, min_value=0.0, step=0.1, key="da_mw"))
        # make hours an int input
        hours_da_i = int(st.number_input("Committed hours per year", value=2000, min_value=0, step=50, key="da_hrs"))

        total_gross = 0.0
        rows: list[dict[str, Any]] = []

        for svc in picks_da:
            rparams = ReserveParams(
                market="DA",
                ancillary_service=svc,   # singular, per estimator signature
                offered_mw=offered_mw_da,
                hours_per_year=hours_da_i,  # int
            )
            try:
                rres = estimate_reserve_revenue(start, end_excl, rparams)
                gross = _sum_or_zero(rres.get("gross_usd"))
                total_gross += gross

                avg_df = rres.get("per_service")
                if isinstance(avg_df, pd.DataFrame) and not avg_df.empty:
                    # expect columns: service, avg_mcp (by our estimator)
                    for _, r in avg_df.iterrows():
                        rows.append({"service": str(r.get("service")), "avg_mcp": _sum_or_zero(r.get("avg_mcp")), "gross_usd": gross})
                else:
                    rows.append({"service": svc, "avg_mcp": float("nan"), "gross_usd": gross})
            except Exception as e:
                st.error(f"PJM API fetch or calc failed (reserves DA {svc}): {e}")

        _safe_metric("Gross revenue", total_gross)
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

with tab_rt:
    if not _have_any_cached_reserves("RT", svc_map_rt, start, end_excl):
        st.warning("No cached RT reserves found in the selected window.")
    else:
        picks_rt = st.multiselect(
            "RT ancillary services",
            options=list(svc_map_rt),
            default=list(svc_map_rt),
            key="rt_picks",
        )
        offered_mw_rt = float(st.number_input("Offered MW", value=1.0, min_value=0.0, step=0.1, key="rt_mw"))
        # make hours an int input
        hours_rt_i = int(st.number_input("Committed hours per year", value=2000, min_value=0, step=50, key="rt_hrs"))

        total_gross = 0.0
        rows: list[dict[str, Any]] = []

        for svc in picks_rt:
            rparams = ReserveParams(
                market="RT",
                ancillary_service=svc,   # singular, per estimator signature
                offered_mw=offered_mw_rt,
                hours_per_year=hours_rt_i,  # int
            )
            try:
                rres = estimate_reserve_revenue(start, end_excl, rparams)
                gross = _sum_or_zero(rres.get("gross_usd"))
                total_gross += gross

                avg_df = rres.get("per_service")
                if isinstance(avg_df, pd.DataFrame) and not avg_df.empty:
                    for _, r in avg_df.iterrows():
                        rows.append({"service": str(r.get("service")), "avg_mcp": _sum_or_zero(r.get("avg_mcp")), "gross_usd": gross})
                else:
                    rows.append({"service": svc, "avg_mcp": float("nan"), "gross_usd": gross})
            except Exception as e:
                st.error(f"PJM API fetch or calc failed (reserves RT {svc}): {e}")

        _safe_metric("Gross revenue", total_gross)
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
