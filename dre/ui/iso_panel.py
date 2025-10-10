from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import streamlit as st

from dre.ops.iso_locator import infer_iso_from_address, is_pudl_based


def render_iso_panel(
    address: str,
    *,
    mapping_year: int = date.today().year,
    openei_api_key: str | None = None,
    title: str = "ISO / RTO identification (PUDL required)",
) -> tuple[Any, pd.DataFrame | None]:
    """
    Renders a consistent ISO/BA/Utility block (PUDL required).
    Returns (iso_info, trace_df).
    """
    st.subheader(title)

    try:
        iso_info, trace = infer_iso_from_address(
            address,
            year=int(mapping_year),
            openei_api_key=openei_api_key,
            require_pudl=True,   # always require PUDL mapping
            want_trace=True,
        )
    except RuntimeError as e:
        st.error(f"ISO inference failed: {e}")
        st.stop()

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    with c1:
        st.metric("Utility", iso_info.utility_name or "Unknown")
    with c2:
        st.metric("Balancing Authority", iso_info.balancing_authority_name or "Unknown")
    with c3:
        st.metric("ISO / RTO", iso_info.iso_rto or "Unknown")
    with c4:
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
                    "utility_name_eia",
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
        else:
            st.info("No PUDL trace rows available (unexpected for required PUDL).")

    return iso_info, trace
