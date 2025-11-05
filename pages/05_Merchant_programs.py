from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import streamlit as st

from dre.markets.catalog import ISO, list_programs
from dre.ui.iso_panel import render_iso_panel

# Make repo importable when running directly
_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

st.set_page_config(page_title="Merchant programs (catalog)", layout="wide")
st.title("Merchant Programs — Catalog")
st.caption(
    "Browse available merchant programs for the active project’s ISO/RTO. "
    "Implemented estimators will appear as actions."
)

# Guard for active project
if "site_bundle" not in st.session_state:
    st.warning("No active project. Open **Home and projects** and activate a project.")
    st.page_link("pages/01_Home_and_projects.py", label="Go to Home and projects", icon="🏠")
    st.stop()

# ISO detection (PUDL-backed)
iso_info, trace = render_iso_panel(
    address=st.session_state["site_bundle"]["identity"].get("site_address", ""),
    mapping_year=date.today().year,
    openei_api_key=None,  # picked up from .env in iso panel if set
)

# --- Robustly extract an ISO/RTO label from iso_info ---
def _first(*vals: str | None) -> str | None:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

iso_label: str | None = _first(
    getattr(iso_info, "rto_iso", None),   # preferred
    getattr(iso_info, "iso_rto", None),   # some versions
    getattr(iso_info, "rto", None),       # fallback
    getattr(iso_info, "iso", None),       # legacy
)

def _to_iso_enum(value: str | None) -> ISO | None:
    if not value:
        return None
    v = value.upper().replace("-", "").replace("_", "").replace(" ", "")
    aliases = {
        "PJM": ISO.PJM,
        "PJMINTERCONNECTION": ISO.PJM,
        "CALIFORNIAINDEPENDENTSYSTEMOPERATOR": ISO.CAISO,
        "CALIFORNIAISO": ISO.CAISO,
        "CAISO": ISO.CAISO,
        "NEWYORKISO": ISO.NYISO,
        "NYISO": ISO.NYISO,
        "ISONE": ISO.ISONE,
        "ISONEWENGLAND": ISO.ISONE,
        "MIDCONTINENTISO": ISO.MISO,
        "MISO": ISO.MISO,
        "ELECTRICRELIABILITYCOUNCILOFTEXAS": ISO.ERCOT,
        "ERCOT": ISO.ERCOT,
        "SOUTHWESTPOWERPOOL": ISO.SPP,
        "SPP": ISO.SPP,
    }
    if v in aliases:
        return aliases[v]
    for iso in ISO:
        if iso.value.upper().replace("-", "") == v:
            return iso
    return None

iso_enum = _to_iso_enum(iso_label)

st.markdown("---")

if iso_enum is None:
    st.error(
        "Could not determine ISO/RTO for this project.\n\n"
        f"Detected label: {iso_label!r}\n\n"
        "Check the address or the PUDL mapping download. "
        "Use the ISO panel on the Services & Programs page to verify details."
    )
    with st.expander("Debug — inference trace / iso_info"):
        try:
            st.write(iso_info)
            st.json(trace or {})
        except Exception:
            pass
    st.stop()

# Pull program list
programs = list_programs(iso_enum)
if not programs:
    st.info(f"No programs registered for ISO {iso_enum.value}.")
    st.stop()

st.subheader(f"Programs in {iso_enum.value}")

def _page_exists(rel_path: str) -> bool:
    return Path(rel_path).exists()

# Display as responsive cards
cols = st.columns(3)
for i, pdsc in enumerate(programs):
    with cols[i % 3]:
        status_ok = pdsc.implemented
        _status_emoji = "✅" if status_ok else "🧩"
        st.markdown(f"**{pdsc.name}**  {_status_emoji}")
        st.caption(pdsc.kind.value)
        if pdsc.description:
            st.write(pdsc.description)

        if pdsc.datasets:
            with st.expander("Data sources"):
                for d in pdsc.datasets:
                    if d.notes:
                        st.write(f"• **{d.name}** — {d.provider} — {d.notes}")
                    else:
                        st.write(f"• **{d.name}** — {d.provider}")

        # Actions by calculator id
        if status_ok and pdsc.calculator_id == "pjm.regulation":
            target = "pages/04_Merchant_overlay.py"
            if _page_exists(target):
                st.page_link(target, label="Open merchant overlay", icon="⚡")
            else:
                st.caption("⚠️ Estimator page missing (expected `pages/04_Merchant_overlay.py`).")

        elif status_ok and pdsc.calculator_id in {"pjm.energy_da", "pjm.energy_rt", "pjm.reserve_rt"}:
            # All implemented programs aggregate on the same Merchant Overlay page
            target = "pages/04_Merchant_overlay.py"
            if _page_exists(target):
                st.page_link(target, label="Open merchant overlay", icon="🧮")
            else:
                st.caption("ℹ️ Merchant overlay page not found.")
        else:
            st.button("Planned", disabled=True, key=f"btn_{pdsc.iso}_{pdsc.code}")
