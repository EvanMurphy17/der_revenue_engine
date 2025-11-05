from __future__ import annotations

from pathlib import Path

import streamlit as st

st.set_page_config(page_title="DER Development Platform — MVP", layout="wide")

st.title("DER Development Platform — MVP")
st.caption(
    "Home hub for your MVP modules. Use the shortcuts below to open each page. "
    "Links only appear when the target file exists in the `pages/` folder."
)

def link_if_exists(path: str, label: str, icon: str | None = None) -> None:
    """Create a Streamlit page_link only if the page file exists; otherwise show a placeholder."""
    if Path(path).exists():
        st.page_link(path, label=label, icon=icon)
    else:
        st.caption(f"○ {label} — pending (missing `{path}`)")

st.markdown("---")
st.subheader("Navigation")

col1, col2, col3 = st.columns(3)

with col1:
    link_if_exists("pages/01_Home_and_projects.py", "Home & Projects", "🏠")
    link_if_exists("pages/02_Project_Inputs.py", "Project Inputs wizard", "🧰")
    link_if_exists("pages/03_Services_and_programs.py", "Services & Programs (DSIRE + DR)", "🎯")

with col2:
    # Existing PJM Regulation estimator
    link_if_exists("pages/04_Merchant_overlay.py", "Merchant overlay — Regulation (PJM)", "⚡")
    # NEW catalog page (replaces the old risk link you had here)
    link_if_exists("pages/05_Merchant_programs.py", "Merchant programs — Catalog", "📚")

with col3:
    # Future pages (safe placeholders; won’t error if not present)
    link_if_exists("pages/06_Risk_and_haircuts.py", "Risk & Haircuts", "✂️")
    link_if_exists("pages/07_Underwriting.py", "Underwriting", "📈")
    link_if_exists("pages/08_Reporting_and_audit.py", "Reporting & Audit", "🧾")
    link_if_exists("pages/09_Downloads.py", "Downloads", "⬇️")

st.markdown("---")
st.write(
    "Tip: If you just added a new page under `pages/`, use the **Rerun** button "
    "(or press `R`) to refresh this list."
)
