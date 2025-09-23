from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from dre.config import project_root
from dre.io import list_projects, load_site_bundle, projects_root

st.set_page_config(page_title="Home and projects", layout="wide")

st.title("Home and projects")
st.write("Browse saved projects, activate one, or create a new project.")

root = project_root()
proj_dir = projects_root(root)

# Controls
top_left, top_right = st.columns([3, 1])
with top_left:
    st.page_link("pages/02_Project_inputs_wizard.py", label="Create or edit a project", icon="➕")
with top_right:
    if st.button("Rescan projects", use_container_width=True):
        st.rerun()

# Discover projects
items = list_projects(proj_dir)

if not items:
    st.info("No projects found yet. Save a project from the Project Inputs wizard.")
    st.stop()

# Optional filter
with st.expander("Filter", expanded=False):
    q = st.text_input("Search by project name or folder")
    if q:
        q_lower = q.lower()
        items = [
            it
            for it in items
            if q_lower in it["project_name"].lower() or q_lower in it["folder"].lower()
        ]

# Table view
df = pd.DataFrame(items)
show_cols = [
    "project_name",
    "folder",
    "customer_type",
    "per_meter",
    "meters",
    "interval_min",
    "start",
    "end",
    "created_at",
]
st.dataframe(df[show_cols], use_container_width=True, hide_index=True)

# Selection and activation
names = [f'{it["project_name"]}  [{it["folder"]}]' for it in items]
default_index = 0

select_col, action_col = st.columns([3, 1])
with select_col:
    choice = st.selectbox("Select a project to activate", names, index=default_index)
with action_col:
    activate = st.button("Activate project", type="primary", use_container_width=True)

if activate and choice:
    idx = names.index(choice)
    json_path = items[idx]["json_path"]
    bundle = load_site_bundle(Path(json_path))

    # Store in session for all other pages
    st.session_state["site_bundle_path"] = json_path
    st.session_state["site_bundle"] = bundle.model_dump()

    st.success(f"Activated project: {bundle.identity.name}")
    # Helpful quick links after activation
    st.page_link("pages/03_Services_and_programs.py", label="Go to Services and programs", icon="➡️")
    st.page_link("pages/04_Merchant_overlay.py", label="Go to Merchant overlay", icon="➡️")
    st.page_link("pages/06_Underwriting.py", label="Go to Underwriting", icon="➡️")

# Current active project banner
if "site_bundle" in st.session_state:
    active = st.session_state["site_bundle"]
    st.markdown("---")
    st.subheader("Active project")

    proj_name = active["identity"]["name"]
    proj_folder = Path(st.session_state["site_bundle_path"]).parent.name
    interval_min = active["load"]["interval_minutes"]
    cov_start = active["load"]["start"]
    cov_end = active["load"]["end"]
    per_meter = active["load"]["per_meter"]

    st.markdown(f"**{proj_name}**  [{proj_folder}]")

    caption_text = (
        f"Interval {interval_min} minutes  "
        f"Coverage {cov_start} to {cov_end}  "
        f"Per meter {per_meter}"
    )
    st.caption(caption_text)
