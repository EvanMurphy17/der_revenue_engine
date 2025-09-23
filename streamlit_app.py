import streamlit as st

st.set_page_config(page_title="DER Development Platform", layout="wide")

st.title("DER Development Platform")
st.caption("MVP scaffold. Streamlit multipage is enabled via the pages folder.")

st.subheader("Quick links")
st.page_link("pages/01_Home_and_projects.py", label="Home and projects")
st.page_link("pages/02_Project_inputs_wizard.py", label="Project Inputs wizard")
st.page_link("pages/03_Services_and_programs.py", label="Services and programs")
st.page_link("pages/04_Merchant_overlay.py", label="Merchant overlay")
st.page_link("pages/05_Risk_and_haircuts.py", label="Risk and haircuts")
st.page_link("pages/06_Underwriting.py", label="Underwriting")
st.page_link("pages/07_Reporting_and_audit.py", label="Reporting and audit")
st.page_link("pages/08_Downloads.py", label="Downloads")

st.info(
    "You can start building each module in the corresponding page file. "
    "The app can import shared logic from the dre package."
)
