# DER Development Platform

MVP Streamlit app to help BTM DER developers value grid services and flexibility and to give lenders a transparent view of bankable and non bankable cash flows.

## App areas
Home and projects
Project Inputs wizard
Services and programs
Merchant overlay
Risk and haircuts
Underwriting
Reporting and audit
Downloads

## Quick start

conda activate der_mvp
pre-commit install
detect-secrets scan > .secrets.baseline
git add .secrets.baseline
streamlit run streamlit_app.py

## Secrets
Copy .env.example to .env and fill in any keys. .env is ignored by git.
