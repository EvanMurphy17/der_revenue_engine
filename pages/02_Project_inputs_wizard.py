import io
from datetime import date, datetime, time, timedelta
from typing import Any

import pandas as pd
import streamlit as st

from dre.config import project_root
from dre.io import save_site_bundle
from dre.models import (
    BESSInputs,
    BESSRow,
    BillingMonth,
    LoadMeta,
    ProjectIdentity,
    PVInputs,
    PVRow,
    SiteBundle,
    TariffInputs,
)

st.set_page_config(page_title="Project Inputs", layout="wide")
st.title("Project Inputs wizard")
st.caption("Single source of truth for identity, load, tariff, PV, and BESS.")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _default_dates() -> tuple[date, date]:
    today = date.today()
    start = date(today.year, 1, 1)
    end = date(today.year, 12, 31)
    return start, end


def _as_date(x: Any) -> date:
    if isinstance(x, tuple):
        x = x[0] if x else date.today()
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    return date.today()


def _generate_timeindex(start_d: date, end_d: date, interval_minutes: int) -> pd.DatetimeIndex:
    start_dt = datetime.combine(start_d, time(0, 0))
    end_dt_exclusive = datetime.combine(end_d + timedelta(days=1), time(0, 0))
    return pd.date_range(start=start_dt, end=end_dt_exclusive, freq=f"{interval_minutes}min", inclusive="left")


def _empty_load_df(meter_ids: list[str], idx: pd.DatetimeIndex, aggregate: bool) -> pd.DataFrame:
    if aggregate:
        df = pd.DataFrame(index=idx, data={"aggregate_kw": 0.0})
    else:
        data = {m: 0.0 for m in meter_ids}
        df = pd.DataFrame(index=idx, data=data)
    df.index.name = "interval_start"
    return df


def _ensure_positive_float(x) -> float:
    try:
        v = float(x)
        return max(v, 0.0)
    except Exception:
        return 0.0


def _month_index(start_d: date, end_d: date) -> list[str]:
    months = pd.date_range(start=start_d.replace(day=1), end=end_d.replace(day=1), freq="MS")
    return [dt.strftime("%Y-%m") for dt in months]


def _billing_template(months: list[str], per_meter: bool, meter_ids: list[str]) -> pd.DataFrame:
    if per_meter and meter_ids:
        rows = []
        for m in meter_ids:
            for mo in months:
                rows.append(
                    {
                        "meter_id": m,
                        "month": mo,
                        "energy_usd": 0.0,
                        "peak_demand_usd": 0.0,
                        "capacity_usd": 0.0,
                        "transmission_usd": 0.0,
                        "total_spend_usd": 0.0,
                    }
                )
        return pd.DataFrame(rows)
    else:
        return pd.DataFrame(
            {
                "month": months,
                "energy_usd": [0.0] * len(months),
                "peak_demand_usd": [0.0] * len(months),
                "capacity_usd": [0.0] * len(months),
                "transmission_usd": [0.0] * len(months),
                "total_spend_usd": [0.0] * len(months),
            }
        )


# -----------------------------------------------------------------------------
# Project identity
# -----------------------------------------------------------------------------
with st.expander("Project identity", expanded=True):
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        proj_name = st.text_input("Project name", placeholder="e.g., Midtown Plaza 1")
        cust_type = st.selectbox(
            "Customer type",
            ["Commercial", "Industrial", "Municipal", "University", "School", "Hospital", "Residential"],
            index=0,
        )
    with col2:
        addr_line = st.text_input("Site address", placeholder="Street, City, State ZIP")
    with col3:
        notes = st.text_area("Internal notes (optional)", placeholder="Short notes for your team")

# -----------------------------------------------------------------------------
# Load inputs
# -----------------------------------------------------------------------------
with st.expander("Load", expanded=True):
    load_mode = st.radio("Load entry mode", ["Aggregate", "Per meter"], horizontal=True)
    per_meter = load_mode == "Per meter"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        interval_min = st.selectbox("Interval minutes", [15, 30, 60], index=2)
    with c2:
        start_d, end_d = _default_dates()
        start_input = st.date_input("Data start date", value=start_d)
        start_date = _as_date(start_input)
    with c3:
        end_input = st.date_input("Data end date", value=end_d, min_value=start_date)
        end_date = _as_date(end_input)
    with c4:
        est_increase_type = st.selectbox("Estimated load increase type", ["None", "kW", "Percent"], index=0)

    if per_meter:
        c5, c6 = st.columns([2, 3])
        with c5:
            meter_count = st.number_input("Number of meters", min_value=1, value=2, step=1)
            base_name = st.text_input("Base meter name", value="MTR")
            default_ids = [f"{base_name}{i+1}" for i in range(int(meter_count))]
            ids_text = st.text_area(
                "Meter IDs, one per line",
                value="\n".join(default_ids),
                height=100,
                help="Edit or paste your own list. Each line becomes a column in the table.",
            )
            meter_ids = [x.strip() for x in ids_text.splitlines() if x.strip()]
        with c6:
            st.info("Paste kW values directly into the table below. Rows are time intervals. Columns are meters.")
    else:
        meter_ids = []

    idx = _generate_timeindex(start_date, end_date, interval_min)
    starter_df = _empty_load_df(meter_ids, idx, aggregate=not per_meter)

    edited_df = st.data_editor(
        starter_df,
        num_rows="dynamic",
        use_container_width=True,
        key="load_editor",
        hide_index=False,
    )

    est_increase_kw = None
    est_increase_pct = None
    if est_increase_type == "kW":
        est_increase_kw = st.number_input("Estimated load increase kW", min_value=0.0, value=0.0, step=1.0)
    elif est_increase_type == "Percent":
        est_increase_pct = st.number_input("Estimated load increase percent", min_value=0.0, value=0.0, step=0.5)

# -----------------------------------------------------------------------------
# Tariff inputs
# -----------------------------------------------------------------------------
with st.expander("Tariff", expanded=False):
    baseline_tariff = st.text_input("Current baseline tariff name", placeholder="Will be inferred from address later")

    st.markdown("**Optional monthly billing summary**")
    months = _month_index(start_date, end_date)
    billing_df = _billing_template(months, per_meter=per_meter, meter_ids=meter_ids)

    column_cfg = {
        "month": st.column_config.TextColumn(help="YYYY-MM"),
        "energy_usd": st.column_config.NumberColumn(help="Energy charge dollars for the month"),
        "peak_demand_usd": st.column_config.NumberColumn(help="Demand charge dollars for the month"),
        "capacity_usd": st.column_config.NumberColumn(help="Billed capacity charge dollars"),
        "transmission_usd": st.column_config.NumberColumn(help="Billed transmission charge dollars"),
        "total_spend_usd": st.column_config.NumberColumn(help="Total utility spend for the month"),
    }
    if per_meter and meter_ids:
        column_cfg = {
            "meter_id": st.column_config.SelectboxColumn(options=meter_ids, help="Billing meter ID"),
            **column_cfg,
        }

    billing_df = st.data_editor(
        billing_df,
        num_rows="dynamic",
        use_container_width=True,
        key="billing_editor",
        column_config=column_cfg,
    )
    st.caption("Leave any fields at zero if unknown. You can also attach raw bills on a later page.")

# -----------------------------------------------------------------------------
# PV inputs
# -----------------------------------------------------------------------------
with st.expander("PV", expanded=False):
    pv_mode_label = "Per meter" if per_meter else "Aggregate"
    st.radio("PV allocation follows Load selection", [pv_mode_label], index=0, horizontal=True, disabled=True)

    if per_meter and meter_ids:
        st.write("Enter DC and AC nameplate by meter")
        pv_df = pd.DataFrame({"meter_id": meter_ids, "dc_kw": 0.0, "ac_kw": 0.0})
    else:
        st.write("Enter a single aggregate PV row")
        pv_df = pd.DataFrame({"meter_id": ["AGG"], "dc_kw": [0.0], "ac_kw": [0.0]})

    pv_df = st.data_editor(pv_df, num_rows="dynamic", use_container_width=True, key="pv_editor")
    pv_rows: list[PVRow] = [
        PVRow(meter_id=str(r["meter_id"]), dc_kw=_ensure_positive_float(r["dc_kw"]), ac_kw=_ensure_positive_float(r["ac_kw"]))
        for _, r in pv_df.iterrows()
    ]

# -----------------------------------------------------------------------------
# BESS inputs
# -----------------------------------------------------------------------------
with st.expander("BESS", expanded=False):
    bess_mode_label = "Per meter" if per_meter else "Aggregate"
    st.radio("BESS allocation follows Load selection", [bess_mode_label], index=0, horizontal=True, disabled=True)

    if per_meter and meter_ids:
        st.write("Enter power and energy by meter")
        bess_df = pd.DataFrame({"meter_id": meter_ids, "power_kw": 0.0, "energy_kwh": 0.0})
    else:
        st.write("Enter a single aggregate BESS row")
        bess_df = pd.DataFrame({"meter_id": ["AGG"], "power_kw": [0.0], "energy_kwh": [0.0]})

    bess_df = st.data_editor(bess_df, num_rows="dynamic", use_container_width=True, key="bess_editor")
    bess_rows: list[BESSRow] = [
        BESSRow(meter_id=str(r["meter_id"]), power_kw=_ensure_positive_float(r["power_kw"]), energy_kwh=_ensure_positive_float(r["energy_kwh"]))
        for _, r in bess_df.iterrows()
    ]

# -----------------------------------------------------------------------------
# Save bundle
# -----------------------------------------------------------------------------
st.markdown("---")
left, right = st.columns([3, 2])

with left:
    st.subheader("Save and export")
    proj_ok = bool(proj_name.strip())
    if not proj_ok:
        st.warning("Enter a project name to enable Save Bundle.")
    save_clicked = st.button("Save bundle to projects folder", disabled=not proj_ok, type="primary")

with right:
    st.subheader("Download")
    csv_buf = io.StringIO()
    edited_df.to_csv(csv_buf)
    st.download_button("Download load CSV", csv_buf.getvalue(), file_name="load.csv")

if save_clicked:
    identity = ProjectIdentity(
        name=proj_name.strip(),
        customer_type=cust_type,
        site_address=addr_line.strip(),
        notes=notes.strip() if notes else None,
    )

    load_meta = LoadMeta(
        per_meter=per_meter,
        meter_ids=meter_ids if per_meter else [],
        interval_minutes=interval_min,
        start=str(edited_df.index.min()) if not edited_df.empty else str(datetime.combine(start_date, time(0, 0))),
        end=str(edited_df.index.max() + timedelta(minutes=interval_min)) if not edited_df.empty else str(datetime.combine(end_date + timedelta(days=1), time(0, 0))),
        est_increase_kw=est_increase_kw,
        est_increase_pct=est_increase_pct,
    )

    # Build monthly billing rows
    monthly_rows: list[BillingMonth] = []
    if per_meter and meter_ids and "meter_id" in billing_df.columns:
        for _, r in billing_df.iterrows():
            monthly_rows.append(
                BillingMonth(
                    month=str(r["month"]),
                    meter_id=str(r["meter_id"]),
                    energy_usd=_ensure_positive_float(r.get("energy_usd", 0.0)),
                    peak_demand_usd=_ensure_positive_float(r.get("peak_demand_usd", 0.0)),
                    capacity_usd=_ensure_positive_float(r.get("capacity_usd", 0.0)),
                    transmission_usd=_ensure_positive_float(r.get("transmission_usd", 0.0)),
                    total_spend_usd=_ensure_positive_float(r.get("total_spend_usd", 0.0)),
                )
            )
        monthly_mode = "per_meter"
    else:
        for _, r in billing_df.iterrows():
            monthly_rows.append(
                BillingMonth(
                    month=str(r["month"]),
                    meter_id=None,
                    energy_usd=_ensure_positive_float(r.get("energy_usd", 0.0)),
                    peak_demand_usd=_ensure_positive_float(r.get("peak_demand_usd", 0.0)),
                    capacity_usd=_ensure_positive_float(r.get("capacity_usd", 0.0)),
                    transmission_usd=_ensure_positive_float(r.get("transmission_usd", 0.0)),
                    total_spend_usd=_ensure_positive_float(r.get("total_spend_usd", 0.0)),
                )
            )
        monthly_mode = "aggregate"

    tariff_inputs = TariffInputs(
        baseline_tariff_name=baseline_tariff.strip() if baseline_tariff else None,
        monthly_mode=monthly_mode,  # aggregate or per_meter
        monthly_billing=monthly_rows,
    )

    pv_inputs = PVInputs(mode="per_meter" if per_meter else "aggregate", rows=pv_rows)
    bess_inputs = BESSInputs(mode="per_meter" if per_meter else "aggregate", rows=bess_rows)

    bundle = SiteBundle(
        identity=identity,
        load=load_meta,
        tariff=tariff_inputs,
        pv=pv_inputs,
        bess=bess_inputs,
    )

    proj_dir = project_root() / "projects" / identity.safe_slug()
    proj_dir.mkdir(parents=True, exist_ok=True)

    json_path, csv_path = save_site_bundle(bundle=bundle, load_df=edited_df, project_dir=proj_dir)

    st.success(f"Saved bundle at {json_path}")
    st.caption(f"Load CSV at {csv_path}")

    st.session_state["site_bundle_path"] = str(json_path)
    st.session_state["site_bundle"] = bundle.model_dump()

    with open(json_path, "rb") as jf:
        st.download_button("Download site bundle JSON", jf.read(), file_name=json_path.name, mime="application/json")
