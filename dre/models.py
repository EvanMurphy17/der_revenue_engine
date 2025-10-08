from __future__ import annotations

import re
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class ProjectIdentity(BaseModel):
    name: str
    customer_type: str
    site_address: str
    notes: str | None = None

    def safe_slug(self) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", self.name).strip("_").lower()
        return slug or "project"


class LoadMeta(BaseModel):
    per_meter: bool
    meter_ids: list[str] = Field(default_factory=list)
    interval_minutes: int = 60
    start: str
    end: str
    est_increase_kw: float | None = None
    est_increase_pct: float | None = None

    @field_validator("interval_minutes")
    @classmethod
    def _valid_interval(cls, v: int) -> int:
        if v not in {15, 30, 60}:
            raise ValueError("interval must be 15, 30, or 60 minutes")
        return v

    @field_validator("meter_ids")
    @classmethod
    def _unique_ids(cls, ids: list[str]) -> list[str]:
        seen = set()
        out: list[str] = []
        for m in ids:
            m2 = m.strip()
            if not m2 or m2 in seen:
                continue
            seen.add(m2)
            out.append(m2)
        return out


class BillingMonth(BaseModel):
    month: str  # YYYY-MM
    meter_id: str | None = None  # present when monthly_mode == "per_meter"
    energy_usd: float | None = None
    peak_demand_usd: float | None = None
    capacity_usd: float | None = None
    transmission_usd: float | None = None
    total_spend_usd: float | None = None


class TariffInputs(BaseModel):
    baseline_tariff_name: str | None = None
    historical_billing_file: str | None = None
    monthly_mode: Literal["aggregate", "per_meter"] = "aggregate"
    monthly_billing: list[BillingMonth] = Field(default_factory=list)


class PVRow(BaseModel):
    meter_id: str
    dc_kw: float = 0.0
    ac_kw: float = 0.0


class PVInputs(BaseModel):
    mode: Literal["aggregate", "per_meter"] = "aggregate"
    rows: list[PVRow] = Field(default_factory=list)


class BESSRow(BaseModel):
    meter_id: str
    power_kw: float = 0.0
    energy_kwh: float = 0.0


class BESSInputs(BaseModel):
    mode: Literal["aggregate", "per_meter"] = "aggregate"
    rows: list[BESSRow] = Field(default_factory=list)


class InferredInfo(BaseModel):
    timezone: str | None = None
    utility_name: str | None = None
    service_territory: str | None = None
    iso_rto: str | None = None
    pricing_node: str | None = None
    notes: str | None = None


class SiteBundle(BaseModel):
    version: str = "0.1.0"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    identity: ProjectIdentity
    load: LoadMeta
    tariff: TariffInputs
    pv: PVInputs
    bess: BESSInputs
    inferred: InferredInfo | None = None

    def load_csv_name(self) -> str:
        return "load.csv"

    def json_name(self) -> str:
        return "site_bundle.json"

# --- Merchant overlay helpers ---

def estimate_plc_nspl_savings(
    *,
    current_plc_kw: float,
    current_nspl_kw: float,
    capacity_rate_per_kw_year: float,
    transmission_rate_per_kw_year: float,
    avg_reduction_kw: float,
    coverage_fraction_capacity: float,
    coverage_fraction_transmission: float,
) -> dict[str, float]:
    """
    Screening calculator for PLC/NSPL savings.

    New PLC = max(PLC - avg_reduction_kw * coverage_cap, 0)
    New NSPL = max(NSPL - avg_reduction_kw * coverage_tx, 0)

    Savings = (PLC_old - PLC_new) * cap_rate + (NSPL_old - NSPL_new) * tx_rate
    """
    plc_red = max(min(avg_reduction_kw * coverage_fraction_capacity, current_plc_kw), 0.0)
    nspl_red = max(min(avg_reduction_kw * coverage_fraction_transmission, current_nspl_kw), 0.0)

    new_plc = max(current_plc_kw - plc_red, 0.0)
    new_nspl = max(current_nspl_kw - nspl_red, 0.0)

    cap_save = plc_red * float(capacity_rate_per_kw_year)
    tx_save = nspl_red * float(transmission_rate_per_kw_year)

    return {
        "plc_reduction_kw": plc_red,
        "new_plc_kw": new_plc,
        "capacity_savings_usd_yr": cap_save,
        "nspl_reduction_kw": nspl_red,
        "new_nspl_kw": new_nspl,
        "transmission_savings_usd_yr": tx_save,
        "total_savings_usd_yr": cap_save + tx_save,
    }
