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
