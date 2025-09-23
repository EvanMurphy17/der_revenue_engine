from __future__ import annotations

import re
from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class ProjectIdentity(BaseModel):
    name: str
    customer_type: str
    site_address: str
    notes: Optional[str] = None

    def safe_slug(self) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", self.name).strip("_").lower()
        return slug or "project"


class LoadMeta(BaseModel):
    per_meter: bool
    meter_ids: List[str] = Field(default_factory=list)
    interval_minutes: int = 60
    start: str
    end: str
    est_increase_kw: Optional[float] = None
    est_increase_pct: Optional[float] = None

    @field_validator("interval_minutes")
    @classmethod
    def _valid_interval(cls, v: int) -> int:
        if v not in {15, 30, 60}:
            raise ValueError("interval must be 15, 30, or 60 minutes")
        return v

    @field_validator("meter_ids")
    @classmethod
    def _unique_ids(cls, ids: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for m in ids:
            m2 = m.strip()
            if not m2 or m2 in seen:
                continue
            seen.add(m2)
            out.append(m2)
        return out


class BillingMonth(BaseModel):
    month: str  # YYYY-MM
    meter_id: Optional[str] = None  # present when monthly_mode == "per_meter"
    energy_usd: Optional[float] = None
    peak_demand_usd: Optional[float] = None
    capacity_usd: Optional[float] = None
    transmission_usd: Optional[float] = None
    total_spend_usd: Optional[float] = None


class TariffInputs(BaseModel):
    baseline_tariff_name: Optional[str] = None
    historical_billing_file: Optional[str] = None
    monthly_mode: Literal["aggregate", "per_meter"] = "aggregate"
    monthly_billing: List[BillingMonth] = Field(default_factory=list)


class PVRow(BaseModel):
    meter_id: str
    dc_kw: float = 0.0
    ac_kw: float = 0.0


class PVInputs(BaseModel):
    mode: Literal["aggregate", "per_meter"] = "aggregate"
    rows: List[PVRow] = Field(default_factory=list)


class BESSRow(BaseModel):
    meter_id: str
    power_kw: float = 0.0
    energy_kwh: float = 0.0


class BESSInputs(BaseModel):
    mode: Literal["aggregate", "per_meter"] = "aggregate"
    rows: List[BESSRow] = Field(default_factory=list)


class InferredInfo(BaseModel):
    timezone: Optional[str] = None
    utility_name: Optional[str] = None
    service_territory: Optional[str] = None
    iso_rto: Optional[str] = None
    pricing_node: Optional[str] = None
    notes: Optional[str] = None


class SiteBundle(BaseModel):
    version: str = "0.1.0"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    identity: ProjectIdentity
    load: LoadMeta
    tariff: TariffInputs
    pv: PVInputs
    bess: BESSInputs
    inferred: Optional[InferredInfo] = None

    def load_csv_name(self) -> str:
        return "load.csv"

    def json_name(self) -> str:
        return "site_bundle.json"
