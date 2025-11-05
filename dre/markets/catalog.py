from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Literal


# --------------------------------------------------------------------------------------
# ISO / Program type enums
# --------------------------------------------------------------------------------------
class ISO(str, Enum):
    PJM = "PJM"
    CAISO = "CAISO"
    NYISO = "NYISO"
    ISONE = "ISO-NE"
    MISO = "MISO"
    ERCOT = "ERCOT"
    SPP = "SPP"


class ProgramKind(str, Enum):
    REGULATION = "Regulation"
    SYNCH_RESERVE = "Synchronized Reserve"
    NONSPIN_RESERVE = "Non-Spinning Reserve"
    CONT_SUPPLEMENTAL = "Contingency/Supplemental Reserve"
    ENERGY_DA = "Energy Day-Ahead"
    ENERGY_RT = "Energy Real-Time"
    CAPACITY = "Capacity"
    FAST_FREQUENCY = "Fast Frequency Response"
    VOLT_VAR = "Voltage/VAR Support"
    BLACK_START = "Black Start"
    DEMAND_RESPONSE = "Demand Response"
    OTHER = "Other"


# --------------------------------------------------------------------------------------
# Descriptor dataclasses
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class DatasetHint:
    """Human-readable hint about the data we’ll use for the estimator."""
    name: str            # e.g., "reg_zone_prelim_bill"
    provider: str        # e.g., "PJM API"
    notes: str | None = None


@dataclass(frozen=True)
class ProgramDescriptor:
    """A single market program offering within an ISO."""
    iso: ISO
    code: str                       # stable code string (unique per ISO), e.g. "regulation"
    kind: ProgramKind
    name: str
    status: Literal["Implemented", "Planned"]
    calculator_id: str | None    # route/id to an estimator, e.g. "pjm.regulation"
    datasets: list[DatasetHint] = field(default_factory=list)
    notes: str | None = None

    # -------- Back-compat properties expected by older pages --------
    @property
    def program_id(self) -> str:
        """Legacy identifier expected by older UIs."""
        return f"{self.iso.value}:{self.code}"

    @property
    def implemented(self) -> bool:
        """Legacy boolean mapping of status -> implemented."""
        return self.status == "Implemented"

    @property
    def description(self) -> str | None:
        """Legacy description alias for notes."""
        return self.notes


# --------------------------------------------------------------------------------------
# Registry of programs per ISO
# --------------------------------------------------------------------------------------
_REGISTRY: dict[ISO, list[ProgramDescriptor]] = {
    ISO.PJM: [
        ProgramDescriptor(
            iso=ISO.PJM,
            code="regulation",
            kind=ProgramKind.REGULATION,
            name="Regulation (RegD/RegA)",
            status="Implemented",
            calculator_id="pjm.regulation",
            datasets=[
                DatasetHint("reg_zone_prelim_bill", "PJM API", "Capability & Performance prices (hourly)"),
                DatasetHint("reg_market_results", "PJM API", "Hourly mileage: RegA / RegD"),
                DatasetHint("ancillary_services_fivemin_hrl", "PJM API", "Optional validation (5-min)"),
            ],
            notes="Top-N hour selection with BESS duration/throughput constraints.",
        ),
        ProgramDescriptor(
            iso=ISO.PJM,
            code="energy_da",
            kind=ProgramKind.ENERGY_DA,
            name="Energy — Day-Ahead",
            status="Implemented",               # beta
            calculator_id="pjm.energy_da",
            datasets=[DatasetHint("da_hrl_lmps", "PJM API", "Hourly LMPs (zone/pnode)")],
            notes="Beta arbitrage estimator using DA LMPs.",
        ),
        ProgramDescriptor(
            iso=ISO.PJM,
            code="energy_rt",
            kind=ProgramKind.ENERGY_RT,
            name="Energy — Real-Time",
            status="Implemented",               # beta
            calculator_id="pjm.energy_rt",
            datasets=[DatasetHint("rt_hrl_lmps", "PJM API", "Hourly LMPs (zone/pnode)")],
            notes="Beta arbitrage estimator using RT LMPs.",
        ),
        ProgramDescriptor(
            iso=ISO.PJM,
            code="synch_reserve",
            kind=ProgramKind.SYNCH_RESERVE,
            name="Synchronized Reserve",
            status="Implemented",               # beta
            calculator_id="pjm.reserve_rt",
            datasets=[DatasetHint("ancillary_services", "PJM API", "RT ancillary prices")],
            notes="Beta price fetch + placeholder settlement.",
        ),
        ProgramDescriptor(
            iso=ISO.PJM,
            code="nonspin_reserve",
            kind=ProgramKind.NONSPIN_RESERVE,
            name="Non-Spin Reserve",
            status="Implemented",               # beta
            calculator_id="pjm.reserve_rt",
            datasets=[DatasetHint("ancillary_services", "PJM API", "RT ancillary prices")],
            notes="Beta price fetch + placeholder settlement.",
        ),
        ProgramDescriptor(
            iso=ISO.PJM,
            code="capacity",
            kind=ProgramKind.CAPACITY,
            name="Capacity (RPM/BRA)",
            status="Planned",
            calculator_id=None,
            datasets=[DatasetHint("BRA report", "PJM", "Zonal clearing results (manual ingest)")],
        ),
        ProgramDescriptor(
            iso=ISO.PJM,
            code="demand_response",
            kind=ProgramKind.DEMAND_RESPONSE,
            name="Demand Response (PJM DR)",
            status="Planned",
            calculator_id=None,
            datasets=[DatasetHint("core_eia861__yearly_demand_response", "PUDL", "Historical participation / costs")],
        ),
    ],

    ISO.CAISO: [
        ProgramDescriptor(ISO.CAISO, "regulation", ProgramKind.REGULATION, "Regulation Up/Down", "Planned", None,
                          [DatasetHint("ancillary_prices", "CAISO OASIS")]),
        ProgramDescriptor(ISO.CAISO, "spin", ProgramKind.SYNCH_RESERVE, "Spinning Reserve", "Planned", None,
                          [DatasetHint("ancillary_prices", "CAISO OASIS")]),
        ProgramDescriptor(ISO.CAISO, "nonspin", ProgramKind.NONSPIN_RESERVE, "Non-Spinning Reserve", "Planned", None,
                          [DatasetHint("ancillary_prices", "CAISO OASIS")]),
        ProgramDescriptor(ISO.CAISO, "energy_da", ProgramKind.ENERGY_DA, "Energy — Day-Ahead", "Planned", None,
                          [DatasetHint("prices", "CAISO OASIS")]),
        ProgramDescriptor(ISO.CAISO, "energy_rt", ProgramKind.ENERGY_RT, "Energy — Real-Time", "Planned", None,
                          [DatasetHint("prices", "CAISO OASIS")]),
        ProgramDescriptor(ISO.CAISO, "dr", ProgramKind.DEMAND_RESPONSE, "Proxy DR / DRP", "Planned", None,
                          [DatasetHint("reports", "CAISO")]),
    ],

    ISO.NYISO: [
        ProgramDescriptor(ISO.NYISO, "regulation", ProgramKind.REGULATION, "Regulation", "Planned", None,
                          [DatasetHint("market_results", "NYISO MIS", "Mileage/price data")]),
        ProgramDescriptor(ISO.NYISO, "ten_min_spin", ProgramKind.SYNCH_RESERVE, "10-Min Spinning Reserve", "Planned", None,
                          [DatasetHint("reserve_prices", "NYISO MIS")]),
        ProgramDescriptor(ISO.NYISO, "ten_min_nonspin", ProgramKind.NONSPIN_RESERVE, "10-Min Non-Spin", "Planned", None,
                          [DatasetHint("reserve_prices", "NYISO MIS")]),
        ProgramDescriptor(ISO.NYISO, "thirty_min", ProgramKind.CONT_SUPPLEMENTAL, "30-Min Reserve", "Planned", None,
                          [DatasetHint("reserve_prices", "NYISO MIS")]),
        ProgramDescriptor(ISO.NYISO, "energy_da", ProgramKind.ENERGY_DA, "Energy — Day-Ahead", "Planned", None,
                          [DatasetHint("lbmp", "NYISO")]),
        ProgramDescriptor(ISO.NYISO, "energy_rt", ProgramKind.ENERGY_RT, "Energy — Real-Time", "Planned", None,
                          [DatasetHint("lbmp", "NYISO")]),
        ProgramDescriptor(ISO.NYISO, "dr", ProgramKind.DEMAND_RESPONSE, "Commercial System Relief / DL", "Planned", None,
                          [DatasetHint("program_reports", "NYISO/Utilities")]),
    ],

    ISO.ISONE: [
        ProgramDescriptor(ISO.ISONE, "regulation", ProgramKind.REGULATION, "Regulation", "Planned", None,
                          [DatasetHint("ancillary_markets", "ISO-NE")]),
        ProgramDescriptor(ISO.ISONE, "spin", ProgramKind.SYNCH_RESERVE, "Spinning Reserve", "Planned", None,
                          [DatasetHint("ancillary_markets", "ISO-NE")]),
        ProgramDescriptor(ISO.ISONE, "nonspin", ProgramKind.NONSPIN_RESERVE, "Non-Spinning Reserve", "Planned", None,
                          [DatasetHint("ancillary_markets", "ISO-NE")]),
        ProgramDescriptor(ISO.ISONE, "energy_da", ProgramKind.ENERGY_DA, "Energy — Day-Ahead", "Planned", None,
                          [DatasetHint("lmp", "ISO-NE")]),
        ProgramDescriptor(ISO.ISONE, "energy_rt", ProgramKind.ENERGY_RT, "Energy — Real-Time", "Planned", None,
                          [DatasetHint("lmp", "ISO-NE")]),
        ProgramDescriptor(ISO.ISONE, "capacity", ProgramKind.CAPACITY, "FCA/FCM Capacity", "Planned", None,
                          [DatasetHint("auction_results", "ISO-NE")]),
    ],

    ISO.MISO: [
        ProgramDescriptor(ISO.MISO, "regulation", ProgramKind.REGULATION, "Regulation", "Planned", None,
                          [DatasetHint("ancillary_prices", "MISO")]),
        ProgramDescriptor(ISO.MISO, "spin", ProgramKind.SYNCH_RESERVE, "Spinning Reserve", "Planned", None,
                          [DatasetHint("ancillary_prices", "MISO")]),
        ProgramDescriptor(ISO.MISO, "supplemental", ProgramKind.CONT_SUPPLEMENTAL, "Supplemental Reserve", "Planned", None,
                          [DatasetHint("ancillary_prices", "MISO")]),
        ProgramDescriptor(ISO.MISO, "energy_da", ProgramKind.ENERGY_DA, "Energy — Day-Ahead", "Planned", None,
                          [DatasetHint("lmp", "MISO")]),
        ProgramDescriptor(ISO.MISO, "energy_rt", ProgramKind.ENERGY_RT, "Energy — Real-Time", "Planned", None,
                          [DatasetHint("lmp", "MISO")]),
        ProgramDescriptor(ISO.MISO, "dr", ProgramKind.DEMAND_RESPONSE, "Demand Response / Load Mod", "Planned", None,
                          [DatasetHint("reports", "MISO")]),
    ],

    ISO.ERCOT: [
        ProgramDescriptor(ISO.ERCOT, "regulation", ProgramKind.REGULATION, "Regulation Up/Down", "Planned", None,
                          [DatasetHint("ancillary_prices", "ERCOT")]),
        ProgramDescriptor(ISO.ERCOT, "rrs", ProgramKind.FAST_FREQUENCY, "Responsive Reserve Service (RRS)", "Planned", None,
                          [DatasetHint("ancillary_prices", "ERCOT")]),
        ProgramDescriptor(ISO.ERCOT, "nonspin", ProgramKind.NONSPIN_RESERVE, "Non-Spin Reserve", "Planned", None,
                          [DatasetHint("ancillary_prices", "ERCOT")]),
        ProgramDescriptor(ISO.ERCOT, "energy_rt", ProgramKind.ENERGY_RT, "Energy — Real-Time", "Planned", None,
                          [DatasetHint("lmp", "ERCOT")]),
        ProgramDescriptor(ISO.ERCOT, "black_start", ProgramKind.BLACK_START, "Black Start", "Planned", None,
                          [DatasetHint("program_docs", "ERCOT")]),
    ],

    ISO.SPP: [
        ProgramDescriptor(ISO.SPP, "regulation", ProgramKind.REGULATION, "Regulation", "Planned", None,
                          [DatasetHint("ancillary_prices", "SPP")]),
        ProgramDescriptor(ISO.SPP, "spin", ProgramKind.SYNCH_RESERVE, "Spinning Reserve", "Planned", None,
                          [DatasetHint("ancillary_prices", "SPP")]),
        ProgramDescriptor(ISO.SPP, "supplemental", ProgramKind.CONT_SUPPLEMENTAL, "Supplemental Reserve", "Planned", None,
                          [DatasetHint("ancillary_prices", "SPP")]),
        ProgramDescriptor(ISO.SPP, "energy_da", ProgramKind.ENERGY_DA, "Energy — Day-Ahead", "Planned", None,
                          [DatasetHint("lmp", "SPP")]),
        ProgramDescriptor(ISO.SPP, "energy_rt", ProgramKind.ENERGY_RT, "Energy — Real-Time", "Planned", None,
                          [DatasetHint("lmp", "SPP")]),
    ],
}


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------
def list_programs(iso: ISO) -> list[ProgramDescriptor]:
    """Return a copy of the program list for an ISO."""
    return _REGISTRY.get(iso, []).copy()


def all_isos() -> list[ISO]:
    return list(_REGISTRY.keys())


def find_program(iso: ISO, code: str) -> ProgramDescriptor | None:
    for p in _REGISTRY.get(iso, []):
        if p.code == code:
            return p
    return None


# --------------------------------------------------------------------------------------
# Back-compat shim (old pages import this)
# --------------------------------------------------------------------------------------
def programs_for_iso(iso: ISO) -> list[ProgramDescriptor]:
    """Deprecated: use list_programs(iso). Kept for compatibility with older pages."""
    return list_programs(iso)
