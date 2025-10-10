from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MarketProgram:
    program_id: str
    name: str
    iso: str
    description: str
    implemented: bool
    estimator_ref: str | None = None  # module:function path for lazy import


# High-level catalog of programs we aim to support
PROGRAMS = [
    # PJM
    MarketProgram(
        program_id="regulation",
        name="Regulation (RegD/RegA)",
        iso="PJM",
        description="Ancillary frequency regulation with capability & performance credits.",
        implemented=True,
        estimator_ref="dre.markets.pjm.estimate_frequency:estimate_reg_revenue_top_n",
    ),
    MarketProgram(
        program_id="sync_reserve",
        iso="PJM",
        name="Synchronized Reserve",
        description="Short-notice reserve product; real-time & day-ahead variants.",
        implemented=False,
    ),
    MarketProgram(
        program_id="capacity_rpm",
        iso="PJM",
        name="Capacity (RPM)",
        description="Forward capacity market (clearing price × UCAP).",
        implemented=False,
    ),
    MarketProgram(
        program_id="energy_arbitrage",
        iso="PJM",
        name="Energy Arbitrage",
        description="Buy low / sell high using LMP spreads and round-trip efficiency.",
        implemented=False,
    ),
    # CAISO
    MarketProgram(
        program_id="regulation",
        iso="CAISO",
        name="Regulation",
        description="Regulation Up/Down via CAISO AS markets.",
        implemented=False,
    ),
    # NYISO
    MarketProgram(
        program_id="regulation",
        iso="NYISO",
        name="Regulation",
        description="Regulation Service via NYISO AS markets.",
        implemented=False,
    ),
    # ISO-NE
    MarketProgram(
        program_id="regulation",
        iso="ISONE",
        name="Regulation",
        description="ISO-NE regulation reserves.",
        implemented=False,
    ),
    # MISO
    MarketProgram(
        program_id="regulation",
        iso="MISO",
        name="Regulation",
        description="MISO regulation reserves.",
        implemented=False,
    ),
    # SPP
    MarketProgram(
        program_id="regulation",
        iso="SPP",
        name="Regulation",
        description="SPP regulation reserves.",
        implemented=False,
    ),
    # ERCOT (not an ISO, but market)
    MarketProgram(
        program_id="regulation",
        iso="ERCOT",
        name="Regulation",
        description="ERCOT Reg-Up / Reg-Down.",
        implemented=False,
    ),
]


def programs_for_iso(iso: str) -> list[MarketProgram]:
    iso = (iso or "").upper()
    return [p for p in PROGRAMS if p.iso == iso]
