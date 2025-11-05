from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd

from dre.markets.pjm.cache import load_energy_cached


@dataclass(frozen=True)
class EnergyArbParams:
    market: str              # "DA" or "RT"
    bess_power_kw: float
    bess_energy_kwh: float
    duration_hr: float
    round_trip_eff: float = 0.9


def _coerce(df: pd.DataFrame,
            params: EnergyArbParams) -> pd.DataFrame:
    out = df.copy()
    if "ts" in out.columns:
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
    da = ("system_energy_price_da", "total_lmp_da", "congestion_price_da", "marginal_loss_price_da")
    rt = ("system_energy_price_rt", "total_lmp_rt", "congestion_price_rt", "marginal_loss_price_rt")
    if params.market == "DA":
        price_cols = da
        lmp_col = "total_lmp_da"
    else:
        price_cols = rt
        lmp_col = "total_lmp_rt"
    out = out.rename(columns={lmp_col: "price"})
    for c in price_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
        else:
            out[c] = np.nan
    return out.dropna(subset=["ts"])

def _daily_best_pair_value(day_df: pd.DataFrame) -> float:
    """
    Simple daily one-cycle spread: max(price[j] - price[i]) with j > i.
    """
    if day_df.empty or "price" not in day_df.columns:
        return 0.0
    arr = pd.to_numeric(day_df["price"], errors="coerce").to_numpy(dtype=float)
    if arr.size < 2:
        return 0.0
    min_so_far = arr[0]
    best = 0.0
    for p in arr[1:]:
        if p - min_so_far > best:
            best = p - min_so_far
        if p < min_so_far:
            min_so_far = p
    return float(best)


def estimate_energy_arbitrage(
    start: datetime,
    end_exclusive: datetime,
    params: EnergyArbParams,
) -> dict[str, object]:
    # load cached prices only
    prices = load_energy_cached(params.market, start, end_exclusive)
    prices = _coerce(prices, params)
    if prices.empty:
        return {"gross_usd": 0.0, "raw": pd.DataFrame(), "avg_spread": 0.0}

    prices["day"] = prices["ts"].dt.date

    # Build spreads without rename(None) to keep type checkers happy
    daily_vals = prices.groupby("day").apply(
        lambda g: _daily_best_pair_value(g.reset_index(drop=True))
    )
    # daily_vals is a Series[day] -> spread
    spreads = daily_vals.reset_index()
    spreads.columns = ["day", "spread"]
    spreads["spread"] = pd.to_numeric(spreads["spread"], errors="coerce").fillna(0.0)
    avg_spread = float(spreads["spread"].mean()) if not spreads.empty else 0.0

    # simple annualization: 365 cycles capped by duration; apply RTE and power cap
    rte = float(params.round_trip_eff)
    power_mw = float(params.bess_power_kw) / 1000.0
    cycles = 365.0
    gross = avg_spread * rte * power_mw * cycles

    return {
        "gross_usd": float(gross),
        "raw": prices[["ts", "price"]].copy(),
        "avg_spread": float(avg_spread),
    }
