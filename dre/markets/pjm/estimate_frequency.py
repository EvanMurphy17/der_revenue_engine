from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd

from dre.clients.pjm import PJMClient
from dre.markets.pjm.cache import load_regulation_cached, month_windows


@dataclass(frozen=True)
class BESSParams:
    nameplate_mw: float
    duration_hours: float
    annual_cycles: int
    throughput_ratio: float
    round_trip_eff: float


Ranking = str  # "full" | "rmccp" | "rmpcp"


def _coerce_prices(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ts" in out.columns:
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
    for c in ("rmccp", "rmpcp"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
        else:
            out[c] = np.nan
    return out.dropna(subset=["ts"])


def _fetch_mileage_monthly(client: PJMClient, start: datetime, end_exclusive: datetime) -> pd.DataFrame:
    """
    Safely fetch reg_market_results month by month to avoid 400s, then normalize.
    """
    frames: list[pd.DataFrame] = []
    for m0, m1, *_ in month_windows(start, end_exclusive):
        df = client.reg_market_results(m0, m1)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["ts", "rega_hourly", "regd_hourly"])

    out = pd.concat(frames, ignore_index=True)

    # normalize columns
    if "datetime_beginning_ept" in out.columns and "ts" not in out.columns:
        out = out.rename(columns={"datetime_beginning_ept": "ts"})
    if "reg_a_hourly" in out.columns and "rega_hourly" not in out.columns:
        out["rega_hourly"] = out["reg_a_hourly"]
    if "reg_d_hourly" in out.columns and "regd_hourly" not in out.columns:
        out["regd_hourly"] = out["reg_d_hourly"]

    # ensure columns exist for to_numeric to avoid Optional errors
    for c in ("rega_hourly", "regd_hourly"):
        if c not in out.columns:
            out[c] = np.nan

    out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
    out["rega_hourly"] = pd.to_numeric(out["rega_hourly"], errors="coerce")
    out["regd_hourly"] = pd.to_numeric(out["regd_hourly"], errors="coerce")
    out = out.dropna(subset=["ts"])
    return out[["ts", "rega_hourly", "regd_hourly"]]


def estimate_reg_revenue_top_n(
    start: datetime,
    end_exclusive: datetime,
    bess: BESSParams,
    performance_score: float,
    ranking: Ranking,   # "full" | "rmccp" | "rmpcp"
    client: PJMClient,
) -> dict[str, object]:
    """
    Use cached regulation prices for rmccp/rmpcp and monthly API for mileage, then compute a simple revenue proxy.
    """
    # prices from cache
    prices = load_regulation_cached(start, end_exclusive)
    prices = _coerce_prices(prices)

    if prices.empty:
        return {"summary": pd.DataFrame(), "topn": pd.DataFrame()}

    # mileage monthly from API but sliced to months (avoids 400)
    # miles = _fetch_mileage_monthly(client, start, end_exclusive)

    df = prices.copy()
    # if not miles.empty:
    #     df = df.merge(miles, on="ts", how="left")
    # else:
    #     # ensure columns exist
    #     df["rega_hourly"] = np.nan
    #     df["regd_hourly"] = np.nan

    # numeric coercions without Optional get()
    df["rega_hourly"] = pd.to_numeric(df["rega_hourly"], errors="coerce")
    df["regd_hourly"] = pd.to_numeric(df["regd_hourly"], errors="coerce")
    df["rmccp"] = pd.to_numeric(df["rmccp"], errors="coerce").fillna(0.0)
    df["rmpcp"] = pd.to_numeric(df["rmpcp"], errors="coerce").fillna(0.0)

    # mileage ratio with safe division
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = df["regd_hourly"] / df["rega_hourly"]
    ratio = ratio.replace([np.inf, -np.inf], np.nan).fillna(1.0)
    df["mileage_ratio"] = ratio

    mw = float(bess.nameplate_mw)
    perf = float(performance_score)

    comp_map = {
        "full": df["rmccp"] + df["rmpcp"] * df["mileage_ratio"],
        "rmccp": df["rmccp"],
        "rmpcp": df["rmpcp"] * df["mileage_ratio"],
    }
    comp = comp_map[ranking]

    df["hourly_usd_per_mw"] = perf * comp
    df["hourly_usd"] = df["hourly_usd_per_mw"] * mw

    gross = float(df["hourly_usd"].sum())
    topn = df.nlargest(50, "hourly_usd")[["ts", "hourly_usd", "rmccp", "rmpcp", "mileage_ratio"]]

    return {"summary": pd.DataFrame([{"gross_usd": gross}]), "topn": topn}
