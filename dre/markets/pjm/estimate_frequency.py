from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import cast

import numpy as np
import pandas as pd

from dre.clients.pjm import PJMClient

from .feeds import (
    reg_zone_prelim_bill,  # keep using feeds for prices; call client directly for market results
)


@dataclass
class BESSParams:
    nameplate_mw: float
    duration_hours: float
    max_reg_hours_per_day: float | None = None
    roundtrip_efficiency: float = 0.88  # placeholder for future energy checks


# ---------- duration-feasible Top-N selector ----------

def _count_consecutive(selected: set[pd.Timestamp], t: pd.Timestamp, step_hours: int) -> int:
    cnt = 0
    cur = t + pd.Timedelta(hours=step_hours)
    while cur in selected:
        cnt += 1
        cur = cur + pd.Timedelta(hours=step_hours)
    return cnt


def select_top_n_hours_respecting_duration(
    hourly_df: pd.DataFrame,
    n_hours: int,
    duration_hours: float,
    *,
    ts_col: str = "datetime_beginning_ept",
    score_col: str = "rank_metric",
) -> pd.DataFrame:
    """
    Greedy select highest `score_col` hours with constraint: no >duration_hours consecutive hours.
    """
    if hourly_df.empty or n_hours <= 0:
        return hourly_df.head(0).copy()

    df = hourly_df.dropna(subset=[ts_col, score_col]).copy()
    df[ts_col] = pd.to_datetime(df[ts_col])
    df = df.sort_values(score_col, ascending=False).reset_index(drop=True)

    dur = int(round(duration_hours))
    selected: set[pd.Timestamp] = set()
    chosen_rows: list[int] = []

    for i, row in df.iterrows():
        idx = cast(int, i)  # appease type checkers
        t = cast(pd.Timestamp, row[ts_col])
        left = _count_consecutive(selected, t, -1)
        right = _count_consecutive(selected, t, +1)
        if left + 1 + right <= dur:
            selected.add(t)
            chosen_rows.append(idx)
            if len(chosen_rows) >= n_hours:
                break

    out = df.loc[chosen_rows].sort_values(ts_col).reset_index(drop=True)
    return out


# ---------- helpers ----------

def _series_numeric(df: pd.DataFrame, candidates: list[str], default: float = np.nan) -> pd.Series:
    """Return the first present candidate column coerced to numeric; else a numeric Series of `default`."""
    for c in candidates:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce")
    return pd.Series(default, index=df.index, dtype="float64")


def _filter_rto(df: pd.DataFrame) -> pd.DataFrame:
    # Restrict to PJM RTO / aggregated area
    for col in ("market_area", "marketregionname", "market_region", "market"):
        if col in df.columns:
            s = df[col].astype(str).str.upper()
            mask = s.str.contains("RTO")
            return df[mask]
    return df  # if no area column, pass through


# ---------- Estimator (capability + performance credits) ----------

def estimate_reg_revenue_top_n(
    client: PJMClient,
    start_ept: datetime,
    end_ept: datetime,
    bess: BESSParams,
    n_hours: int,
    *,
    ranking: str = "full",            # "full" (default), "rmccp", or "rmpcp"
    performance_score: float = 0.90,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    1) Pull hourly RMCCP/RMPCP (reg_zone_prelim_bill).
    2) Pull regA/regD hourly mileage (reg_market_results), compute ratio=regd/rega.
    3) Build ranking metric per `ranking` and select Top-N with duration feasibility.
    4) Compute credits:
         mwh = committed_mw * 1.0  (hourly resolution)
         capability_credit = rmccp * mwh * performance_score
         performance_credit = rmpcp * mwh * mileage_ratio * performance_score
         total_payment = capability_credit + performance_credit
    """
    # Prices (via feeds wrapper)
    prices = reg_zone_prelim_bill(client, start_ept, end_ept)
    if prices.empty:
        return prices.head(0), pd.DataFrame({"metric": ["hours_selected"], "value": [0]})
    prices = prices[["datetime_beginning_ept", "rmccp", "rmpcp"]].dropna(subset=["datetime_beginning_ept"])
    prices["datetime_beginning_ept"] = pd.to_datetime(prices["datetime_beginning_ept"])
    prices["rmccp"] = pd.to_numeric(prices["rmccp"], errors="coerce")
    prices["rmpcp"] = pd.to_numeric(prices["rmpcp"], errors="coerce")

    # Market results: hourly mileage fields (call the client method directly)
    mkt = client.reg_market_results(start_ept, end_ept)  # omit fields to avoid 400s
    if not mkt.empty and "datetime_beginning_ept" in mkt.columns:
        mkt["datetime_beginning_ept"] = pd.to_datetime(mkt["datetime_beginning_ept"], errors="coerce")
        mkt = _filter_rto(mkt)

        # Accept multiple plausible field names; per PJM docs: rega_hourly / regd_hourly
        rega = _series_numeric(mkt, ["rega_hourly", "reg_a_hourly", "rega_mileage"]).replace(0, np.nan)
        regd = _series_numeric(mkt, ["regd_hourly", "reg_d_hourly", "regd_mileage"])
        mkt = mkt.assign(mileage_ratio=(regd / rega).replace([np.inf, -np.inf], np.nan).fillna(1.0))

        ratio_df = mkt[["datetime_beginning_ept", "mileage_ratio"]].dropna(subset=["datetime_beginning_ept"])
        ratio_df = ratio_df.groupby("datetime_beginning_ept", as_index=False)["mileage_ratio"].mean()
    else:
        ratio_df = pd.DataFrame({"datetime_beginning_ept": prices["datetime_beginning_ept"], "mileage_ratio": 1.0})

    # Merge ratio into prices
    merged = prices.merge(ratio_df, on="datetime_beginning_ept", how="left")
    merged["mileage_ratio"] = pd.to_numeric(merged["mileage_ratio"], errors="coerce").fillna(1.0)

    # Build ranking metric
    r = ranking.strip().lower()
    if r == "rmccp":
        merged["rank_metric"] = merged["rmccp"]
    elif r == "rmpcp":
        merged["rank_metric"] = merged["rmpcp"]
    else:
        # Full hourly payment per MW (constants M & S omitted since they don't affect ranking)
        merged["rank_metric"] = merged["rmccp"] + merged["rmpcp"] * merged["mileage_ratio"]

    # Top-N selection with duration constraint
    chosen = select_top_n_hours_respecting_duration(
        merged,
        n_hours=n_hours,
        duration_hours=float(bess.duration_hours),
        ts_col="datetime_beginning_ept",
        score_col="rank_metric",
    )

    committed_mw = float(bess.nameplate_mw)
    chosen = chosen.copy()
    chosen["committed_mw"] = committed_mw
    chosen["mwh"] = committed_mw * 1.0
    chosen["performance_score"] = float(performance_score)

    # Credits
    chosen["capability_credit_$"] = chosen["rmccp"] * chosen["mwh"] * chosen["performance_score"]
    chosen["performance_credit_$"] = (
        chosen["rmpcp"] * chosen["mwh"] * chosen["mileage_ratio"] * chosen["performance_score"]
    )
    chosen["total_payment_$"] = chosen["capability_credit_$"] + chosen["performance_credit_$"]

    # Summary (keep numeric for downloads; cast to string in UI if needed)
    summary = pd.DataFrame(
        {
            "metric": [
                "ranking",
                "hours_selected",
                "avg_RMPCP",
                "avg_RMCCP",
                "avg_mileage_ratio",
                "performance_score",
                "committed_MW",
                "total_capability_$",
                "total_performance_$",
                "total_payment_$",
            ],
            "value": [
                r,
                int(len(chosen)),
                float(np.nanmean(chosen["rmpcp"])) if len(chosen) else np.nan,
                float(np.nanmean(chosen["rmccp"])) if len(chosen) else np.nan,
                float(np.nanmean(chosen["mileage_ratio"])) if len(chosen) else np.nan,
                float(performance_score),
                committed_mw,
                float(chosen["capability_credit_$"].sum()),
                float(chosen["performance_credit_$"].sum()),
                float(chosen["total_payment_$"].sum()),
            ],
        }
    )

    return chosen, summary
