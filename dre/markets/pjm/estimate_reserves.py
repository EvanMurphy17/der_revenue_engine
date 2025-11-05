from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from dre.markets.pjm.cache import (
    load_reserves_da_cached,
    load_reserves_rt_cached,
)


@dataclass(frozen=True)
class ReserveParams:
    market: str                      # "DA" or "RT"
    ancillary_service: str
    offered_mw: float
    hours_per_year: int
    # backward-compat with old call sites that passed 'locale'
    locale: str | None = None

def _rename_exact(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    cols = {c: mapping.get(c, c) for c in df.columns}
    return df.rename(columns=cols, errors="ignore")

def _coerce_price_ts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ts" in out.columns:
        out["ts"] = pd.to_datetime(out["ts"], errors="coerce")
    if "price" in out.columns:
        out["price"] = pd.to_numeric(out["price"], errors="coerce")
    return out.dropna(subset=["price"])

def _avg_price(df: pd.DataFrame) -> float:
    if df.empty or "price" not in df.columns:
        return 0.0
    s = pd.to_numeric(df["price"], errors="coerce")
    val = float(s.mean(skipna=True)) if s.notna().any() else 0.0
    return 0.0 if math.isnan(val) else val

def estimate_reserve_revenue(
    start: datetime,
    end_exclusive: datetime,
    params: ReserveParams,
) -> dict[str, object]:
    mkt = params.market.upper()
    if mkt == "DA":
        df = load_reserves_da_cached(params.ancillary_service, start, end_exclusive)
    else:
        df = load_reserves_rt_cached(params.ancillary_service, start, end_exclusive)

    if df is None or df.empty:
        return {"avg_mcp": 0.0, "gross_usd": 0.0, "raw": pd.DataFrame()}

    colmap: dict[str, str] = {}
    if "datetime_beginning_ept" in df.columns and "ts" not in df.columns:
        colmap["datetime_beginning_ept"] = "ts"
    if "clearing_price" in df.columns and "price" not in df.columns:
        colmap["clearing_price"] = "price"

    df2 = _rename_exact(df, colmap)
    df2 = _coerce_price_ts(df2)

    avg_mcp = _avg_price(df2)
    gross = float(avg_mcp) * float(params.offered_mw) * float(params.hours_per_year)

    return {"avg_mcp": float(avg_mcp), "gross_usd": float(gross), "raw": df2}
