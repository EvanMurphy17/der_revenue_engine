from __future__ import annotations

import pandas as pd

from dre.clients.pjm import PJMClient


def _coerce_float(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def reg_zone_prelim_bill(client: PJMClient, start_ept, end_ept) -> pd.DataFrame:
    """
    Returns hourly RMPCP/RMCCP (PJM-wide) from reg_zone_prelim_bill with clean columns.

    Output columns: ['datetime_beginning_ept','rmpcp','rmccp']
    """
    df = client.reg_zone_prelim_bill(start_ept, end_ept)
    if df.empty:
        return df

    # expected columns are already lowercased in the client
    # parse timestamp and keep key price columns
    if "datetime_beginning_ept" in df.columns:
        df["datetime_beginning_ept"] = pd.to_datetime(df["datetime_beginning_ept"], errors="coerce")

    df = _coerce_float(df, ["rmpcp", "rmccp"])
    keep = [c for c in ["datetime_beginning_ept", "rmpcp", "rmccp"] if c in df.columns]
    df = df[keep].dropna(subset=["datetime_beginning_ept"]).sort_values("datetime_beginning_ept").reset_index(drop=True)
    return df


def reg_market_results(client: PJMClient, start_ept, end_ept) -> pd.DataFrame:
    """
    Thin wrapper for future mileage/performance integration.
    """
    df = client.reg_market_results(start_ept, end_ept)
    if df.empty:
        return df
    if "datetime_beginning_ept" in df.columns:
        df["datetime_beginning_ept"] = pd.to_datetime(df["datetime_beginning_ept"], errors="coerce")
    for c in ["rmccp", "rmpcp", "mileage_ratio"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("datetime_beginning_ept").reset_index(drop=True)
