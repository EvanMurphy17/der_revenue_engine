from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from dre.clients.pjm import PJMClient

# ---------- paths ----------

def cache_root(project_root: Path) -> Path:
    base = project_root / "data" / "markets" / "pjm"
    base.mkdir(parents=True, exist_ok=True)
    (base / "prices").mkdir(exist_ok=True)
    (base / "market").mkdir(exist_ok=True)
    (base / "combined").mkdir(exist_ok=True)
    return base


# ---------- month helpers ----------

def _month_start(d: date) -> datetime:
    return datetime(d.year, d.month, 1, 0, 0, 0)


def _next_month(dt: datetime) -> datetime:
    y, m = dt.year, dt.month
    return datetime(y + 1, 1, 1, 0, 0, 0) if m == 12 else datetime(y, m + 1, 1, 0, 0, 0)


def month_bounds(y: int, m: int) -> tuple[datetime, datetime]:
    start = datetime(y, m, 1, 0, 0, 0)
    end = _next_month(start)  # exclusive upper bound
    return start, end


def months_in_window(start: datetime, end: datetime) -> Iterable[tuple[int, int]]:
    """
    Yield (year, month) for each month whose start lies in [start, end).
    i.e., EXCLUDES the end month (end is exclusive).
    """
    cur = datetime(start.year, start.month, 1)
    stop = datetime(end.year, end.month, 1)
    while cur < stop:
        yield cur.year, cur.month
        cur = _next_month(cur)


def rolling_12_full_months(asof: date) -> tuple[datetime, datetime]:
    """
    asof=Jan-2024 -> window is Jan-2023 00:00:00 to Jan-2024 00:00:00 (exclusive end).
    """
    anchor = _month_start(asof)
    end = anchor  # exclusive
    # start = first day of the month 12 months earlier
    y, m = anchor.year, anchor.month
    m0 = m - 12
    y0 = y - 1 if m0 <= 0 else y
    m0 = m0 + 12 if m0 <= 0 else m0
    start = datetime(y0, m0, 1, 0, 0, 0)
    return start, end


# ---------- parquet filenames ----------

def _prices_month_fp(root: Path, y: int, m: int) -> Path:
    return cache_root(root) / "prices" / f"prices_{y:04d}_{m:02d}.parquet"


def _market_month_fp(root: Path, y: int, m: int) -> Path:
    return cache_root(root) / "market" / f"market_{y:04d}_{m:02d}.parquet"


def _combined_month_fp(root: Path, y: int, m: int) -> Path:
    return cache_root(root) / "combined" / f"combined_{y:04d}_{m:02d}.parquet"


# ---------- merge & ratio ----------

def _merge_and_compute_ratio(prices: pd.DataFrame, mkt: pd.DataFrame) -> pd.DataFrame:
    df = prices.copy()
    df["datetime_beginning_ept"] = pd.to_datetime(df["datetime_beginning_ept"], errors="coerce")

    if mkt is not None and not mkt.empty:
        mkt = mkt.copy()
        mkt["datetime_beginning_ept"] = pd.to_datetime(mkt["datetime_beginning_ept"], errors="coerce")

        # accept multiple plausible field names
        if any(c in mkt.columns for c in ["rega_hourly", "reg_a_hourly", "rega_mileage"]):
            rega = pd.to_numeric(
                mkt[[c for c in ["rega_hourly", "reg_a_hourly", "rega_mileage"] if c in mkt.columns]].iloc[:, 0],
                errors="coerce",
            ).replace(0, np.nan)
        else:
            rega = pd.Series(np.nan, index=mkt.index)

        if any(c in mkt.columns for c in ["regd_hourly", "reg_d_hourly", "regd_mileage"]):
            regd = pd.to_numeric(
                mkt[[c for c in ["regd_hourly", "reg_d_hourly", "regd_mileage"] if c in mkt.columns]].iloc[:, 0],
                errors="coerce",
            )
        else:
            regd = pd.Series(np.nan, index=mkt.index)

        mkt["mileage_ratio"] = (regd / rega).replace([np.inf, -np.inf], np.nan).fillna(1.0)
        mkt_ratio = (
            mkt[["datetime_beginning_ept", "mileage_ratio"]]
            .dropna(subset=["datetime_beginning_ept"])
            .groupby("datetime_beginning_ept", as_index=False)["mileage_ratio"]
            .mean()
        )
        df = df.merge(mkt_ratio, on="datetime_beginning_ept", how="left")
    else:
        df["mileage_ratio"] = 1.0

    for c in ("rmccp", "rmpcp", "mileage_ratio"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["datetime_beginning_ept"]).sort_values("datetime_beginning_ept")


# ---------- fetch & cache (month) ----------

def fetch_and_cache_month(client: PJMClient, root: Path, y: int, m: int) -> pd.DataFrame | None:
    """
    Fetch one month of prices + market, write three parquets (prices/market/combined), return combined df.
    Uses EXCLUSIVE end-bound at client level (end - 1s).
    """
    start, end = month_bounds(y, m)

    # prices
    try:
        prices = client.reg_zone_prelim_bill(start, end)
    except Exception:
        prices = pd.DataFrame()

    # market (schema varies; don't pass fields)
    try:
        market = client.reg_market_results(start, end)
    except Exception:
        market = pd.DataFrame()

    if prices is None or prices.empty:
        return None

    _prices_month_fp(root, y, m).parent.mkdir(parents=True, exist_ok=True)
    prices.to_parquet(_prices_month_fp(root, y, m), index=False)

    if market is not None and not market.empty:
        _market_month_fp(root, y, m).parent.mkdir(parents=True, exist_ok=True)
        market.to_parquet(_market_month_fp(root, y, m), index=False)

    combined = _merge_and_compute_ratio(prices, market)
    _combined_month_fp(root, y, m).parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(_combined_month_fp(root, y, m), index=False)
    return combined


def load_cached_month(root: Path, y: int, m: int) -> pd.DataFrame | None:
    f = _combined_month_fp(root, y, m)
    if f.exists():
        try:
            return pd.read_parquet(f)
        except Exception:
            return None
    return None


# ---------- window loaders ----------

def load_or_fetch_window(
    client: PJMClient | None,
    project_root: Path,
    start: datetime,
    end: datetime,
    *,
    fetch_missing: bool = False,
) -> pd.DataFrame:
    """
    Legacy: returns concatenated DataFrame only.
    """
    cache_root(project_root)  # ensure directories
    parts: list[pd.DataFrame] = []
    for y, m in months_in_window(start, end):
        df = load_cached_month(project_root, y, m)
        if df is None and fetch_missing and client is not None:
            df = fetch_and_cache_month(client, project_root, y, m)
        if df is not None and not df.empty:
            parts.append(df)

    if not parts:
        return pd.DataFrame(columns=["datetime_beginning_ept", "rmccp", "rmpcp", "mileage_ratio"])

    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["datetime_beginning_ept"]).sort_values("datetime_beginning_ept").reset_index(drop=True)
    keep = [c for c in ["datetime_beginning_ept", "rmccp", "rmpcp", "mileage_ratio"] if c in out.columns]
    return out[keep]


def load_or_fetch_window_report(
    client: PJMClient | None,
    project_root: Path,
    start: datetime,
    end: datetime,
    *,
    fetch_missing: bool = False,
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    """
    Same as `load_or_fetch_window`, but also returns a per-month report:
      [{'year': 2024, 'month': 1, 'action': 'loaded'|'fetched'|'missing'|'error',
        'prices_path': '...', 'market_path': '...', 'combined_path': '...', 'rows': 744, 'error': '...'}]
    """
    base = cache_root(project_root)
    parts: list[pd.DataFrame] = []
    report: list[dict[str, object]] = []

    for y, m in months_in_window(start, end):
        prices_p = _prices_month_fp(project_root, y, m)
        market_p = _market_month_fp(project_root, y, m)
        comb_p = _combined_month_fp(project_root, y, m)

        error_msg = None
        df = load_cached_month(project_root, y, m)
        action = "loaded" if df is not None else "missing"

        if df is None and fetch_missing and client is not None:
            try:
                df = fetch_and_cache_month(client, project_root, y, m)
                action = "fetched" if df is not None else "missing"
            except Exception as exc:
                action = "error"
                error_msg = str(exc)

        rows = 0 if (df is None or df.empty) else int(len(df))
        if df is not None and not df.empty:
            parts.append(df)

        report.append(
            {
                "year": y,
                "month": m,
                "action": action,
                "rows": rows,
                "prices_path": str(prices_p),
                "market_path": str(market_p),
                "combined_path": str(comb_p),
                "cache_root": str(base),
                "error": error_msg,
            }
        )

    if not parts:
        return pd.DataFrame(columns=["datetime_beginning_ept", "rmccp", "rmpcp", "mileage_ratio"]), report

    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["datetime_beginning_ept"]).sort_values("datetime_beginning_ept").reset_index(drop=True)
    keep = [c for c in ["datetime_beginning_ept", "rmccp", "rmpcp", "mileage_ratio"] if c in out.columns]
    return out[keep], report
