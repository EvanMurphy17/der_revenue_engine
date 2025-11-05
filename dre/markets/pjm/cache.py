from __future__ import annotations

import random
import time
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from dre.clients.pjm import PJMClient

# ----------------- paths -----------------

def _root() -> Path:
    return Path("data/markets/pjm").resolve()

def _ensure_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def energy_path(market: str, year: int, yyyymm: str) -> Path:
    sub = "energy_da" if market.upper() == "DA" else "energy_rt"
    return _root() / sub / f"{year}" / f"{yyyymm}.parquet"

def reserves_path(market: str, service_slug: str, year: int, yyyymm: str) -> Path:
    sub = "reserves_da" if market.upper() == "DA" else "reserves_rt"
    safe = service_slug.replace(" ", "_").lower()
    return _root() / sub / safe / f"{year}" / f"{yyyymm}.parquet"

def regulation_path(year: int, yyyymm: str) -> Path:
    return _root() / "regulation" / f"{year}" / f"{yyyymm}.parquet"

# ----------------- month iteration -----------------

def month_windows(start: datetime, end_exclusive: datetime) -> list[tuple[datetime, datetime, str, int]]:
    """
    Return (m0, m1, yyyymm, year) for each closed-open month window in [start, end_exclusive).
    """
    out: list[tuple[datetime, datetime, str, int]] = []
    cur = datetime(start.year, start.month, 1, tzinfo=start.tzinfo)
    while cur < end_exclusive:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        out.append((cur, min(nxt, end_exclusive), f"{cur:%Y%m}", cur.year))
        cur = nxt
    return out

# ----------------- loaders -----------------

def _read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    return None

def load_energy_cached(market: str, start: datetime, end_exclusive: datetime, **_: object) -> pd.DataFrame:
    """
    Load cached DA or RT energy LMPs from parquet files.
    Extra kwargs (zone, pnode_id, etc.) are accepted and ignored for backward compatibility.
    """
    frames: list[pd.DataFrame] = []
    for _m0, _m1, yyyymm, year in month_windows(start, end_exclusive):
        p = energy_path(market, year, yyyymm)
        df = _read_parquet_if_exists(p)
        if df is not None and not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def load_reserves_cached(market: str, ancillary_service: str, start: datetime, end_exclusive: datetime) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    slug = ancillary_service.replace(" ", "_").lower()
    for _m0, _m1, yyyymm, year in month_windows(start, end_exclusive):
        p = reserves_path(market, slug, year, yyyymm)
        df = _read_parquet_if_exists(p)
        if df is not None and not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def load_regulation_cached(start: datetime, end_exclusive: datetime) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for _m0, _m1, yyyymm, year in month_windows(start, end_exclusive):
        p = regulation_path(year, yyyymm)
        df = _read_parquet_if_exists(p)
        if df is not None and not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def load_reserves_da_cached(ancillary_service: str, start: datetime, end_exclusive: datetime) -> pd.DataFrame:
    return load_reserves_cached("DA", ancillary_service, start, end_exclusive)

def load_reserves_rt_cached(ancillary_service: str, start: datetime, end_exclusive: datetime) -> pd.DataFrame:
    return load_reserves_cached("RT", ancillary_service, start, end_exclusive)

# ----------------- prefetchers -----------------

def _sleep_between(sleep_range: tuple[float, float]) -> None:
    lo, hi = sleep_range
    time.sleep(random.uniform(lo, hi))

def prefetch_energy(
    client: PJMClient,
    start: datetime,
    end_exclusive: datetime,
    market: str = "DA",
    pnode_id: int = 1,
    sleep_range: tuple[float, float] = (1.0, 2.0),
    force: bool = False,
) -> int:
    """
    Prefetch DA or RT LMPs at pnode_id=1 (PJM RTO) and write monthly parquet with your naming convention.
    """
    written = 0
    fetch_fn = client.da_hrl_lmps if market.upper() == "DA" else client.rt_hrl_lmps

    for m0, m1, yyyymm, year in month_windows(start, end_exclusive):
        path = energy_path(market, year, yyyymm)
        if path.exists() and not force:
            continue
        df = fetch_fn(m0, m1, pnode_id=pnode_id)
        if df is not None and not df.empty:
            _ensure_dir(path)
            df.to_parquet(path, index=False)
            written += 1
            _sleep_between(sleep_range)
    return written

def prefetch_reserves(
    client: PJMClient,
    start: datetime,
    end_exclusive: datetime,
    market: str,
    ancillary_services: Iterable[str],
    sleep_range: tuple[float, float] = (1.0, 2.0),
    force: bool = False,
) -> int:
    """
    Prefetch DA or RT reserves using dataset-appropriate product names.
    """
    written = 0
    if market.upper() == "DA":
        fetch_fn = client.da_ancillary_services
        valid = set(PJMClient.DA_ANCILLARY_PRODUCTS)
    else:
        fetch_fn = client.ancillary_services
        valid = set(PJMClient.RT_ANCILLARY_PRODUCTS)

    for svc in ancillary_services:
        if svc not in valid:
            print(f"[prefetch_reserves] Skipping invalid product for {market}: {svc!r}")
            continue
        slug = svc.replace(" ", "_").lower()
        for m0, m1, yyyymm, year in month_windows(start, end_exclusive):
            path = reserves_path(market, slug, year, yyyymm)
            if path.exists() and not force:
                continue
            df = fetch_fn(m0, m1, ancillary_service=svc)
            if df is not None and not df.empty:
                _ensure_dir(path)
                df.to_parquet(path, index=False)
                written += 1
                _sleep_between(sleep_range)
    return written

def prefetch_regulation(
    client: PJMClient,
    start: datetime,
    end_exclusive: datetime,
    sleep_range: tuple[float, float] = (1.0, 2.0),
    force: bool = False,
) -> int:
    """
    Prefetch regulation hourlies (rmccp, rmpcp) as monthly parquet.
    """
    written = 0
    for m0, m1, yyyymm, year in month_windows(start, end_exclusive):
        path = regulation_path(year, yyyymm)
        if path.exists() and not force:
            continue
        df_bill = client.reg_zone_prelim_bill(m0, m1)
        _sleep_between(sleep_range)
        df_result = client.reg_market_results(m0, m1)
        df = pd.merge(df_bill, df_result, on="ts", how="inner")
        if df is not None and not df.empty:
            _ensure_dir(path)
            df.to_parquet(path, index=False)
            written += 1
            _sleep_between(sleep_range)
    return written
