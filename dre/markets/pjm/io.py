from __future__ import annotations

from pathlib import Path

import pandas as pd


def _pjm_market_dir(repo_root: Path) -> Path:
    d = Path(repo_root) / "data" / "market" / "pjm"
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_reg_prices_local(repo_root: Path, start_year: int, end_year: int) -> pd.DataFrame | None:
    """
    Load cached PJM regulation prices from Parquet (if present).

    Expected schema:
      ts (timestamp, UTC), rmccp (float), rmpcp (float), mileage_ratio (float)
    """
    p = _pjm_market_dir(repo_root) / "reg_prices.parquet"
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p)
        if not {"ts", "rmccp", "rmpcp", "mileage_ratio"}.issubset(df.columns):
            return None
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df = df.dropna(subset=["ts"])
        df = df[(df["ts"].dt.year >= start_year) & (df["ts"].dt.year <= end_year)]
        return df
    except Exception:
        return None
