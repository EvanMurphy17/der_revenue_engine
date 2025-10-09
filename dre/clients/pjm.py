from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
import requests


class PJMClient:
    """
    Thin wrapper for PJM Data Miner v1 endpoints used by the Merchant Overlay.

    Environment variables checked (first match wins):
      - PJM_API_PRIMARY_KEY (or PJM_API_KEY, PJM_PRIMARY_KEY, PJM_KEY)
      - PJM_API_SECONDARY_KEY (optional)

    Methods return pandas.DataFrame.
    """

    BASE = "https://api.pjm.com/api/v1"

    def __init__(
        self,
        primary_key: str | None = None,
        secondary_key: str | None = None,
        timeout: int = 60,
    ) -> None:
        # Resolve primary as a definite str (not Optional) for type safety
        pk = (
            primary_key
            or os.getenv("PJM_API_PRIMARY_KEY")
            or os.getenv("PJM_API_KEY")
            or os.getenv("PJM_PRIMARY_KEY")
            or os.getenv("PJM_KEY")
        )
        if not pk:
            raise ValueError(
                "No PJM API key provided. Set PJM_API_PRIMARY_KEY (and optional PJM_API_SECONDARY_KEY) "
                "in your environment or pass keys to PJMClient()."
            )
        self.primary_key: str = pk

        # Secondary can remain Optional
        self.secondary_key: str | None = secondary_key or os.getenv("PJM_API_SECONDARY_KEY")
        self.timeout: int = timeout

    # ----------------- helpers -----------------

    @staticmethod
    def _fmt_ept_range(start: datetime, end_exclusive: datetime) -> str:
        """
        Format as EPT range with EXCLUSIVE end (end - 1 second).
        Example: '2023-07-01 00:00:00 to 2023-07-31 23:59:59'
        """
        end_inclusive = end_exclusive - timedelta(seconds=1)
        return f"{start:%Y-%m-%d %H:%M:%S} to {end_inclusive:%Y-%m-%d %H:%M:%S}"

    def _headers(self) -> dict[str, str]:
        """
        Always return a dict[str, str] (no Nones).
        """
        headers: dict[str, str] = {"Ocp-Apim-Subscription-Key": self.primary_key}
        # If you later want to add a secondary header, only include when present to keep typing strict.
        if self.secondary_key:
            headers["Ocp-Apim-Subscription-Key-Secondary"] = self.secondary_key
        return headers

    def _get(self, dataset: str, params: dict[str, str]) -> pd.DataFrame:
        url = f"{self.BASE}/{dataset}"
        # default pagination generous enough for month windows
        params.setdefault("rowCount", "50000")
        params.setdefault("startRow", "1")

        r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)
        r.raise_for_status()
        js = r.json()

        # PJM DMv1 almost always uses top-level "items"
        items = js.get("items") if isinstance(js, dict) else None
        if not items or not isinstance(items, list):
            return pd.DataFrame()
        return pd.DataFrame(items)

    # ----------------- endpoints -----------------

    def reg_zone_prelim_bill(self, start: datetime, end_exclusive: datetime) -> pd.DataFrame:
        """
        Regulation Zone Preliminary Billing Data (hourly):
          We need datetime_beginning_ept, datetime_ending_ept, rmccp, rmpcp.
        """
        ept = self._fmt_ept_range(start, end_exclusive)
        params = {
            # Field filtering can be brittle across vintages; keep minimal then trim columns after.
            # "fields": "datetime_beginning_ept;datetime_ending_ept;rmccp;rmpcp",
            "datetime_beginning_ept": ept,
        }
        df = self._get("reg_zone_prelim_bill", params)
        keep = [c for c in ["datetime_beginning_ept", "datetime_ending_ept", "rmccp", "rmpcp"] if c in df.columns]
        return df[keep] if keep else df

    def reg_market_results(self, start: datetime, end_exclusive: datetime) -> pd.DataFrame:
        """
        Regulation Market Data (hourly):
          We need rega_hourly and regd_hourly to compute Mileage Ratio = regd / rega.
          Avoid passing 'fields' (schema varies by vintage). Trim afterward.
        """
        ept = self._fmt_ept_range(start, end_exclusive)
        params = {
            "datetime_beginning_ept": ept,
            # "fields": "datetime_beginning_ept;market_area;product;rega_hourly;regd_hourly"  # optional; can 400 in some vintages
        }
        df = self._get("reg_market_results", params)

        # unify possible column name variants then trim
        colmap = {
            "reg_a_hourly": "rega_hourly",
            "rega_mileage": "rega_hourly",
            "reg_d_hourly": "regd_hourly",
            "regd_mileage": "regd_hourly",
        }
        for old, new in colmap.items():
            if old in df.columns and new not in df.columns:
                df[new] = df[old]

        keep = [c for c in ["datetime_beginning_ept", "rega_hourly", "regd_hourly", "market_area", "product"] if c in df.columns]
        return df[keep] if keep else df
