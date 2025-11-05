from __future__ import annotations

import os
import random
import time
from datetime import datetime, timedelta

import pandas as pd
import requests


class PJMClient:
    """
    Thin wrapper for PJM Data Miner v1 endpoints used by the Merchant modules.

    Env vars checked (first match wins):
      - PJM_API_PRIMARY_KEY  (or PJM_API_KEY, PJM_PRIMARY_KEY, PJM_KEY)
      - PJM_API_SECONDARY_KEY (optional)

    All methods return pandas.DataFrame.
    """

    BASE = "https://api.pjm.com/api/v1"

    # Accepted product names per dataset (authoritative)
    DA_ANCILLARY_PRODUCTS = (
        "PJM RTO Thirty Minutes Reserve",
        "PJM RTO Synchronized Reserve",
        "PJM RTO Primary Reserve",
    )
    RT_ANCILLARY_PRODUCTS = (
        "RTO Non-Synchronized Reserve",
        "RTO Secondary Reserve",
        "RTO Synchronized Reserve",
    )

    def __init__(
        self,
        primary_key: str | None = None,
        secondary_key: str | None = None,
        timeout: int = 60,
        max_retries: int = 6,
        backoff_base: float = 1.25,
    ) -> None:
        pk = (
            primary_key
            or os.getenv("PJM_API_PRIMARY_KEY")
            or os.getenv("PJM_API_KEY")
            or os.getenv("PJM_PRIMARY_KEY")
            or os.getenv("PJM_KEY")
        )
        if not pk:
            raise ValueError(
                "No PJM API key provided. Set PJM_API_PRIMARY_KEY (and optional PJM_API_SECONDARY_KEY)."
            )
        self.primary_key: str = pk
        self.secondary_key: str | None = secondary_key or os.getenv("PJM_API_SECONDARY_KEY")
        self.timeout: int = timeout
        self.max_retries: int = max_retries
        self.backoff_base: float = backoff_base

    # ----------------- helpers -----------------

    @staticmethod
    def _fmt_ept_range(start: datetime, end_exclusive: datetime) -> str:
        """Exclusive end window expressed in EPT, e.g. '2025-01-01 00:00:00 to 2025-01-31 23:59:59'."""
        end_inclusive = end_exclusive - timedelta(seconds=1)
        return f"{start:%Y-%m-%d %H:%M:%S} to {end_inclusive:%Y-%m-%d %H:%M:%S}"

    def _headers(self) -> dict[str, str]:
        h: dict[str, str] = {"Ocp-Apim-Subscription-Key": self.primary_key}
        if self.secondary_key:
            h["Ocp-Apim-Subscription-Key-Secondary"] = self.secondary_key
        return h

    @staticmethod
    def _stringify_params(params_in: dict[str, object]) -> dict[str, str]:
        """Requests likes Mapping[str, str]. Drop Nones and stringify everything else."""
        out: dict[str, str] = {}
        for k, v in params_in.items():
            if v is None:
                continue
            out[str(k)] = str(v)
        return out

    def _get(self, endpoint: str, params_in: dict[str, object]) -> pd.DataFrame:
        """
        GET with retries for 429 and transient 5xx.
        We avoid 'fields' unless necessary because schemas vary by vintage.
        """
        url = f"{self.BASE}/{endpoint}"
        params_obj = dict(params_in)
        params_obj.setdefault("rowCount", "50000")
        params_obj.setdefault("startRow", "1")
        params = self._stringify_params(params_obj)

        last_exc: requests.HTTPError | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                req = requests.Request("GET", url, params=params, headers=self._headers())
                prepped = req.prepare()
                # Log the exact URL that will be requested
                print(f"[PJMClient] GET {prepped.url}")

                with requests.Session() as s:
                    resp = s.send(prepped, timeout=self.timeout)
                resp.raise_for_status()

                js = resp.json()
                items = js.get("items") if isinstance(js, dict) else None
                if not items or not isinstance(items, list):
                    return pd.DataFrame()
                return pd.DataFrame(items)

            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                last_exc = e
                if status in {429, 500, 502, 503, 504}:
                    sleep_s = (self.backoff_base ** attempt) + random.uniform(0.25, 0.75)
                    time.sleep(sleep_s)
                    continue
                raise

        if last_exc:
            raise last_exc
        return pd.DataFrame()

    # ----------------- endpoints -----------------

    # Energy LMPs — always use pnode_id for DA/RT. pnode_id=1 is PJM RTO.
    def da_hrl_lmps(self, start: datetime, end_exclusive: datetime, pnode_id: int = 1) -> pd.DataFrame:
        ept = self._fmt_ept_range(start, end_exclusive)
        params: dict[str, object] = {"datetime_beginning_ept": ept, "pnode_id": pnode_id}
        df = self._get("da_hrl_lmps", params)
        if "datetime_beginning_ept" in df.columns:
            df = df.rename(columns={"datetime_beginning_ept": "ts"})
        return df

    def rt_hrl_lmps(self, start: datetime, end_exclusive: datetime, pnode_id: int = 1) -> pd.DataFrame:
        ept = self._fmt_ept_range(start, end_exclusive)
        params: dict[str, object] = {"datetime_beginning_ept": ept, "pnode_id": pnode_id}
        df = self._get("rt_hrl_lmps", params)
        if "datetime_beginning_ept" in df.columns:
            df = df.rename(columns={"datetime_beginning_ept": "ts"})
        return df

    # Day-ahead reserves — must use the DA product names exactly
    def da_ancillary_services(
        self,
        start: datetime,
        end_exclusive: datetime,
        ancillary_service: str,
    ) -> pd.DataFrame:
        if ancillary_service not in self.DA_ANCILLARY_PRODUCTS:
            raise ValueError(
                f"Invalid DA ancillary_service: {ancillary_service!r}. "
                f"Must be one of: {self.DA_ANCILLARY_PRODUCTS}."
            )
        ept = self._fmt_ept_range(start, end_exclusive)
        params: dict[str, object] = {"datetime_beginning_ept": ept, "ancillary_service": ancillary_service}
        df = self._get("da_ancillary_services", params)
        if not df.empty:
            if "datetime_beginning_ept" in df.columns:
                df = df.rename(columns={"datetime_beginning_ept": "ts"})
            if "value" in df.columns:
                df = df.rename(columns={"value": "price"})
            if "ancillary_service" in df.columns:
                df = df[df["ancillary_service"] == ancillary_service]
        keep = [c for c in ["ts", "ancillary_service", "price"] if c in df.columns]
        return df[keep] if keep else df

    # Real-time reserves — must use the RT product names exactly
    def ancillary_services(
        self,
        start: datetime,
        end_exclusive: datetime,
        ancillary_service: str,
    ) -> pd.DataFrame:
        if ancillary_service not in self.RT_ANCILLARY_PRODUCTS:
            raise ValueError(
                f"Invalid RT ancillary_service: {ancillary_service!r}. "
                f"Must be one of: {self.RT_ANCILLARY_PRODUCTS}."
            )
        ept = self._fmt_ept_range(start, end_exclusive)
        params: dict[str, object] = {"datetime_beginning_ept": ept, "ancillary_service": ancillary_service}
        df = self._get("ancillary_services", params)
        if not df.empty:
            if "datetime_beginning_ept" in df.columns:
                df = df.rename(columns={"datetime_beginning_ept": "ts"})
            if "value" in df.columns:
                df = df.rename(columns={"value": "price"})
            if "ancillary_service" in df.columns:
                df = df[df["ancillary_service"] == ancillary_service]
        keep = [c for c in ["ts", "ancillary_service", "price"] if c in df.columns]
        return df[keep] if keep else df

    # Regulation — unchanged
    def reg_zone_prelim_bill(self, start: datetime, end_exclusive: datetime) -> pd.DataFrame:
        ept = self._fmt_ept_range(start, end_exclusive)
        df = self._get("reg_zone_prelim_bill", {"datetime_beginning_ept": ept})
        if "datetime_beginning_ept" in df.columns:
            df = df.rename(columns={"datetime_beginning_ept": "ts"})
        keep = [c for c in ["ts", "rmccp", "rmpcp"] if c in df.columns]
        return df[keep] if keep else df

    def reg_market_results(self, start: datetime, end_exclusive: datetime) -> pd.DataFrame:
        ept = self._fmt_ept_range(start, end_exclusive)
        df = self._get("reg_market_results", {"datetime_beginning_ept": ept})
        if "datetime_beginning_ept" in df.columns:
            df = df.rename(columns={"datetime_beginning_ept": "ts"})
        keep = [c for c in ["ts", "rega_hourly", "regd_hourly"] if c in df.columns]
        return df[keep] if keep else df
