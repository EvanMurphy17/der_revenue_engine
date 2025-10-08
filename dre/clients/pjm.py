from __future__ import annotations

import os
from typing import Any

import pandas as pd
import requests
from dotenv import dotenv_values, find_dotenv, load_dotenv

try:
    from dre.config import project_root  # type: ignore
except Exception:
    project_root = None


def _first_env(*names: str) -> str | None:
    for n in names:
        v = os.getenv(n)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _ensure_env_loaded() -> None:
    """Load .env from nearest and from <project_root>/.env without overriding existing env."""
    try:
        load_dotenv(find_dotenv(), override=False)
    except Exception:
        pass
    try:
        if project_root:
            env_path = project_root() / ".env"
            if env_path.exists():
                load_dotenv(env_path, override=False)
            else:
                for k, v in dotenv_values(env_path).items():
                    if k and v and k not in os.environ:
                        os.environ[k] = v
    except Exception:
        pass


class PJMClient:
    """
    Minimal PJM Data Miner 2 client.

    Keys may come from args or env:
      Primary: PJM_API_PRIMARY_KEY, PJM_API_KEY, PJM_PRIMARY_KEY, PJM_KEY,
               PJM_DATAMINER_PRIMARY_KEY, DATAMINER_PRIMARY_KEY
      Secondary: PJM_API_SECONDARY_KEY, PJM_SECONDARY_KEY,
                 PJM_DATAMINER_SECONDARY_KEY, DATAMINER_SECONDARY_KEY
    """

    BASE = "https://api.pjm.com/api/v1/"

    def __init__(
        self,
        *,
        primary_key: str | None = None,
        secondary_key: str | None = None,
        session: requests.Session | None = None,
        timeout: float = 60.0,
    ) -> None:
        _ensure_env_loaded()

        self.primary_key = (
            primary_key
            or _first_env(
                "PJM_API_PRIMARY_KEY",
                "PJM_API_KEY",
                "PJM_PRIMARY_KEY",
                "PJM_KEY",
                "PJM_DATAMINER_PRIMARY_KEY",
                "DATAMINER_PRIMARY_KEY",
            )
        )
        self.secondary_key = secondary_key or _first_env(
            "PJM_API_SECONDARY_KEY",
            "PJM_SECONDARY_KEY",
            "PJM_DATAMINER_SECONDARY_KEY",
            "DATAMINER_SECONDARY_KEY",
        )

        if not self.primary_key and not self.secondary_key:
            raise ValueError(
                "No PJM API key provided. Set PJM_API_PRIMARY_KEY (and optional PJM_API_SECONDARY_KEY) "
                "in your environment or pass keys to PJMClient()."
            )

        self._session = session or requests.Session()
        self._timeout = timeout
        self._last_json: dict[str, Any] | list[dict[str, Any]] | None = None
        self._last_by_dataset: dict[str, dict[str, Any] | list[dict[str, Any]]] = {}

    def last_response_json(self, dataset: str | None = None):
        if dataset:
            return self._last_by_dataset.get(dataset)
        return self._last_json

    # ------------------------- internal -------------------------

    def _headers(self, use_secondary: bool = False) -> dict[str, str]:
        key = (self.secondary_key if use_secondary else self.primary_key) or ""
        return {
            "Ocp-Apim-Subscription-Key": key,
            "Accept": "application/json",
        }

    def _get(self, dataset: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.BASE}{dataset}"
        r = self._session.get(url, headers=self._headers(False), params=params, timeout=self._timeout)
        if r.status_code in (401, 403) and self.secondary_key:
            r = self._session.get(url, headers=self._headers(True), params=params, timeout=self._timeout)
        r.raise_for_status()
        obj = r.json()
        self._last_json = obj
        self._last_by_dataset[dataset] = obj
        return obj

    @staticmethod
    def _unwrap_items(obj: dict[str, Any]) -> list[dict]:
        for k in ("items", "data", "result", "Results"):
            if isinstance(obj, dict) and k in obj and isinstance(obj[k], list):
                return obj[k]  # type: ignore[return-value]
        return obj if isinstance(obj, list) else []

    # ------------------------- datasets -------------------------

    def reg_zone_prelim_bill(
        self,
        start_ept,
        end_ept,
        fields: list[str] | None = None,
        row_count: int = 50000,
        start_row: int = 1,
    ) -> pd.DataFrame:
        # Capability & performance prices come from here
        if fields is None:
            fields = [
                "datetime_beginning_ept",
                "datetime_ending_ept",
                "rmccp",  # capability price ($/MW-h)
                "rmpcp",  # performance price ($/MW-h)
            ]

        def _fmt(dt) -> str:
            return dt.strftime("%Y-%m-%d %H:%M:%S") if hasattr(dt, "strftime") else str(dt)

        params = {
            "fields": ",".join(fields),
            "datetime_beginning_ept": f"{_fmt(start_ept)} to {_fmt(end_ept)}",
            "rowCount": int(row_count),
            "startRow": int(start_row),
        }
        data = self._get("reg_zone_prelim_bill", params)
        rows = self._unwrap_items(data)
        df = pd.DataFrame(rows)
        df.columns = [str(c).strip().lower() for c in df.columns]
        return df

    def reg_market_results(
        self,
        start_ept,
        end_ept,
        *,
        include_fields: bool = False,  # IMPORTANT: default False to avoid 400s from unknown field names
        row_count: int = 50000,
        start_row: int = 1,
    ) -> pd.DataFrame:
        """
        Returns hourly market results. We omit `fields` by default to avoid schema mismatch 400s.
        We only need rega/regd hourly mileage and a market area to filter to RTO.
        """
        def _fmt(dt) -> str:
            return dt.strftime("%Y-%m-%d %H:%M:%S") if hasattr(dt, "strftime") else str(dt)

        params: dict[str, Any] = {
            "datetime_beginning_ept": f"{_fmt(start_ept)} to {_fmt(end_ept)}",
            "rowCount": int(row_count),
            "startRow": int(start_row),
        }
        if include_fields:
            # For tenants that require explicit fields, try the conservative set:
            params["fields"] = ",".join(
                [
                    "datetime_beginning_ept",
                    "market_area",
                    "rega_hourly",
                    "regd_hourly",
                ]
            )

        data = self._get("reg_market_results", params)
        rows = self._unwrap_items(data)
        df = pd.DataFrame(rows)
        df.columns = [str(c).strip().lower() for c in df.columns]
        return df
