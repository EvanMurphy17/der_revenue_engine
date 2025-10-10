from __future__ import annotations

import os
from dataclasses import dataclass

import requests


@dataclass
class OpenEIResult:
    utility_name: str | None = None
    utility_id_eia: int | None = None
    state: str | None = None
    raw: dict | None = None

class OpenEIClient:
    """
    Thin OpenEI client:
      - util_rates (address) -> infer local utility (and sometimes EIA ID)
      - util_cos (aliases) -> EIA utility companies & aliases, incl. EIA IDs
    Docs: util_rates v7, util_cos v3. :contentReference[oaicite:3]{index=3}
    """
    def __init__(self, api_key: str | None = None, session: requests.Session | None = None) -> None:
        self.api_key = api_key or os.getenv("NREL_OPENEI_API_KEY") or os.getenv("OPENEI_API_KEY")
        self.session = session or requests.Session()

    def _get(self, url: str, params: dict) -> dict:
        params = dict(params)
        if self.api_key:
            params.setdefault("api_key", self.api_key)
        params.setdefault("format", "json")
        r = self.session.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def get_by_address(self, address: str) -> list[OpenEIResult]:
        # util_rates supports address-based lookups; we only need the utility name & state. :contentReference[oaicite:4]{index=4}
        url = "https://api.openei.org/utility_rates"
        data = self._get(url, {"version": 7, "address": address, "detail": "full"})
        out: list[OpenEIResult] = []
        for rec in (data.get("items") or []):
            util = rec.get("utility") or rec.get("utility_name")
            state = rec.get("state")
            # some records include eia id fields:
            eiaid = rec.get("eiaid") or rec.get("eia_id") or rec.get("utility_id_eia")
            out.append(OpenEIResult(
                utility_name=(util if isinstance(util, str) else None),
                utility_id_eia=(int(eiaid) if isinstance(eiaid, (int, str)) and str(eiaid).isdigit() else None),
                state=(state if isinstance(state, str) else None),
                raw=rec,
            ))
        # de-duplicate by (name, eia id)
        uniq: dict[tuple, OpenEIResult] = {}
        for r in out:
            key = (r.utility_name, r.utility_id_eia, r.state)
            if key not in uniq:
                uniq[key] = r
        return list(uniq.values())

    def get_utility_aliases(self, query: str) -> list[dict]:
        # util_cos (Utility Companies & Aliases) returns normalized names + aliases with EIA IDs. :contentReference[oaicite:5]{index=5}
        url = "https://api.openei.org/utility_companies"
        data = self._get(url, {"version": 3, "q": query})
        return list(data.get("items") or [])
