from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from dre.clients.dsire import (
    _US_STATE_CODE_TO_NAME,  # for query convenience
    DSIREClient,
    dedupe_records_by_program_id,
    month_chunks,
    normalize_programs,
    parameters_long,
    yyyymmdd,
)


# ---------- Paths ----------
def catalog_dir(repo_root: Path) -> Path:
    d = Path(repo_root) / "data" / "catalog"
    d.mkdir(parents=True, exist_ok=True)
    return d


def catalog_path(repo_root: Path) -> Path:
    return catalog_dir(repo_root) / "dsire.db"


def catalog_exists(repo_root: Path) -> bool:
    return catalog_path(repo_root).exists()


# ---------- Schema ----------
_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS programs (
  program_id TEXT PRIMARY KEY,
  state TEXT,             -- two-letter code when known; some historical rows may be names
  program_name TEXT,
  administrator TEXT,
  type TEXT,
  category TEXT,
  website_url TEXT,
  status TEXT,
  last_update TEXT,
  technologies TEXT,
  sectors TEXT,
  utilities TEXT,
  raw_json TEXT,          -- exact record for audit/traceability
  source_tag TEXT,        -- e.g., YYYY-MM-DD or import label
  updated_ts DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS parameters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  program_id TEXT,
  tech TEXT,
  sector TEXT,
  source TEXT,
  qualifier TEXT,
  amount REAL,
  units TEXT,
  raw_label TEXT,
  raw_value TEXT,
  FOREIGN KEY(program_id) REFERENCES programs(program_id)
);

CREATE INDEX IF NOT EXISTS idx_programs_state ON programs(state);
CREATE INDEX IF NOT EXISTS idx_programs_type ON programs(type);
CREATE INDEX IF NOT EXISTS idx_parameters_program_id ON parameters(program_id);
"""


def _connect(repo_root: Path) -> sqlite3.Connection:
    p = catalog_path(repo_root)
    con = sqlite3.connect(p)
    con.execute("PRAGMA foreign_keys=ON;")
    return con


def ensure_schema(repo_root: Path) -> None:
    con = _connect(repo_root)
    try:
        con.executescript(_SCHEMA)
        con.commit()
    finally:
        con.close()


# ---------- Ingest / upsert ----------
def upsert_records(repo_root: Path, records: list[dict], source_tag: str) -> dict:
    if not records:
        return {"programs_upserted": 0, "parameters_inserted": 0}

    records = dedupe_records_by_program_id(records)
    prog_df = normalize_programs(records)
    params_df = parameters_long(records)

    con = _connect(repo_root)
    try:
        sql = """
        INSERT INTO programs
          (program_id,state,program_name,administrator,type,category,website_url,status,last_update,
           technologies,sectors,utilities,raw_json,source_tag)
        VALUES
          (:program_id,:state,:program_name,:administrator,:type,:category,:website_url,:status,:last_update,
           :technologies,:sectors,:utilities,:raw_json,:source_tag)
        ON CONFLICT(program_id) DO UPDATE SET
          state=excluded.state,
          program_name=excluded.program_name,
          administrator=excluded.administrator,
          type=excluded.type,
          category=excluded.category,
          website_url=excluded.website_url,
          status=excluded.status,
          last_update=excluded.last_update,
          technologies=excluded.technologies,
          sectors=excluded.sectors,
          utilities=excluded.utilities,
          raw_json=excluded.raw_json,
          source_tag=excluded.source_tag,
          updated_ts=CURRENT_TIMESTAMP;
        """
        to_insert = []
        for _, r in prog_df.iterrows():
            d = {
                "program_id": r.get("program_id"),
                "state": r.get("state"),
                "program_name": r.get("program_name"),
                "administrator": r.get("administrator"),
                "type": r.get("type"),
                "category": r.get("category"),
                "website_url": r.get("website_url"),
                "status": r.get("status"),
                "last_update": r.get("last_update"),
                "technologies": r.get("technologies"),
                "sectors": r.get("sectors"),
                "utilities": r.get("utilities"),
                "raw_json": json.dumps(r.get("raw_json", {}), ensure_ascii=False),
                "source_tag": source_tag,
            }
            to_insert.append(d)
        if to_insert:
            con.executemany(sql, to_insert)

        inserted_params = 0
        if not params_df.empty:
            pids = set(params_df["program_id"].dropna().astype(str))
            if pids:
                con.executemany("DELETE FROM parameters WHERE program_id = ?;", [(pid,) for pid in pids])

            p_sql = """
            INSERT INTO parameters
              (program_id,tech,sector,source,qualifier,amount,units,raw_label,raw_value)
            VALUES
              (:program_id,:tech,:sector,:source,:qualifier,:amount,:units,:raw_label,:raw_value);
            """

            def _tofloat(x) -> float | None:
                if x is None:
                    return None
                try:
                    return float(x)
                except Exception:
                    return None

            param_rows: list[dict[str, Any]] = []
            for _, r in params_df.iterrows():
                raw_val = r.get("raw_value")
                param_rows.append(
                    {
                        "program_id": str(r.get("program_id", "")),
                        "tech": _nz(r.get("tech")),
                        "sector": _nz(r.get("sector")),
                        "source": _nz(r.get("source")),
                        "qualifier": _nz(r.get("qualifier")),
                        "amount": _tofloat(r.get("amount")),
                        "units": _nz(r.get("units")),
                        "raw_label": _nz(r.get("raw_label")),
                        "raw_value": json.dumps(raw_val, ensure_ascii=False) if raw_val is not None else None,
                    }
                )
            if param_rows:
                con.executemany(p_sql, param_rows)
                inserted_params = len(param_rows)

        con.commit()
        return {"programs_upserted": len(to_insert), "parameters_inserted": inserted_params}
    finally:
        con.close()


def _nz(x) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


# ---------- Builders ----------
def build_from_api(
    repo_root: Path,
    start_date: str,
    end_date: str,
    tag: str | None = None,
) -> dict:
    ensure_schema(repo_root)
    client = DSIREClient()

    s = date.fromisoformat(start_date)
    e = date.fromisoformat(end_date)
    all_recs: list[dict] = []

    for a, b in month_chunks(s, e):
        s_str, e_str = yyyymmdd(a), yyyymmdd(b)
        recs = client.get_programs_by_date(s_str, e_str)
        all_recs.extend(recs)

    if not all_recs:
        return {"programs_upserted": 0, "parameters_inserted": 0, "note": "No records fetched"}

    return upsert_records(repo_root, all_recs, source_tag=(tag or end_date))


# ---------- Queries for the app ----------
def query_programs_by_state(repo_root: Path, state_code: str) -> pd.DataFrame:
    """
    Return programs for a given state code (e.g., 'IL'). To support older rows
    that may have full state names, we also accept the full name in the query.
    """
    state_name = _US_STATE_CODE_TO_NAME.get(state_code.upper(), None)
    con = _connect(repo_root)
    try:
        if state_name:
            df = pd.read_sql_query(
                "SELECT program_id,state,program_name,administrator,type,category,website_url,status,last_update,"
                "technologies,sectors,utilities,raw_json,source_tag,updated_ts "
                "FROM programs WHERE state = ? OR state = ? ORDER BY program_name;",
                con,
                params=(state_code.upper(), state_name),
            )
        else:
            df = pd.read_sql_query(
                "SELECT program_id,state,program_name,administrator,type,category,website_url,status,last_update,"
                "technologies,sectors,utilities,raw_json,source_tag,updated_ts "
                "FROM programs WHERE state = ? ORDER BY program_name;",
                con,
                params=(state_code.upper(),),
            )
        return df
    finally:
        con.close()


def get_parameters_for_program(repo_root: Path, program_id: str) -> pd.DataFrame:
    con = _connect(repo_root)
    try:
        df = pd.read_sql_query(
            "SELECT program_id,tech,sector,source,qualifier,amount,units,raw_label,raw_value "
            "FROM parameters WHERE program_id = ?;",
            con,
            params=(program_id,),
        )
        return df
    finally:
        con.close()


def stats(repo_root: Path) -> dict:
    con = _connect(repo_root)
    try:
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM programs;")
        n_prog = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM parameters;")
        n_par = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT state) FROM programs;")
        n_states = cur.fetchone()[0]
        return {
            "programs": n_prog,
            "parameters": n_par,
            "states": n_states,
            "path": str(catalog_path(repo_root)),
        }
    finally:
        con.close()
