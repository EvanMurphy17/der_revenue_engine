from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path

import requests
import typer

from dre.config import project_root

# PUDL nightly parquet bucket & required tables
# These match the “Download full table as Parquet” links in Catalyst’s Data Viewer. 
# We rely on PUDL’s nightly S3 for direct parquet downloads. 
DEFAULT_BASE = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly"

REQUIRED = {
    "core_eia861__yearly_utility_data_misc": "core_eia861__yearly_utility_data_misc.parquet",
    "core_eia861__assn_balancing_authority": "core_eia861__assn_balancing_authority.parquet",
    "core_eia861__yearly_utility_data_rto": "core_eia861__yearly_utility_data_rto.parquet",
    # NEW: Demand Response (EIA-861)
    "core_eia861__yearly_demand_response": "core_eia861__yearly_demand_response.parquet",
}

@dataclass
class FetchReport:
    name: str
    dest: str
    url: str
    status: str
    bytes: int | None = None


def _dest_dir() -> Path:
    return project_root() / "data" / "external" / "pudl"


def _base_url() -> str:
    return os.getenv("PUDL_PARQUET_BASE", DEFAULT_BASE).rstrip("/")


def _exists(p: Path) -> bool:
    return p.exists() and p.stat().st_size > 0


def _download(url: str, dest: Path) -> FetchReport:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return FetchReport(name=dest.name, dest=str(dest), url=url, status="downloaded", bytes=dest.stat().st_size)


def ensure_tables(force: bool = False, only: Iterable[str] | None = None) -> list[FetchReport]:
    reports: list[FetchReport] = []
    base = _base_url()
    out = _dest_dir()
    items = REQUIRED.items() if only is None else [(k, REQUIRED[k]) for k in only if k in REQUIRED]
    for key, fname in items:
        dest = out / fname
        url = f"{base}/{fname}"
        if not force and _exists(dest):
            reports.append(FetchReport(name=key, dest=str(dest), url=url, status="exists", bytes=dest.stat().st_size))
            continue
        try:
            rep = _download(url, dest)
            rep.name = key
            reports.append(rep)
        except Exception as e:
            reports.append(FetchReport(name=key, dest=str(dest), url=url, status=f"error: {e}", bytes=None))
    return reports


# ---------------- CLI ----------------
app = typer.Typer(help="Fetch / manage local PUDL (EIA-861) parquet tables for ISO/BA/DR inference.")

@app.command()
def status():
    """Show local presence of required parquets."""
    out = _dest_dir()
    base = _base_url()
    rows: list[FetchReport] = []
    for key, fname in REQUIRED.items():
        p = out / fname
        rows.append(FetchReport(
            name=key, dest=str(p), url=f"{base}/{fname}",
            status=("exists" if _exists(p) else "missing"),
            bytes=(p.stat().st_size if p.exists() else None),
        ))
    for r in rows:
        typer.echo(asdict(r))


@app.command()
def download(
    force: bool = typer.Option(False, "--force", help="Re-download even if file exists."),
    table: str | None = typer.Option(None, "--table", help="Download only this table (by key in REQUIRED)."),
):
    """Download required parquets (nightly)."""
    only = [table] if table else None
    reps = ensure_tables(force=force, only=only)
    for r in reps:
        typer.echo(asdict(r))


if __name__ == "__main__":
    app()
