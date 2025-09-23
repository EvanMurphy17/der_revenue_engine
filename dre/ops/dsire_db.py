from __future__ import annotations

from datetime import date
from pathlib import Path

import typer
from rich import print as rprint

from dre.catalog.dsire_catalog import (
    build_from_api,
    catalog_path,
    ensure_schema,
    stats,
)
from dre.config import project_root

app = typer.Typer(help="DSIRE local catalog (SQLite) builder and importer")

# ----------------------------- Typer Options (module-scope) -----------------------------

OPT_START: str | None = typer.Option(
    "2020-01-01",
    help="Start date (YYYY-MM-DD) for DSIRE 'updated' window",
)
OPT_END: str | None = typer.Option(
    None,
    help="End date (YYYY-MM-DD). If omitted, defaults to today's date.",
)
OPT_TAG: str | None = typer.Option(
    None,
    help="Source tag to stamp on rows (defaults to END date)",
)

# -------------------------------------- Helpers --------------------------------------

def _root() -> Path:
    return project_root()

# -------------------------------------- Commands --------------------------------------

@app.command()
def where() -> None:
    """Print the DSIRE catalog path on disk."""
    rprint(catalog_path(_root()))

@app.command()
def init() -> None:
    """Create/ensure the empty DSIRE catalog schema."""
    ensure_schema(_root())
    rprint(f"[green]Catalog ready at:[/green] {catalog_path(_root())}")

# Register both "build-api" and "build_api"
def _do_build_api(start: str | None, end: str | None, tag: str | None) -> None:
    start_str = start or "2020-01-01"
    end_str = end or date.today().isoformat()
    eff_tag = tag or end_str

    out = build_from_api(_root(), start_str, end_str, eff_tag)
    rprint(
        f"[bold]Upserted:[/bold] programs={out.get('programs_upserted',0):,}, "
        f"parameters={out.get('parameters_inserted',0):,}  {out.get('note','')}"
    )
    rprint(f"[blue]{stats(_root())}[/blue]")

@app.command(name="build-api")
def build_api_hyphen(
    start: str | None = OPT_START,
    end: str | None = OPT_END,
    tag: str | None = OPT_TAG,
) -> None:
    """Fetch programs via DSIRE endpoint (month-chunked) and upsert."""
    _do_build_api(start, end, tag)

@app.command(name="build_api")
def build_api_underscore(
    start: str | None = OPT_START,
    end: str | None = OPT_END,
    tag: str | None = OPT_TAG,
) -> None:
    """Alias for build-api (underscore name)."""
    _do_build_api(start, end, tag)

@app.command(name="stats")
def stats_cmd() -> None:
    """Show row counts and catalog path."""
    rprint(stats(_root()))

if __name__ == "__main__":
    app()
