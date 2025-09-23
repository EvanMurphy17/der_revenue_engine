from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from dre.models import SiteBundle


def save_site_bundle(
    bundle: SiteBundle, load_df: pd.DataFrame, project_dir: Path
) -> tuple[Path, Path]:
    """
    Write load CSV and bundle JSON into project_dir.
    Returns paths to JSON and CSV.
    """
    project_dir = Path(project_dir)
    project_dir.mkdir(parents=True, exist_ok=True)

    # Save load
    csv_path = project_dir / bundle.load_csv_name()
    # Ensure index name
    if load_df.index.name is None:
        load_df.index.name = "interval_start"
    load_df.to_csv(csv_path)

    # Save bundle
    json_path = project_dir / bundle.json_name()
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(bundle.model_dump(mode="json"), f, ensure_ascii=False, indent=2)

    return json_path, csv_path


# ---------- New helpers for discovery and loading ----------


def projects_root(repo_root: Path) -> Path:
    """Return the projects folder path, creating it if missing."""
    root = Path(repo_root) / "projects"
    root.mkdir(parents=True, exist_ok=True)
    return root


def iter_bundle_paths(projects_dir: Path) -> Iterable[Path]:
    """Yield all site_bundle.json paths under the projects directory."""
    projects_dir = Path(projects_dir)
    if not projects_dir.exists():
        return []
    for json_path in projects_dir.glob("*/site_bundle.json"):
        if json_path.is_file():
            yield json_path


def load_site_bundle(json_path: Path) -> SiteBundle:
    """Load a SiteBundle from a JSON file path."""
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    return SiteBundle.model_validate(data)


def summarize_bundle(json_path: Path, bundle: SiteBundle) -> dict:
    """Lightweight metadata for listing in the UI."""
    meters = len(bundle.load.meter_ids) if bundle.load.per_meter else 1
    return {
        "project_name": bundle.identity.name,
        "folder": json_path.parent.name,
        "json_path": str(json_path),
        "created_at": bundle.created_at.isoformat(timespec="seconds"),
        "customer_type": bundle.identity.customer_type,
        "per_meter": bundle.load.per_meter,
        "meters": meters,
        "interval_min": bundle.load.interval_minutes,
        "start": bundle.load.start,
        "end": bundle.load.end,
    }


def list_projects(projects_dir: Path) -> list[dict]:
    """Return a list of project summaries for all bundles found."""
    items: list[dict] = []
    for jp in iter_bundle_paths(projects_dir):
        try:
            b = load_site_bundle(jp)
            items.append(summarize_bundle(jp, b))
        except Exception:
            # Skip unreadable bundles
            continue
    # sort newest first by created_at then name
    items.sort(key=lambda d: (d.get("created_at", ""), d.get("project_name", "")), reverse=True)
    return items
