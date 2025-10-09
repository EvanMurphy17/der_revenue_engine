from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

Ranking = Literal["full", "rmccp", "rmpcp"]


@dataclass
class BESSParams:
    """
    Minimal BESS shape for frequency estimation.
    nameplate_mw: regulation capability (MW) we assume offered when selected.
    duration_hours: usable energy (MWh) / nameplate_mw
    """
    nameplate_mw: float
    duration_hours: float


def compute_n_hours_from_cycles(
    *,
    bess: BESSParams,
    annual_cycles: int,
    throughput_ratio_mwh_per_hour: float,
    window_hours: int,
) -> int:
    """
    Convert an annual cycles budget into a total hours budget for Top-N selection.

    Each full cycle consumes `duration_hours` of energy.
    If operating regulation "uses" on average `throughput_ratio` MWh per hour
    (e.g., 0.5 MWh/h), then hours per cycle ≈ duration_hours / throughput_ratio.
    For N cycles per year, total hours ≈ (duration_hours / throughput_ratio) * N.

    We clamp to the number of available rows in the window.
    """
    tr = max(float(throughput_ratio_mwh_per_hour), 1e-6)  # prevent divide-by-zero
    raw_hours = (float(bess.duration_hours) / tr) * float(max(annual_cycles, 0))
    hours = int(math.floor(raw_hours))
    hours = max(0, min(hours, int(max(window_hours, 0))))
    return hours


def _num_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Return a numeric Series for `col` (or NaNs if not present)."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    # ensure index length matches for arithmetic
    return pd.Series(np.nan, index=df.index, dtype="float64")


def _hourly_payment_components(
    df: pd.DataFrame,
    *,
    nameplate_mw: float,
    performance_score: float,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Build hourly capability & performance credits given RMCCP, RMPCP, mileage ratio.

      capability_credit   = RMCCP * MWh * performance_score
      performance_credit  = RMPCP * MWh * mileage_ratio * performance_score
      where MWh = nameplate_mw * 1h  (hourly granularity)
    """
    mw = float(nameplate_mw)
    perf = float(performance_score)

    rmccp = _num_col(df, "rmccp")
    rmpcp = _num_col(df, "rmpcp")
    mileage_ratio = _num_col(df, "mileage_ratio").fillna(1.0)

    mwh = mw * 1.0  # 1h resolution
    cap = (rmccp * mwh * perf).astype("float64")
    perf_cr = (rmpcp * mwh * mileage_ratio * perf).astype("float64")
    total = (cap.fillna(0) + perf_cr.fillna(0)).fillna(0).astype("float64")
    return cap.fillna(0), perf_cr.fillna(0), total


def _rank_vector(
    df: pd.DataFrame,
    *,
    nameplate_mw: float,
    performance_score: float,
    ranking: Ranking,
) -> pd.Series:
    """Ranking vector used for Top-N pick."""
    if ranking == "rmccp":
        return _num_col(df, "rmccp")
    if ranking == "rmpcp":
        return _num_col(df, "rmpcp")
    # default: full hourly payment
    _, _, total = _hourly_payment_components(
        df, nameplate_mw=nameplate_mw, performance_score=performance_score
    )
    return total


def _pick_top_n_with_duration_guard(
    df: pd.DataFrame,
    *,
    score: pd.Series,
    n_hours: int,
    max_consecutive: int,
) -> pd.Index:
    """
    Greedy pick of top-N hours with a 'no more than max_consecutive consecutive hours' constraint.

    Implementation detail:
      - Sort by score descending, then iterate.
      - Keep a boolean mask of chosen hours; when considering hour t, reject if it would
        make any block of > max_consecutive consecutive 'True' values once selected.
    """
    if n_hours <= 0 or df.empty:
        return pd.Index([])

    # Ensure datetime is sorted and unique
    tmp = df.copy()
    tmp = tmp.dropna(subset=["datetime_beginning_ept"]).copy()
    tmp["datetime_beginning_ept"] = pd.to_datetime(tmp["datetime_beginning_ept"], errors="coerce")
    tmp = tmp.dropna(subset=["datetime_beginning_ept"]).drop_duplicates("datetime_beginning_ept")
    tmp = tmp.sort_values("datetime_beginning_ept").reset_index(drop=False)  # keep original index
    orig_index: pd.Series = tmp["index"]

    # Candidate order by score
    score_aligned = score.reindex(df.index)
    order = score_aligned.loc[tmp["index"]].sort_values(ascending=False).index.to_list()

    chosen = np.zeros(len(tmp), dtype=bool)
    n_selected = 0
    pos_map = {idx: pos for pos, idx in enumerate(tmp["index"])}

    for idx in order:
        if n_selected >= n_hours:
            break
        pos = pos_map.get(idx)
        if pos is None:
            continue

        # Try selecting this position
        chosen[pos] = True

        # compute run length around this pos
        run_len = 1
        i = pos - 1
        while i >= 0 and chosen[i]:
            run_len += 1
            i -= 1
        i = pos + 1
        while i < len(chosen) and chosen[i]:
            run_len += 1
            i += 1

        if run_len > max_consecutive:
            chosen[pos] = False  # revert
            continue

        n_selected += 1

    selected_idx_series = orig_index[chosen]
    # Return as an Index (not a Series) to satisfy typing
    return pd.Index(selected_idx_series.to_list())


def estimate_reg_revenue_top_n(
    *,
    hourly_df: pd.DataFrame,
    bess: BESSParams,
    n_hours: int,
    ranking: Ranking = "full",
    performance_score: float = 0.9,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Select top-N hours (with consecutive-hours constraint = round(bess.duration_hours))
    and compute capability/performance/total revenue.

    Returns:
      selected_hours_df, summary_df (richer metrics)
    """
    df = hourly_df.copy()
    if df.empty or n_hours <= 0:
        empty_sel = df.iloc[:0].copy()
        empty_summary = pd.DataFrame(
            {
                "metric": [
                    "selected_hours",
                    "capability_credit_usd",
                    "performance_credit_usd",
                    "total_payment_usd",
                    "avg_rmccp",
                    "avg_rmpcp",
                    "avg_mileage_ratio",
                    "ranking",
                    "nameplate_mw",
                    "performance_score",
                ],
                "value": [0, 0.0, 0.0, 0.0, np.nan, np.nan, np.nan, ranking, bess.nameplate_mw, performance_score],
            }
        )
        return empty_sel, empty_summary

    df["datetime_beginning_ept"] = pd.to_datetime(df["datetime_beginning_ept"], errors="coerce")
    df = df.dropna(subset=["datetime_beginning_ept"]).drop_duplicates("datetime_beginning_ept")
    df = df.sort_values("datetime_beginning_ept").reset_index(drop=True)

    # Ranking vector
    score = _rank_vector(df, nameplate_mw=bess.nameplate_mw, performance_score=performance_score, ranking=ranking)

    # Pick with duration guard
    max_consec = max(1, int(round(bess.duration_hours)))
    sel_idx = _pick_top_n_with_duration_guard(df, score=score, n_hours=int(n_hours), max_consecutive=max_consec)

    sel = df.loc[sel_idx].sort_values("datetime_beginning_ept").reset_index(drop=True)

    # Payments
    cap, perf, total = _hourly_payment_components(sel, nameplate_mw=bess.nameplate_mw, performance_score=performance_score)
    sel = sel.assign(
        capability_credit_usd=cap,
        performance_credit_usd=perf,
        total_payment_usd=total,
    )

    # Summary (richer but simple scalars)
    avg_rmccp = float(_num_col(sel, "rmccp").mean()) if len(sel) else float("nan")
    avg_rmpcp = float(_num_col(sel, "rmpcp").mean()) if len(sel) else float("nan")
    avg_mileage = float(_num_col(sel, "mileage_ratio").mean()) if len(sel) else float("nan")

    summary = pd.DataFrame(
        {
            "metric": [
                "selected_hours",
                "capability_credit_usd",
                "performance_credit_usd",
                "total_payment_usd",
                "avg_rmccp",
                "avg_rmpcp",
                "avg_mileage_ratio",
                "ranking",
                "nameplate_mw",
                "performance_score",
            ],
            "value": [
                int(len(sel)),
                float(cap.sum()),
                float(perf.sum()),
                float(total.sum()),
                avg_rmccp,
                avg_rmpcp,
                avg_mileage,
                ranking,
                float(bess.nameplate_mw),
                float(performance_score),
            ],
        }
    )

    return sel, summary
