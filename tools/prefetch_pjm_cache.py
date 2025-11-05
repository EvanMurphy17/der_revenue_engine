from __future__ import annotations

import argparse
from datetime import UTC, datetime

from dre.clients.pjm import PJMClient
from dre.markets.pjm.cache import (
    prefetch_energy,
    prefetch_regulation,
    prefetch_reserves,
)


def _start_end_for_months(months: int) -> tuple[datetime, datetime]:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    end_exclusive = datetime(today.year, today.month, 1, tzinfo=UTC)
    total_months = (end_exclusive.year * 12 + end_exclusive.month - 1) - (months - 1)
    start_year = total_months // 12
    start_month = total_months % 12 + 1
    start = datetime(start_year, start_month, 1, tzinfo=UTC)
    return start, end_exclusive


def main() -> None:
    parser = argparse.ArgumentParser(description="Prefetch PJM cache to avoid 429s.")
    parser.add_argument("--months", type=int, default=24, help="Trailing months to prefetch.")
    parser.add_argument("--sleep", type=float, default=2.0, help="Average sleep seconds; jittered internally.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing cached parquet files.")
    args = parser.parse_args()

    s = max(0.5, args.sleep)
    sleep_range: tuple[float, float] = (s * 0.75, s * 1.25)

    start, end_excl = _start_end_for_months(args.months)
    print(f"== Prefetch window {start:%Y-%m}..{end_excl:%Y-%m} ==")

    client = PJMClient()

    print("regulation:")
    n_reg = prefetch_regulation(client, start, end_excl, sleep_range=sleep_range, force=args.force)
    print(f"  wrote {n_reg} file(s)")

    print("energy DA:")
    n_da = prefetch_energy(client, start, end_excl, market="DA", pnode_id=1, sleep_range=sleep_range, force=args.force)
    print(f"  wrote {n_da} file(s)")

    print("energy RT:")
    n_rt = prefetch_energy(client, start, end_excl, market="RT", pnode_id=1, sleep_range=sleep_range, force=args.force)
    print(f"  wrote {n_rt} file(s)")

    # Dataset-specific product names
    da_services = PJMClient.DA_ANCILLARY_PRODUCTS
    rt_services = PJMClient.RT_ANCILLARY_PRODUCTS

    print("reserves DA:")
    n_rda = prefetch_reserves(
        client, start, end_excl, market="DA", ancillary_services=da_services, sleep_range=sleep_range, force=args.force
    )
    print(f"  wrote {n_rda} file(s)")

    print("reserves RT:")
    n_rrt = prefetch_reserves(
        client, start, end_excl, market="RT", ancillary_services=rt_services, sleep_range=sleep_range, force=args.force
    )
    print(f"  wrote {n_rrt} file(s)")


if __name__ == "__main__":
    main()
