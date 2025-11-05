from __future__ import annotations

import argparse
from datetime import datetime

from dre.clients.pjm import PJMClient
from dre.markets.pjm.cache import prefetch_energy, prefetch_regulation, prefetch_reserves


def _ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def main() -> None:
    p = argparse.ArgumentParser(description="Prefetch and cache PJM datasets by month to avoid 429s.")
    p.add_argument("--start", required=True, help="YYYY-MM-DD inclusive month start")
    p.add_argument("--end", required=True, help="YYYY-MM-DD exclusive overall end")
    p.add_argument("--reg", action="store_true", help="Prefetch regulation price + mileage")
    p.add_argument("--energy", choices=["DA", "RT"], help="Prefetch energy LMP market")
    p.add_argument("--zone", default="PJM_RTO", help="Zone when using zone-based LMP")
    p.add_argument("--pnode_id", type=int, help="Optional pnode id; when set, overrides zone")
    p.add_argument("--reserves", choices=["DA", "RT"], help="Prefetch reserves market")
    p.add_argument("--area", default="PJM_RTO", help="Market area for reserves")
    p.add_argument("--products", nargs="*", default=["SYNCH_RESERVE", "NONSPIN_RESERVE", "PRIMARY_RESERVE", "SUPPLEMENTAL"])
    args = p.parse_args()

    start = _ymd(args.start)
    end = _ymd(args.end)
    client = PJMClient()

    if args.reg:
        paths = prefetch_regulation(client, start, end)
        print(f"Regulation cached: {len(paths)} files")

    if args.energy:
        paths = prefetch_energy(client, start, end, market=args.energy, zone=args.zone, pnode_id=args.pnode_id)
        print(f"Energy {args.energy} cached: {len(paths)} files")

    if args.reserves:
        paths = prefetch_reserves(client, start, end, market=args.reserves, market_area=args.area, products=args.products)
        print(f"Reserves {args.reserves} cached: {len(paths)} files")


if __name__ == "__main__":
    main()
