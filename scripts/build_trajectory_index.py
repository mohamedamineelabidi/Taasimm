"""Build GPS trajectory index from Phase-3 curated routes.

Reads data/curated_trajectories_v4.parquet (500 OSRM-routed Casa trips) and
writes a lookup index to data/casa_synthesis/gps_trajectory_index.json:

{
  "by_zone_pair": { "o_id-d_id": [trip_id, ...] },
  "by_tier_pair": { "A-B": [trip_id, ...] },
  "trajectories": { trip_id: {"wkt": "...", "length_m": ..., "duration_s": ...,
                               "start_lat": ..., "start_lon": ...,
                               "end_lat": ...,   "end_lon": ...,
                               "origin_zone_id": ..., "dest_zone_id": ...,
                               "origin_class": ..., "dest_class": ...} }
}

The GPS producer uses by_zone_pair first; falls back to by_tier_pair; finally
picks a random trajectory if both miss.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC  = ROOT / "data" / "curated_trajectories_v4.parquet"
OUT  = ROOT / "data" / "casa_synthesis" / "gps_trajectory_index.json"


def main() -> int:
    if not SRC.exists():
        print(f"ERROR: missing {SRC}")
        return 1

    df = pd.read_parquet(SRC)
    print(f"Loaded {len(df)} trajectories from {SRC.name}")

    by_zone_pair: dict[str, list[str]] = defaultdict(list)
    by_tier_pair: dict[str, list[str]] = defaultdict(list)
    trajectories: dict[str, dict] = {}

    for row in df.itertuples(index=False):
        tid = str(int(row.trip_id))
        trajectories[tid] = {
            "wkt": row.route_wkt,
            "length_m": float(row.route_length_m),
            "duration_s": float(row.route_duration_s),
            "start_lat": float(row.start_lat),
            "start_lon": float(row.start_lon),
            "end_lat": float(row.end_lat),
            "end_lon": float(row.end_lon),
            "origin_zone_id": int(row.origin_zone_id),
            "dest_zone_id": int(row.dest_zone_id),
            "origin_class": str(row.origin_class),
            "dest_class": str(row.dest_class),
        }
        zp = f"{int(row.origin_zone_id)}-{int(row.dest_zone_id)}"
        tp = f"{row.origin_class}-{row.dest_class}"
        by_zone_pair[zp].append(tid)
        by_tier_pair[tp].append(tid)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as f:
        json.dump({
            "by_zone_pair": by_zone_pair,
            "by_tier_pair": by_tier_pair,
            "trajectories": trajectories,
        }, f)
    size_mb = OUT.stat().st_size / 1024 / 1024
    print(f"Wrote {OUT.relative_to(ROOT)} ({size_mb:.2f} MB)")
    print(f"  zone pairs covered: {len(by_zone_pair)}")
    print(f"  tier pairs covered: {len(by_tier_pair)}  {sorted(by_tier_pair)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
