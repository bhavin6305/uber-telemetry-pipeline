"""
prep_sample.py - Build a small, time-ordered Uber sample from the NYC TLC FHVHV parquet.

Why this exists:
  The raw monthly file has ~20M rows (hundreds of MB) across several companies.
  Our producer needs a modest, chronologically ordered CSV of Uber trips only,
  so we can replay it as a "live" event stream.

Memory-safe: the parquet is scanned in batches, never loaded whole.

Usage:
  python3 prep_sample.py --input data/fhvhv_tripdata_2025-09.parquet
  python3 prep_sample.py --input data/fhvhv_tripdata_2025-09.parquet --rows 50000 --day 2025-09-03
"""
import argparse
import re
import sys
from datetime import datetime, timedelta

import pandas as pd
import pyarrow.parquet as pq

UBER_LICENSE = "HV0003"  # HV0005 = Lyft

# Columns we need to run the pipeline. The first group is mandatory.
REQUIRED = ["hvfhs_license_num", "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID"]
OPTIONAL = [
    "request_datetime",
    "dispatching_base_num",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tips",
    "driver_pay",
]


def parse_args():
    p = argparse.ArgumentParser(description="Create an Uber-only CSV sample from FHVHV parquet.")
    p.add_argument("--input", required=True, help="Path to fhvhv_tripdata_YYYY-MM.parquet")
    p.add_argument("--output", default="data/uber_trips.csv", help="Output CSV path")
    p.add_argument("--rows", type=int, default=100_000, help="Max rows to keep (earliest first)")
    p.add_argument("--day", help="Day to sample, YYYY-MM-DD (default: first day of the file's month)")
    p.add_argument("--batch-size", type=int, default=200_000, help="Rows per scan batch")
    return p.parse_args()


def resolve_day(args):
    if args.day:
        return datetime.strptime(args.day, "%Y-%m-%d")
    m = re.search(r"(\d{4})-(\d{2})", args.input)
    if not m:
        sys.exit("Could not infer month from filename. Pass --day YYYY-MM-DD.")
    return datetime(int(m.group(1)), int(m.group(2)), 1)


def main():
    args = parse_args()
    day_start = resolve_day(args)
    day_end = day_start + timedelta(days=1)

    pf = pq.ParquetFile(args.input)
    available = pf.schema_arrow.names
    print(f"File rows     : {pf.metadata.num_rows:,}")
    print(f"Columns found : {available}")

    missing = [c for c in REQUIRED if c not in available]
    if missing:
        sys.exit(f"Missing required columns: {missing}. Paste the column list above to me.")

    columns = REQUIRED + [c for c in OPTIONAL if c in available]
    print(f"Sampling day  : {day_start.date()} (Uber only, license {UBER_LICENSE})")

    kept = []
    scanned = 0
    for batch in pf.iter_batches(batch_size=args.batch_size, columns=columns):
        df = batch.to_pandas()
        scanned += len(df)
        df = df[
            (df["hvfhs_license_num"] == UBER_LICENSE)
            & (df["pickup_datetime"] >= day_start)
            & (df["pickup_datetime"] < day_end)
        ]
        if not df.empty:
            kept.append(df)
        print(f"  scanned {scanned:>12,} rows", end="\r")
    print()

    if not kept:
        sys.exit("No matching rows. Try another --day inside the file's month.")

    out = pd.concat(kept, ignore_index=True)
    out = out.sort_values("pickup_datetime", kind="stable").head(args.rows).reset_index(drop=True)
    out = out.drop(columns=["hvfhs_license_num"])  # constant after filtering

    out.to_csv(args.output, index=False)

    print("\n=== Sample written ===")
    print(f"Path           : {args.output}")
    print(f"Rows           : {len(out):,}")
    print(f"Pickup range   : {out['pickup_datetime'].min()}  ->  {out['pickup_datetime'].max()}")
    print(f"Distinct zones : {out['PULocationID'].nunique()} pickup zones")
    print("\nFirst 3 rows:")
    print(out.head(3).to_string(index=False))


if __name__ == "__main__":
    main()