"""
check_gold.py - Verifies Gold by recomputing the same aggregation as a plain batch query from Silver.

If the streaming job handled late events and watermarks correctly, every window Gold emitted
must match the batch recomputation exactly.

Run:  python check_gold.py
"""
import argparse
import sys

from pyspark.sql import functions as F

from spark_utils import GOLD_PATH, SILVER_PATH, get_spark

p = argparse.ArgumentParser()
p.add_argument("--window", default="5 minutes")
p.add_argument("--slide", default="1 minute")
args = p.parse_args()

spark = get_spark("check-gold")
gold = spark.read.format("delta").load(GOLD_PATH)
silver = spark.read.format("delta").load(SILVER_PATH)

g_rows = gold.count()
windows = gold.select("window_start").distinct().count()
dupes = gold.groupBy("window_start", "pickup_zone_id").count().filter("count > 1").count()
unnamed = gold.filter(F.col("pickup_zone").isNull()).count()

expected = (
    silver.groupBy(F.window("event_time", args.window, args.slide), "pickup_zone_id")
    .agg(F.count("*").alias("expected_count"))
    .select(F.col("window.start").alias("window_start"), "pickup_zone_id", "expected_count")
)
compared = gold.select("window_start", "pickup_zone_id", "ride_count").join(
    expected, ["window_start", "pickup_zone_id"], "left"
)
mismatches = compared.filter(
    F.col("expected_count").isNull() | (F.col("ride_count") != F.col("expected_count"))
).count()

silver_max = silver.agg(F.max("event_time")).first()[0]
gold_first, gold_last = gold.agg(F.min("window_start"), F.max("window_end")).first()

print("\n=== Gold verification ===")
print(f"Gold rows (window x zone)        : {g_rows:,}")
print(f"Distinct windows emitted         : {windows:,}")
print(f"Gold covers                      : {gold_first}  ->  {gold_last}")
print(f"Latest Silver event_time         : {silver_max}")
print("(The gap between those last two is expected: windows still open to late events are not emitted yet.)")

checks = {
    "Gold has data": g_rows > 0,
    "No duplicate (window, zone) rows": dupes == 0,
    "Zone names resolved for every row": unnamed == 0,
    "Counts match batch recomputation from Silver": mismatches == 0,
}
print()
ok = True
for name, passed in checks.items():
    print(f"[{'PASS' if passed else 'FAIL'}] {name}")
    ok &= passed
if mismatches:
    print(f"   mismatching rows: {mismatches}")

if g_rows:
    latest = gold.agg(F.max("window_start")).first()[0]
    print(f"\nBusiest pickup zones in the latest emitted window (starting {latest}):")
    (gold.filter(F.col("window_start") == latest)
         .orderBy(F.col("ride_count").desc())
         .select("pickup_zone", "pickup_borough", "ride_count", "avg_base_fare", "avg_wait_sec")
         .show(10, truncate=40))

spark.stop()
sys.exit(0 if ok else 1)