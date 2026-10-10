"""
check_silver.py - Verifies Silver against Bronze with explicit PASS/FAIL checks.
Run:  python check_silver.py
"""
import sys

from pyspark.sql import functions as F

from spark_utils import BRONZE_PATH, SILVER_PATH, get_spark

spark = get_spark("check-silver")

bronze = spark.read.format("delta").load(BRONZE_PATH)
silver = spark.read.format("delta").load(SILVER_PATH)

b_rows = bronze.count()
b_unique = bronze.select(F.get_json_object("raw_payload", "$.ride_id").alias("rid")).distinct().count()
s_rows = silver.count()
s_unique = silver.select("ride_id").distinct().count()
null_keys = silver.filter(
    F.col("ride_id").isNull() | F.col("event_time").isNull() | F.col("pickup_zone_id").isNull()
).count()
min_t, max_t = silver.agg(F.min("event_time"), F.max("event_time")).first()

print("\n=== Silver verification ===")
print(f"Bronze rows (raw, incl. duplicates) : {b_rows:,}")
print(f"Bronze unique ride_ids              : {b_unique:,}")
print(f"Silver rows                         : {s_rows:,}")
print(f"Silver unique ride_ids              : {s_unique:,}")
print(f"Unique rides missing from Silver    : {b_unique - s_unique:,}  (dropped as too late or invalid)")
print(f"Silver event_time range             : {min_t}  ->  {max_t}")

checks = {
    "No duplicate ride_ids in Silver": s_rows == s_unique,
    "No NULL keys in Silver": null_keys == 0,
    "Silver is smaller than Bronze (duplicates removed)": s_rows < b_rows,
}
print()
ok = True
for name, passed in checks.items():
    print(f"[{'PASS' if passed else 'FAIL'}] {name}")
    ok &= passed

print("\nSample rows:")
silver.select("ride_id", "event_time", "pickup_zone_id", "event_hour", "day_of_week",
              "wait_time_sec", "trip_duration_min", "base_fare").orderBy("event_time").show(5, truncate=40)

spark.stop()
sys.exit(0 if ok else 1)