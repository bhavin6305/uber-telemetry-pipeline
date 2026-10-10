"""
silver.py - Bronze -> Silver: schema enforcement, typing, time features, watermark + deduplication.

Bronze holds raw JSON strings (duplicates and late events included). Silver is the trusted layer:
  1. Parse raw_payload with an EXPLICIT schema (no inference, no surprises).
  2. Cast timestamps; drop rows missing the fields every downstream job depends on.
  3. Derive time features.
  4. Declare a watermark on event_time, then drop duplicates on (ride_id, event_time).

Usage:
  python silver.py --available-now
  python silver.py                      # continuous
"""
import argparse
import os

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from spark_utils import BRONZE_PATH, CHECKPOINT_ROOT, SILVER_PATH, get_spark
from stream_utils import DEFAULT_WATERMARK, run_query

# The contract for the JSON the producer emits. Timestamps arrive as ISO strings and are cast below.
EVENT_SCHEMA = StructType([
    StructField("ride_id", StringType()),
    StructField("event_time", StringType()),
    StructField("request_time", StringType()),
    StructField("dropoff_time", StringType()),
    StructField("pickup_zone_id", IntegerType()),
    StructField("dropoff_zone_id", IntegerType()),
    StructField("trip_miles", DoubleType()),
    StructField("trip_time_sec", LongType()),
    StructField("base_fare", DoubleType()),
    StructField("tips", DoubleType()),
    StructField("driver_pay", DoubleType()),
    StructField("base_num", StringType()),
])


def parse_args():
    p = argparse.ArgumentParser(description="Bronze -> Silver streaming transformation.")
    p.add_argument("--available-now", action="store_true", help="Process all available Bronze data, then stop.")
    p.add_argument("--watermark", default=DEFAULT_WATERMARK, help="Allowed lateness, e.g. '10 minutes'")
    p.add_argument("--trigger-seconds", type=int, default=10)
    return p.parse_args()


def build_silver(bronze_df, watermark):
    # 1) Enforce schema: from_json yields NULL fields for anything that doesn't match.
    parsed = bronze_df.select(
        F.from_json(F.col("raw_payload"), EVENT_SCHEMA).alias("e"),
        F.col("ingestion_ts").alias("bronze_ingestion_ts"),
    ).select("e.*", "bronze_ingestion_ts")

    # 2) Cast ISO strings to proper timestamps.
    typed = (
        parsed
        .withColumn("event_time", F.col("event_time").cast("timestamp"))
        .withColumn("request_time", F.col("request_time").cast("timestamp"))
        .withColumn("dropoff_time", F.col("dropoff_time").cast("timestamp"))
    )

    # Minimum contract: without these we can't dedupe, window, or group.
    valid = typed.filter(
        F.col("ride_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("pickup_zone_id").isNotNull()
    )

    # 3) Time features and derived metrics.
    featured = (
        valid
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("day_of_week", F.date_format("event_time", "E"))
        .withColumn("wait_time_sec", F.col("event_time").cast("long") - F.col("request_time").cast("long"))
        .withColumn("trip_duration_min", F.round(F.col("trip_time_sec") / 60.0, 2))
    )

    # 4) Watermark + dedup. Including event_time in the key lets Spark evict old keys from state
    #    once the watermark passes them; without it, state would grow forever.
    deduped = (
        featured
        .withWatermark("event_time", watermark)
        .dropDuplicates(["ride_id", "event_time"])
    )

    return deduped.withColumn("silver_processed_ts", F.current_timestamp())


def main():
    args = parse_args()
    checkpoint = os.path.join(CHECKPOINT_ROOT, "silver")
    spark = get_spark("silver-transform")

    bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)
    silver = build_silver(bronze_stream, args.watermark)

    writer = (
        silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
    )
    writer = writer.trigger(availableNow=True) if args.available_now \
        else writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

    query = writer.start(SILVER_PATH)
    print(f"Silver stream started. source={BRONZE_PATH}\n target={SILVER_PATH}\n watermark={args.watermark}")
    try:
        run_query(query, args.available_now, "silver")
    finally:
        spark.stop()
        print("Silver transformation finished.")


if __name__ == "__main__":
    main()