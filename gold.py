"""
gold.py - Silver -> Gold: 5-minute sliding-window aggregation of rides per pickup zone.

For every (window, pickup zone) we report how many rides started, plus average fare, wait and distance.
Zone names are joined from the TLC lookup table so the output is human-readable.

How watermarks work here:
  * The watermark is (max event_time seen so far) - allowed lateness.
  * In append mode a window is emitted exactly once, and only after the watermark passes its end.
    That is why the newest windows are not visible yet: Spark is still willing to accept late events for them.
  * Events older than the watermark are dropped from the aggregation.

Usage:
  python gold.py --available-now
  python gold.py                          # continuous
"""
import argparse
import os

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_utils import BASE_DIR, CHECKPOINT_ROOT, GOLD_PATH, SILVER_PATH, get_spark
from stream_utils import DEFAULT_WATERMARK, run_query

ZONE_LOOKUP_PATH = os.path.join(BASE_DIR, "data", "taxi_zone_lookup.csv")

ZONE_SCHEMA = StructType([
    StructField("LocationID", IntegerType()),
    StructField("Borough", StringType()),
    StructField("Zone", StringType()),
    StructField("service_zone", StringType()),
])


def parse_args():
    p = argparse.ArgumentParser(description="Silver -> Gold windowed aggregation.")
    p.add_argument("--available-now", action="store_true")
    p.add_argument("--window", default="5 minutes", help="Window length")
    p.add_argument("--slide", default="1 minute", help="Slide interval (window advances this often)")
    p.add_argument("--watermark", default=DEFAULT_WATERMARK)
    p.add_argument("--trigger-seconds", type=int, default=10)
    return p.parse_args()


def build_gold(spark, silver_df, window, slide, watermark):
    # Static dimension table: small, so it is broadcast to every task.
    zones = (
        spark.read.csv(ZONE_LOOKUP_PATH, header=True, schema=ZONE_SCHEMA)
        .select(
            F.col("LocationID").alias("zone_id"),
            F.col("Zone").alias("pickup_zone"),
            F.col("Borough").alias("pickup_borough"),
        )
    )

    # Windows are defined on event time, NOT on when the row happened to arrive.
    agg = (
        silver_df
        .withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window, slide), F.col("pickup_zone_id"))
        .agg(
            F.count("*").alias("ride_count"),
            F.round(F.avg("base_fare"), 2).alias("avg_base_fare"),
            F.round(F.avg("wait_time_sec"), 1).alias("avg_wait_sec"),
            F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pickup_zone_id", "ride_count", "avg_base_fare", "avg_wait_sec", "avg_trip_miles",
        )
    )

    # Stream-static join: enrich each emitted window row with the zone name.
    return (
        agg.join(F.broadcast(zones), agg.pickup_zone_id == zones.zone_id, "left")
        .drop("zone_id")
        .withColumn("gold_processed_ts", F.current_timestamp())
    )


def main():
    args = parse_args()
    checkpoint = os.path.join(CHECKPOINT_ROOT, "gold")
    spark = get_spark("gold-aggregate")

    silver_stream = spark.readStream.format("delta").load(SILVER_PATH)
    gold = build_gold(spark, silver_stream, args.window, args.slide, args.watermark)

    writer = (
        gold.writeStream.format("delta")
        .outputMode("append")  # each window is written once, after the watermark closes it
        .option("checkpointLocation", checkpoint)
    )
    writer = writer.trigger(availableNow=True) if args.available_now \
        else writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

    query = writer.start(GOLD_PATH)
    print(f"Gold stream started. source={SILVER_PATH}\n target={GOLD_PATH}\n "
          f"window={args.window} slide={args.slide} watermark={args.watermark}")
    try:
        run_query(query, args.available_now, "gold")
    finally:
        spark.stop()
        print("Gold aggregation finished.")


if __name__ == "__main__":
    main()