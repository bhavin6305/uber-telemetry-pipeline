"""
inspect_delta.py - Quick inspection of any Delta table in the lakehouse (reused for Silver and Gold).

Usage:
  python inspect_delta.py                      # Bronze table
  python inspect_delta.py --path lakehouse/silver/uber_ride_events --rows 10
"""
import argparse

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from spark_utils import BRONZE_PATH, get_spark


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--path", default=BRONZE_PATH)
    p.add_argument("--rows", type=int, default=5)
    args = p.parse_args()

    spark = get_spark("inspect")
    df = spark.read.format("delta").load(args.path)

    print(f"\n=== {args.path} ===")
    print(f"Row count: {df.count():,}")
    print("\nSchema:")
    df.printSchema()

    print(f"First {args.rows} rows:")
    df.show(args.rows, truncate=60)

    if "kafka_partition" in df.columns:
        print("Per-partition check (max_offset + 1 should equal Kafka's end offset):")
        (df.groupBy("kafka_partition")
           .agg(F.count("*").alias("rows"), F.max("kafka_offset").alias("max_offset"))
           .orderBy("kafka_partition")
           .show())

    print("Delta transaction log (latest 5 commits):")
    (DeltaTable.forPath(spark, args.path).history(5)
        .select("version", "timestamp", "operation", "operationMetrics")
        .show(truncate=70))

    spark.stop()


if __name__ == "__main__":
    main()