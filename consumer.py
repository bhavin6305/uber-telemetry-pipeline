"""
consumer.py - Bronze ingestion: Kafka topic -> raw Delta table.

Bronze = an immutable, append-only copy of exactly what arrived, plus lineage metadata.
No parsing, no cleaning, no deduplication here. That is Silver's job. Keeping Bronze raw
means we can always replay and re-derive Silver/Gold if our transformation logic changes.

Usage:
  python consumer.py --available-now     # drain what is in the topic, then exit (great for testing)
  python consumer.py                     # run continuously (Ctrl+C to stop)
"""
import argparse
import os

from pyspark.sql import functions as F

from spark_utils import BRONZE_PATH, CHECKPOINT_ROOT, get_spark


def parse_args():
    p = argparse.ArgumentParser(description="Kafka -> Bronze Delta streaming ingestion.")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default="uber-ride-events")
    p.add_argument("--available-now", action="store_true",
                   help="Process everything currently in the topic, then stop.")
    p.add_argument("--trigger-seconds", type=int, default=10, help="Micro-batch interval (continuous mode)")
    p.add_argument("--max-offsets", type=int, default=5000, help="Max Kafka records per micro-batch")
    p.add_argument("--starting-offsets", default="earliest",
                   help="Only used on the very first run; afterwards the checkpoint decides.")
    return p.parse_args()


def main():
    args = parse_args()
    checkpoint = os.path.join(CHECKPOINT_ROOT, "bronze")

    spark = get_spark("bronze-ingest", with_kafka=True)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets)  # backpressure: bounds batch size
        .option("failOnDataLoss", "true")                  # fail loudly if offsets vanished
        .load()
    )

    bronze = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_payload"),  # untouched JSON string
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),         # when the broker received it
        F.current_timestamp().alias("ingestion_ts"),         # when Spark ingested it (per micro-batch)
    )

    writer = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)  # offsets + progress live here -> restart-safe
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

    query = writer.start(BRONZE_PATH)
    print(f"Bronze stream started. table={BRONZE_PATH}\n checkpoint={checkpoint}")

    try:
        if args.available_now:
            query.awaitTermination()
        else:
            # Poll with a timeout so Ctrl+C is handled promptly (a bare awaitTermination blocks in the JVM).
            while query.isActive:
                query.awaitTermination(15)
                p = query.lastProgress
                if p:
                    print(f"batch={p['batchId']} rows={p['numInputRows']} "
                          f"in/s={p.get('inputRowsPerSecond', 0):.1f}")
    except KeyboardInterrupt:
        print("Stopping stream...")
        query.stop()
    finally:
        spark.stop()
        print("Bronze ingestion finished.")


if __name__ == "__main__":
    main()