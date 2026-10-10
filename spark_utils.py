"""
spark_utils.py - Shared Spark session factory and lakehouse paths for every pipeline stage.

Keeping this in one place means Bronze, Silver and Gold jobs are configured identically.
"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

SPARK_VERSION = "3.5.3"  # must match the pyspark pin in requirements.txt
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"

# Absolute paths, so jobs behave the same regardless of the directory you launch them from.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BRONZE_PATH = os.path.join(BASE_DIR, "lakehouse", "bronze", "uber_ride_events")
SILVER_PATH = os.path.join(BASE_DIR, "lakehouse", "silver", "uber_ride_events")
GOLD_PATH = os.path.join(BASE_DIR, "lakehouse", "gold", "active_rides_by_zone")
CHECKPOINT_ROOT = os.path.join(BASE_DIR, "checkpoints")


def get_spark(app_name, with_kafka=False, driver_memory="1g", shuffle_partitions=4):
    """Build a local-mode SparkSession with Delta Lake (and optionally the Kafka connector)."""
    builder = (
        SparkSession.builder.master("local[2]")  # 2 threads = the 2 cores of our Codespace
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", driver_memory)
        # Default is 200 shuffle partitions, which is wasteful (and slow) in local mode.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        # Parse timestamps deterministically, independent of the machine's timezone.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    extra_packages = [KAFKA_PACKAGE] if with_kafka else []
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


