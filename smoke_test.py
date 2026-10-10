"""
smoke_test.py - Verifies Java + PySpark + Delta Lake work together before Milestone 3.
Run:  python smoke_test.py
Expected last line:  SMOKE TEST PASSED (delta rows: 5)
"""
import shutil

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

PATH = "/tmp/delta_smoke"

builder = (
    SparkSession.builder.master("local[2]")
    .appName("smoke")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "1g")
    .config("spark.ui.showConsoleProgress", "false")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

shutil.rmtree(PATH, ignore_errors=True)
spark.range(5).write.format("delta").mode("overwrite").save(PATH)
count = spark.read.format("delta").load(PATH).count()
spark.stop()

assert count == 5, f"expected 5 rows, got {count}"
print(f"SMOKE TEST PASSED (delta rows: {count})")