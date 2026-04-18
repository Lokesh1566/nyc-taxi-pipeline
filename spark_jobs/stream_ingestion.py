"""
Structured streaming job that watches the raw/ directory and writes to bronze.

Bronze layer is intentionally close to source — minimal transformation,
just standardize column names and add metadata. The idea is that if we
mess up silver or gold we can always replay from bronze.
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType,
    DoubleType, TimestampType, StringType,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# Schema for yellow taxi trip data
# (pinning this explicitly — inferSchema on streaming source is flaky)
YELLOW_TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),  # yes, TLC uses double here
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("_ingested_at", StringType(), True),
])


def build_spark(app_name: str = "taxi-stream-ingestion") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # spark-sql is enough for local, but tweak these for actual cluster work
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.streaming.schemaInference", "false")
        # checkpoint dir is required for structured streaming
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints/bronze")
        .getOrCreate()
    )


def run_stream(spark: SparkSession, input_path: str, output_path: str,
               checkpoint_path: str) -> None:
    log.info("starting stream: %s -> %s", input_path, output_path)

    raw = (
        spark.readStream
        .schema(YELLOW_TAXI_SCHEMA)
        .option("maxFilesPerTrigger", 3)  # control throughput
        .parquet(input_path)
    )

    # minimal bronze transformation: rename to snake_case + add metadata
    bronze = (
        raw
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumn("_bronze_loaded_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        # partition by pickup date for downstream pruning
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
    )

    query = (
        bronze.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("pickup_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("stream started, query id: %s", query.id)
    log.info("awaiting termination — Ctrl+C to stop")
    query.awaitTermination()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw/streaming")
    parser.add_argument("--output", default="data/processed/bronze")
    parser.add_argument("--checkpoint", default="data/checkpoints/bronze")
    args = parser.parse_args()

    # make sure paths exist — spark won't create them itself for streaming
    for p in [args.input, args.output, args.checkpoint]:
        Path(p).mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    try:
        run_stream(spark, args.input, args.output, args.checkpoint)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
