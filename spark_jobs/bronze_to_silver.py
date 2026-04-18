"""
Bronze -> Silver transformation.

Bronze is raw-ish. Silver is clean, conformed, and typed correctly.
Main things happening here:
  - drop obvious garbage rows (negative distances, impossible timestamps)
  - compute trip_duration_minutes
  - join against the zone lookup to get borough + zone name
  - cast types properly
  - quarantine bad rows instead of silently dropping
"""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-to-silver")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def apply_quality_filters(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split df into (clean, quarantined) based on quality rules.

    Quarantining instead of dropping — it's useful to be able to look at the
    bad data later to understand why it's bad. Plus if our rules are wrong,
    we haven't lost anything.
    """
    # individual rule columns so we can see exactly why each row failed
    dq = (
        df
        .withColumn("_dq_neg_distance", F.col("trip_distance") < 0)
        .withColumn("_dq_zero_distance", F.col("trip_distance") == 0)
        .withColumn("_dq_huge_distance", F.col("trip_distance") > 200)
        .withColumn("_dq_neg_fare", F.col("fare_amount") < 0)
        .withColumn("_dq_bad_passenger",
                    (F.col("passenger_count") < 0) | (F.col("passenger_count") > 9))
        .withColumn("_dq_null_times",
                    F.col("pickup_datetime").isNull() | F.col("dropoff_datetime").isNull())
        .withColumn("_dq_reverse_time",
                    F.col("dropoff_datetime") <= F.col("pickup_datetime"))
    )

    dq_cols = [c for c in dq.columns if c.startswith("_dq_")]
    # a row is bad if any dq check fails
    dq = dq.withColumn(
        "_is_bad_row",
        F.expr(" OR ".join(dq_cols))
    )

    bad = dq.filter(F.col("_is_bad_row"))
    good = dq.filter(~F.col("_is_bad_row")).drop(*dq_cols, "_is_bad_row")

    return good, bad


def enrich_with_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """
    Broadcast-join trip data with the zone lookup. Zone lookup is tiny (~260 rows)
    so broadcasting is the right call.
    """
    zones_bc = F.broadcast(zones)

    # join for pickup
    pickup_zones = zones_bc.select(
        F.col("LocationID").alias("pickup_location_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    # join for dropoff
    dropoff_zones = zones_bc.select(
        F.col("LocationID").alias("dropoff_location_id"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )

    return (
        trips
        .join(pickup_zones, on="pickup_location_id", how="left")
        .join(dropoff_zones, on="dropoff_location_id", how="left")
    )


def transform(bronze: DataFrame, zones: DataFrame) -> tuple[DataFrame, DataFrame]:
    # normalize column names — the streaming ingestion job does this, but
    # if bronze was populated directly from raw TLC parquet the columns
    # are still in their original tpep_ / CamelCase form.
    rename_map = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "rate_code_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "Airport_fee": "airport_fee",
    }
    for old, new in rename_map.items():
        if old in bronze.columns and new not in bronze.columns:
            bronze = bronze.withColumnRenamed(old, new)

    # first compute derived fields
    with_derived = (
        bronze
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0
        )
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        # nullability cleanup — some rows have null passenger_count, default to 1
        .withColumn("passenger_count",
                    F.when(F.col("passenger_count").isNull(), F.lit(1))
                     .otherwise(F.col("passenger_count")).cast("int"))
    )

    clean, quarantine = apply_quality_filters(with_derived)

    # enrich the clean rows with zone info
    enriched = enrich_with_zones(clean, zones)

    # final column selection + ordering
    silver = enriched.select(
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "pickup_hour",
        "pickup_dow",
        "passenger_count",
        "trip_distance",
        "trip_duration_minutes",
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "dropoff_location_id",
        "dropoff_borough",
        "dropoff_zone",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "congestion_surcharge",
        F.current_timestamp().alias("_silver_loaded_at"),
    )

    return silver, quarantine


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", default="data/processed/bronze")
    parser.add_argument("--zones", default="data/raw/taxi_zone_lookup.csv")
    parser.add_argument("--silver-output", default="data/processed/silver")
    parser.add_argument("--quarantine-output", default="data/processed/quarantine")
    args = parser.parse_args()

    spark = build_spark()

    log.info("reading bronze from %s", args.bronze)
    bronze = spark.read.parquet(args.bronze)
    bronze_count = bronze.count()
    log.info("bronze row count: %d", bronze_count)

    log.info("reading zone lookup from %s", args.zones)
    zones = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(args.zones)
    )

    silver, quarantine = transform(bronze, zones)

    # write silver — partition by pickup_date for downstream pruning
    log.info("writing silver to %s", args.silver_output)
    (silver.write
     .mode("overwrite")
     .partitionBy("pickup_date")
     .parquet(args.silver_output))

    silver_count = spark.read.parquet(args.silver_output).count()
    log.info("silver row count: %d (dropped %d bad rows)",
             silver_count, bronze_count - silver_count)

    log.info("writing quarantine to %s", args.quarantine_output)
    (quarantine.write
     .mode("overwrite")
     .parquet(args.quarantine_output))

    quarantine_count = quarantine.count()
    bad_pct = 100 * quarantine_count / bronze_count if bronze_count else 0
    log.info("quarantined %d rows (%.2f%% of bronze)", quarantine_count, bad_pct)

    spark.stop()


if __name__ == "__main__":
    main()
