"""
Silver -> Gold aggregations.

Gold layer is what business users / dashboards query. Heavily aggregated,
pre-joined, optimized for read.

Five gold tables:
  1. fct_trips_hourly — hour x borough, volume + revenue metrics
  2. fct_trips_daily — daily trip-level facts, slightly denormalized
  3. agg_top_routes — most popular pickup->dropoff pairs
  4. agg_zone_stats — per-zone trip volume w/ 7-day rolling avg
  5. agg_payment_breakdown — payment type mix by day

Window functions are used for the rolling avg and for ranking top routes.
"""

import argparse
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver-to-gold")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def build_hourly_fact(silver: DataFrame) -> DataFrame:
    """Hourly aggregations by pickup borough + hour of day."""
    return (
        silver
        .groupBy(
            F.date_trunc("hour", "pickup_datetime").alias("pickup_hour_ts"),
            "pickup_borough",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration_min"),
            F.avg("fare_amount").alias("avg_fare"),
            F.sum("passenger_count").alias("total_passengers"),
        )
        .withColumn("avg_revenue_per_trip",
                    F.col("total_revenue") / F.col("trip_count"))
    )


def build_daily_fact(silver: DataFrame) -> DataFrame:
    """Daily rollup — one row per day, for trend charts."""
    return (
        silver
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.sum("tip_amount").alias("total_tips"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration_min"),
            F.countDistinct("pickup_location_id").alias("unique_pickup_zones"),
        )
    )


def build_top_routes(silver: DataFrame, top_n: int = 100) -> DataFrame:
    """
    Top pickup -> dropoff pairs by trip count.

    Uses a window function to rank within each pickup borough so we get the
    top routes per borough, not just the global top 100.
    """
    route_stats = (
        silver
        .groupBy(
            "pickup_borough",
            "pickup_zone",
            "dropoff_borough",
            "dropoff_zone",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("total_amount").alias("avg_fare"),
            F.avg("trip_duration_minutes").alias("avg_duration_min"),
        )
    )

    w = Window.partitionBy("pickup_borough").orderBy(F.desc("trip_count"))
    ranked = (
        route_stats
        .withColumn("rank_within_borough", F.row_number().over(w))
        .filter(F.col("rank_within_borough") <= top_n)
    )
    return ranked


def build_zone_stats(silver: DataFrame) -> DataFrame:
    """
    Per-zone daily stats with a 7-day rolling average of trip count.

    This is the classic "window over a range of days" pattern. The trick is
    that our window is based on a date, not just row ordering, so we need
    rangeBetween over a unix-timestamp'd column.
    """
    daily_per_zone = (
        silver
        .groupBy("pickup_date", "pickup_borough", "pickup_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("revenue"),
        )
    )

    # convert date to days-since-epoch so we can use rangeBetween
    with_epoch = daily_per_zone.withColumn(
        "_date_as_int",
        F.datediff("pickup_date", F.lit("1970-01-01"))
    )

    w = (
        Window
        .partitionBy("pickup_zone")
        .orderBy("_date_as_int")
        .rangeBetween(-6, 0)  # 7 day window (today + 6 prior)
    )

    return (
        with_epoch
        .withColumn("trip_count_7d_avg", F.avg("trip_count").over(w))
        .withColumn("revenue_7d_avg", F.avg("revenue").over(w))
        .drop("_date_as_int")
    )


def build_payment_breakdown(silver: DataFrame) -> DataFrame:
    """Daily payment type mix. Useful for dashboard pie charts."""
    # TLC payment_type codes:
    # 1=credit card, 2=cash, 3=no charge, 4=dispute, 5=unknown, 6=voided
    payment_labels = F.when(F.col("payment_type") == 1, "credit_card") \
        .when(F.col("payment_type") == 2, "cash") \
        .when(F.col("payment_type") == 3, "no_charge") \
        .when(F.col("payment_type") == 4, "dispute") \
        .otherwise("other")

    return (
        silver
        .withColumn("payment_method", payment_labels)
        .groupBy("pickup_date", "payment_method")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("revenue"),
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", default="data/processed/silver")
    parser.add_argument("--gold-output", default="data/processed/gold")
    args = parser.parse_args()

    spark = build_spark()

    log.info("reading silver from %s", args.silver)
    silver = spark.read.parquet(args.silver)

    # cache since we use it 5 times below
    silver.cache()
    silver_count = silver.count()
    log.info("silver row count: %d", silver_count)

    out = args.gold_output

    log.info("building fct_trips_hourly...")
    build_hourly_fact(silver).write.mode("overwrite").parquet(f"{out}/fct_trips_hourly")

    log.info("building fct_trips_daily...")
    build_daily_fact(silver).write.mode("overwrite").parquet(f"{out}/fct_trips_daily")

    log.info("building agg_top_routes...")
    build_top_routes(silver).write.mode("overwrite").parquet(f"{out}/agg_top_routes")

    log.info("building agg_zone_stats...")
    build_zone_stats(silver).write.mode("overwrite").parquet(f"{out}/agg_zone_stats")

    log.info("building agg_payment_breakdown...")
    build_payment_breakdown(silver).write.mode("overwrite").parquet(f"{out}/agg_payment_breakdown")

    silver.unpersist()
    spark.stop()
    log.info("done.")


if __name__ == "__main__":
    main()
