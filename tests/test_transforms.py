"""
Unit tests for the transform logic.

Uses pytest + pyspark with a local spark session fixture.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession

from spark_jobs.bronze_to_silver import apply_quality_filters, enrich_with_zones
from spark_jobs.silver_to_gold import build_hourly_fact, build_top_routes


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder
        .master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture
def sample_bronze(spark):
    """Minimal bronze-shaped dataframe for testing."""
    data = [
        # good row
        (1, datetime(2023, 6, 1, 10, 0), datetime(2023, 6, 1, 10, 30),
         1.0, 3.5, 1, 161, 236, 1, 15.0, 2.5, 0.5, 3.0, 0.0, 0.3, 21.3, 2.5, 0.0),
        # bad: negative distance
        (1, datetime(2023, 6, 1, 11, 0), datetime(2023, 6, 1, 11, 20),
         1.0, -1.0, 1, 161, 236, 1, 10.0, 2.5, 0.5, 2.0, 0.0, 0.3, 15.3, 2.5, 0.0),
        # bad: dropoff before pickup
        (1, datetime(2023, 6, 1, 12, 30), datetime(2023, 6, 1, 12, 0),
         1.0, 2.0, 1, 161, 236, 1, 12.0, 2.5, 0.5, 2.0, 0.0, 0.3, 17.3, 2.5, 0.0),
        # bad: passenger_count of 15
        (1, datetime(2023, 6, 1, 13, 0), datetime(2023, 6, 1, 13, 30),
         15.0, 2.0, 1, 161, 236, 1, 12.0, 2.5, 0.5, 2.0, 0.0, 0.3, 17.3, 2.5, 0.0),
    ]
    cols = [
        "vendor_id", "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "rate_code_id",
        "pickup_location_id", "dropoff_location_id", "payment_type",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "congestion_surcharge",
        "airport_fee",
    ]
    return spark.createDataFrame(data, cols)


@pytest.fixture
def sample_zones(spark):
    data = [
        (161, "Manhattan", "Midtown Center", "Yellow Zone"),
        (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
        (132, "Queens", "JFK Airport", "Airports"),
    ]
    return spark.createDataFrame(
        data, ["LocationID", "Borough", "Zone", "service_zone"]
    )


class TestQualityFilters:
    def test_good_row_passes(self, spark, sample_bronze):
        # add the derived columns that silver expects
        from pyspark.sql import functions as F
        bronze = sample_bronze.withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("dropoff_datetime") -
             F.unix_timestamp("pickup_datetime")) / 60.0
        )
        good, bad = apply_quality_filters(bronze)
        assert good.count() == 1, "only one row should pass all checks"
        assert bad.count() == 3

    def test_quarantined_rows_not_lost(self, spark, sample_bronze):
        from pyspark.sql import functions as F
        bronze = sample_bronze.withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("dropoff_datetime") -
             F.unix_timestamp("pickup_datetime")) / 60.0
        )
        good, bad = apply_quality_filters(bronze)
        assert good.count() + bad.count() == bronze.count()


class TestZoneEnrichment:
    def test_zones_joined_correctly(self, spark, sample_bronze, sample_zones):
        from pyspark.sql import functions as F
        df = sample_bronze.filter(F.col("trip_distance") > 0).limit(1)
        enriched = enrich_with_zones(df, sample_zones)
        result = enriched.collect()[0]
        assert result["pickup_borough"] == "Manhattan"
        assert result["dropoff_borough"] == "Manhattan"

    def test_missing_zone_becomes_null(self, spark, sample_zones):
        # location id 999 doesn't exist in zones
        data = [(999, 999, 1.0, 10.0, 20.0, datetime(2023, 1, 1), datetime(2023, 1, 1, 0, 10))]
        cols = ["pickup_location_id", "dropoff_location_id",
                "trip_distance", "fare_amount", "total_amount",
                "pickup_datetime", "dropoff_datetime"]
        df = spark.createDataFrame(data, cols)
        enriched = enrich_with_zones(df, sample_zones)
        result = enriched.collect()[0]
        assert result["pickup_borough"] is None


class TestHourlyFact:
    def test_aggregation_produces_rows(self, spark):
        from pyspark.sql import functions as F
        data = [
            (datetime(2023, 6, 1, 10, 0), datetime(2023, 6, 1, 10, 30),
             "Manhattan", 3.5, 30.0, 15.0, 2),
        ] * 10
        cols = ["pickup_datetime", "dropoff_datetime", "pickup_borough",
                "trip_distance", "trip_duration_minutes", "fare_amount",
                "passenger_count"]
        df = spark.createDataFrame(data, cols).withColumn(
            "total_amount", F.col("fare_amount") * 1.2
        )
        result = build_hourly_fact(df).collect()
        assert len(result) == 1
        assert result[0]["trip_count"] == 10


class TestTopRoutes:
    def test_rank_within_borough(self, spark):
        data = [
            ("Manhattan", "Midtown", "Manhattan", "Uptown", 100, 20.0, 15.0),
            ("Manhattan", "Midtown", "Manhattan", "Downtown", 50, 15.0, 10.0),
            ("Manhattan", "SoHo", "Queens", "Astoria", 30, 25.0, 20.0),
            ("Brooklyn", "DUMBO", "Manhattan", "Midtown", 200, 30.0, 25.0),
        ]
        # need to re-create as silver-shape with count-based data
        # actually build_top_routes expects raw trip rows, not pre-aggregated
        # so we need to expand these back out
        rows = []
        for row in data:
            for _ in range(row[4]):  # repeat by trip_count
                rows.append((row[0], row[1], row[2], row[3], row[5], row[6]))
        cols = ["pickup_borough", "pickup_zone", "dropoff_borough",
                "dropoff_zone", "total_amount", "trip_duration_minutes"]
        df = spark.createDataFrame(rows, cols)
        result = build_top_routes(df, top_n=5).collect()
        # should have 4 route entries
        assert len(result) == 4
        # check ranking: within Manhattan, Midtown->Uptown (100) ranks 1
        manhattan_rank1 = [r for r in result if r["pickup_borough"] == "Manhattan"
                          and r["rank_within_borough"] == 1][0]
        assert manhattan_rank1["dropoff_zone"] == "Uptown"
