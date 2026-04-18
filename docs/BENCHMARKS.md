# Benchmarks & Tuning Notes

Performance numbers and what I learned tuning this thing.

## Test environment

- MacBook Pro M2, 16GB RAM, 8 cores
- Local Spark (no cluster, `--master local[4]`)
- PySpark 3.5.0, Python 3.10
- Dataset: June 2023 yellow taxi trips, 3,104,267 rows

## Baseline numbers

First pass, no tuning, defaults everywhere:

| Stage | Runtime | Notes |
|-------|---------|-------|
| Bronze ingestion (batch) | 1m 42s | read parquet + rename + partition-write |
| Silver transform | 3m 10s | includes zone join + quality filters |
| Gold aggregations | 1m 20s | 5 aggregate tables |
| GE validation | 31s | pandas-based, reads silver |
| Snowflake load | 45s | pandas write_pandas, 5 tables |
| **Total** | **~7m 30s** | |

## After tuning

| Stage | Runtime | Change | Notes |
|-------|---------|--------|-------|
| Bronze ingestion | 1m 42s | same | not the bottleneck |
| Silver transform | **1m 34s** | -50% | broadcast join + caching |
| Gold aggregations | **47s** | -41% | silver cached before use |
| GE validation | 31s | same | already fast |
| Snowflake load | 45s | same | bounded by network |
| **Total** | **~5m 19s** | -29% | |

## What worked

### 1. Broadcasting the zone lookup (biggest win)

The zone lookup CSV is ~260 rows. Spark was still doing a sort-merge join
because I didn't tell it otherwise.

Before:
```python
trips.join(zones, on="pickup_location_id", how="left")
# 2m 10s for silver transform
```

After:
```python
trips.join(F.broadcast(zones), on="pickup_location_id", how="left")
# 1m 34s for silver transform
```

Spark's auto-broadcast threshold defaults to 10MB. My zones df is tiny
but was being read from a CSV with inferSchema on, and was getting a bloated
internal representation. Explicit broadcast forces the right plan.

### 2. Caching silver before gold aggregations

The gold job runs 5 aggregations, each reading silver. Without caching,
parquet got read 5 times.

```python
silver = spark.read.parquet("data/processed/silver")
silver.cache()
silver.count()   # materialize the cache

# now all 5 aggregations hit memory, not disk
build_hourly_fact(silver).write.parquet(...)
build_daily_fact(silver).write.parquet(...)
# ...
```

Took gold runtime from 1m 20s to 47s. Don't forget to `unpersist()` at the end.

### 3. Adaptive Query Execution

```python
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

Didn't measure this one in isolation but the plans looked cleaner in the
Spark UI and shuffle sizes dropped.

### 4. Partition by pickup_date

Silver gets partitioned by `pickup_date` on write. Cost: slightly slower
write, more small files. Benefit: any downstream query filtering on date
prunes partitions automatically, which is most dashboard queries.

## What didn't work

### Increasing shuffle partitions

I tried `spark.sql.shuffle.partitions = 200` (the default, for clusters)
and also 4 (very low). For this data size on this machine, 16 was the
sweet spot — higher created scheduling overhead, lower created stragglers.

Lesson: the default of 200 is way too high for local/small-cluster work.

### Using a Spark->Snowflake connector

Tried the official Snowflake Spark connector (spark-snowflake). Ran into
JAR compatibility issues with Spark 3.5. Gave up after an hour of fighting
with Maven coordinates and went with the simpler pandas+write_pandas
approach. For gold data (small), this is fine. For silver you'd want the
connector working.

### Increasing driver memory beyond 4GB

No measurable difference from 4g to 8g. For the silver/gold size of this
pipeline, 4g is plenty.

## Throughput

Rough throughput numbers (after tuning):

| Stage | Rows/sec |
|-------|----------|
| Parquet read + rename (bronze) | ~30,000 |
| Silver transform + zone join | ~33,000 |
| Gold aggregations (input rows) | ~66,000 |
| GE validation | ~100,000 |

## Scaling notes

I tested with 3 months of data (~9M rows) as a larger test:

| Stage | 1 month | 3 months | Scaling |
|-------|---------|----------|---------|
| Silver | 1m 34s | 4m 12s | ~linear |
| Gold | 47s | 2m 05s | ~linear |

Both scale roughly linearly with input size, which is what you want.
Nothing pathologically bad.

For really big data (say, a year of trips, ~40M rows), you'd want:
- A real cluster, not local mode
- More aggressive partitioning (by year+month, not just date)
- Incremental silver builds instead of full refresh

## Snowflake-side perf

On the Snowflake free trial (X-Small warehouse):

| Table | Rows | Load time | Query time (dashboard) |
|-------|------|-----------|------------------------|
| FCT_TRIPS_HOURLY | 8,412 | 12s | ~200ms |
| FCT_TRIPS_DAILY | 30 | 5s | ~150ms |
| AGG_TOP_ROUTES | 340 | 6s | ~150ms |
| AGG_ZONE_STATS | 7,560 | 9s | ~250ms |
| AGG_PAYMENT_BREAKDOWN | 120 | 5s | ~150ms |

Nothing here is big enough to stress Snowflake. The X-Small warehouse
handles all of it with caching on most queries. A production version
with multi-year data would benefit from clustering keys (already set in
DDL for FCT_TRIPS_HOURLY and AGG_ZONE_STATS).

## Things I'd benchmark next

- End-to-end streaming latency (time from file landed to gold table updated).
  Haven't measured this properly yet.
- Cost per run on Snowflake. My trial hasn't burned noticeable credits yet
  but with real-volume data it would matter.
- Impact of Snowflake clustering keys once tables are big enough (> 1GB).
