# NYC Taxi Real Time Data Pipeline

Processes NYC yellow taxi trip records through a medallion lakehouse (bronze, silver, gold), validates data quality, loads aggregates to Snowflake, and powers a Streamlit dashboard. PySpark, Airflow, Great Expectations, Snowflake.

I started this because I wanted to actually *use* the tools on my resume instead of just listing them. Turns out building a real pipeline teaches you way more than any tutorial.

**Status:** Runs end to end locally. Bronze, silver, gold, Snowflake, and dashboard all verified on a full month of TLC yellow taxi data, 3.3M rows.

## Flow

The real NYC TLC feed is batch only, so the pipeline simulates a streaming source, processes it with Spark, runs quality validation, and loads aggregates to Snowflake. A Streamlit dashboard sits on top of the warehouse.

```
NYC TLC Data  ->  File Stream  ->  Spark Structured Streaming
                                            |
                                  Bronze -> Silver -> Gold
                                            |
                                  Great Expectations (QC)
                                            |
                                  Snowflake Warehouse
                                            |
                                  Streamlit Dashboard
```

Airflow orchestrates the batch backfill jobs and the daily aggregation runs. Streaming is a long running Spark job that Airflow health checks but doesn't trigger directly.

## Why I built it this way

Medallion architecture because it separates raw landing data, cleaned business entities, and aggregated facts. Also how most real teams structure things.

File based streaming instead of Kafka. Original plan was Kafka and I built it that way first, then realized the operational overhead wasn't worth it for a solo project. Rewrote ingestion to use Spark's `readStream` with the file source, which is what a lot of teams actually do for slower moving streaming. The Kafka version is still in `spark_jobs/kafka_version/` if you want to look at it.

Snowflake over Redshift or BigQuery. Generous free trial, the Python connector is pleasant, and it's what most of the jobs I'm applying to use. The pipeline design doesn't really depend on the warehouse. Only `snowflake_loader.py` would change.

Great Expectations because I tried writing custom validation functions first and quickly realized I was reinventing a worse version of GE. Switched after about a day of pain.

## Project structure

```
nyc-taxi-pipeline/
├── dags/                      # Airflow DAGs
│   ├── taxi_ingestion_dag.py
│   ├── taxi_transform_dag.py
│   └── taxi_quality_dag.py
├── spark_jobs/
│   ├── stream_ingestion.py    # Structured Streaming job
│   ├── bronze_to_silver.py    # cleaning, type coercion, quarantine
│   ├── silver_to_gold.py      # aggregations, window functions
│   └── kafka_version/         # earlier Kafka based version
├── scripts/
│   ├── generate_stream.py     # simulates streaming from TLC data
│   ├── snowflake_loader.py
│   └── download_tlc_data.py
├── sql/
│   ├── ddl_snowflake.sql
│   └── analytics_queries.sql
├── tests/
│   ├── test_transforms.py
│   └── great_expectations/
├── dashboards/
│   └── streamlit_app.py
├── config/
│   ├── pipeline_config.yaml
│   └── snowflake_config.example.yaml
├── docs/
│   ├── ARCHITECTURE.md
│   ├── SETUP.md
│   └── BENCHMARKS.md
├── docker-compose.yml
├── requirements.txt
└── Makefile
```

## Quick start

You'll need Python 3.11, Java 11 for PySpark, and a Snowflake account. Free trial is fine. Docker is optional, only needed for the Airflow runs.

```bash
git clone https://github.com/Lokesh1566/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Snowflake creds
cp config/snowflake_config.example.yaml config/snowflake_config.yaml
# edit that file

# sample data, about 52MB, one month of yellow taxi trips
python scripts/download_tlc_data.py --year 2023 --month 06

# run it end to end
mkdir -p data/processed/bronze
cp data/raw/yellow_tripdata_2023-06.parquet data/processed/bronze/
python spark_jobs/bronze_to_silver.py
python spark_jobs/silver_to_gold.py
python -m tests.great_expectations.run_validation --dataset silver
python scripts/snowflake_loader.py --setup --load

# dashboard
streamlit run dashboards/streamlit_app.py
```

Full walkthrough in `docs/SETUP.md`. I hit some annoying PySpark version mismatches on Apple Silicon that I documented there.

## Results and benchmarks

Apple Silicon MacBook Pro, 16GB RAM, one month of yellow taxi data:

| Stage | Input rows | Output rows | Runtime |
|-------|-----------:|------------:|--------:|
| Bronze ingestion | 3,307,234 | 3,307,234 | ~2s (parquet read) |
| Silver transform | 3,307,234 | 3,231,373 | ~13s |
| Gold aggregations | 3,231,373 | ~16,500 across 5 tables | ~30s |
| GE validation | 3,231,373 | (checks only) | <5s |
| Snowflake load | 16,500 | 16,500 | ~30s |

Silver transform dropped 75,861 rows, 2.29% of bronze, to quarantine. More detail in `docs/BENCHMARKS.md`.

## Data quality findings

Three things the validation layer surfaced in the June 2023 data while I was building it.

One row with a fare over $100,000. Almost certainly a meter error or a bulk charter that got past TLC's own validation.

Two trips lasting longer than 48 hours. Forgotten meters, probably. One ran about 72 hours.

1,506 rows with a pickup borough of `"N/A"`. Turned out to be a quirk of TLC's zone lookup CSV, where `LocationID 264` maps to a placeholder zone.

None of these were pipeline bugs. I tuned the expectations to accept these known source data patterns while keeping the rules tight enough to catch real corruption. Rows failing the hard rules (negative distances, null timestamps, timestamps in reverse order, impossible passenger counts) go to a quarantine directory instead of getting silently dropped, so they can be looked at later. This is why I like quarantine over hard drops. If my rules turn out to be wrong, nothing is lost.

## Current expectations

Silver layer checks, in `tests/great_expectations/run_validation.py`:

* `trip_distance` between 0 and 200 miles
* `fare_amount` between 0 and $1,000,000, a generous cap meant to catch corruption only
* `passenger_count` between 0 and 9, nulls allowed and defaulted to 1 upstream
* `pickup_datetime` and `dropoff_datetime` must be non null
* `trip_duration_minutes` between 0 and 10,080, one week
* `pickup_borough` must be in the known borough set, including `N/A` for zone 264

The quarantine rules in `bronze_to_silver.py` are stricter. Negative distances, zero distances, timestamps in reverse order, and so on.

## Dashboard

Trip volume by hour on a rolling 24 hours, revenue by pickup borough, average trip duration and distance trends, the 10 busiest pickup zones, and payment type distribution.

Pulls straight from Snowflake's gold tables with a 30 second cache. Not the prettiest thing, but functional.

## What I'd do differently

Real Kafka instead of file based streaming. The
