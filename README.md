# NYC Taxi Pipeline

Yellow taxi trip records through a medallion lakehouse, bronze to silver to gold, validated with Great Expectations, aggregates loaded into Snowflake, Streamlit dashboard on top. PySpark and Airflow doing the work.

I built it because I was tired of having Spark and Airflow on my resume without having used either on anything real.

Runs end to end on my laptop. Verified on a full month of TLC data, 3.3M rows.

## How it works

TLC publishes in batch, not as a stream, so `generate_stream.py` fakes a streaming source by dripping files into a directory. Spark picks them up with `readStream`, writes bronze, then bronze to silver to gold. Great Expectations validates silver. Aggregates go to Snowflake. Dashboard reads from there.

```
TLC files -> file stream -> Spark structured streaming
                                   |
                         bronze -> silver -> gold
                                   |
                         Great Expectations
                                   |
                              Snowflake
                                   |
                              Streamlit
```

Airflow runs the backfills and the daily aggregations. The streaming job runs on its own and Airflow just health checks it.

I started with Kafka. Built the whole ingestion path on it, then admitted the operational overhead made no sense for something one person runs on a laptop. Rewrote it on Spark's file source, which is what plenty of teams use anyway when the data isn't moving that fast. Old version is still in `spark_jobs/kafka_version/`.

Also wrote my own validation functions before switching to Great Expectations. Got about a day in before it was obvious I was building a worse version of a thing that already exists.

Snowflake because the free trial is generous and the Python connector doesn't fight you. Nothing here depends on it. Swap `snowflake_loader.py` and you're on Redshift or BigQuery.

## Running it

Python 3.11, Java 11, a Snowflake account. Docker only if you want the Airflow runs.

```bash
git clone https://github.com/Lokesh1566/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp config/snowflake_config.example.yaml config/snowflake_config.yaml
# put your creds in that file

python scripts/download_tlc_data.py --year 2023 --month 06   # ~52MB

mkdir -p data/processed/bronze
cp data/raw/yellow_tripdata_2023-06.parquet data/processed/bronze/
python spark_jobs/bronze_to_silver.py
python spark_jobs/silver_to_gold.py
python -m tests.great_expectations.run_validation --dataset silver
python scripts/snowflake_loader.py --setup --load

streamlit run dashboards/streamlit_app.py
```

Java 11 specifically, not 17, or PySpark won't start on Apple Silicon. Homebrew, then set `JAVA_HOME` yourself. Python 3.11 specifically because Airflow 2.8 doesn't do 3.12. If you're on a VPN and Snowflake just hangs, set `login_timeout=30`. And restart the Airflow scheduler after you touch a DAG, hot reload lies to you.

More in `docs/SETUP.md`.

## Numbers

MacBook Pro, M series, 16GB. One month of data.

| Stage | In | Out | Time |
|---|---|---|---|
| Bronze ingestion | 3,307,234 | 3,307,234 | ~2s |
| Silver transform | 3,307,234 | 3,231,373 | ~13s |
| Gold aggregations | 3,231,373 | ~16,500 across 5 tables | ~30s |
| GE validation | 3,231,373 | checks only | <5s |
| Snowflake load | 16,500 | 16,500 | ~30s |

75,861 rows quarantined at silver, 2.29% of bronze. `docs/BENCHMARKS.md` has the rest.

## What the data actually looked like

Some things validation caught in June 2023 that turned out to be real, not bugs in my code.

A fare over $100,000. One row. Meter error or a bulk charter that TLC's own checks let through.

Two trips over 48 hours, one around 72. Somebody forgot to end the meter.

1,506 rows with pickup borough `"N/A"`. Chased this one for a while assuming I'd broken a join. It's TLC's zone lookup CSV, `LocationID 264` maps to a placeholder.

So the expectations are looser than you'd write from scratch, because the source data has known weirdness in it. `fare_amount` caps at $1,000,000, which is absurd, but it's there to catch corruption and nothing else. `trip_distance` 0 to 200 miles. `passenger_count` 0 to 9, nulls allowed and defaulted to 1 upstream. Both timestamps non null. Duration under a week. Borough in the known set, `N/A` included.

Harder rules live in `bronze_to_silver.py` and those send rows to quarantine instead of dropping them. Negative distances, zero distances, dropoff before pickup. Quarantine rather than drop because my rules might be wrong and I'd rather find out later with the data still sitting there.

## Dashboard

Trip volume by hour, revenue by borough, duration and distance trends, busiest 10 zones, payment types. Reads gold tables from Snowflake with a 30 second cache. Not pretty.

## Not done

The gold transformations are raw PySpark. They should be dbt models, they'd be testable.

No schema registry, so a schema change breaks things quietly, which is the bad way.

Full refresh every run. Fine at one month. Wouldn't survive years of data without CDC.

Backfills don't recover from partial failure yet.

Monitoring is Airflow emailing me. Real version wants Grafana.

## Credits

[NYC TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), zone lookup CSV theirs as well. Medallion pattern from [Databricks](https://www.databricks.com/glossary/medallion-architecture).

Issues or email, redfylokesh@gmail.com
