# NYC Taxi Real-Time Data Pipeline

End-to-end data pipeline that processes NYC yellow taxi trip records through a medallion lakehouse architecture (bronze → silver → gold), validates data quality, loads aggregates to Snowflake, and powers a Streamlit dashboard. Built with PySpark, Airflow, Great Expectations, and Snowflake.

I started this because I wanted to actually *use* the tools on my resume instead of just listing them. Turns out building a real pipeline teaches you way more than any tutorial.

**Status:** Runs end-to-end locally. Bronze → silver → gold → Snowflake → dashboard verified on a full month of TLC yellow taxi data (3.3M rows).

## Flow

The pipeline simulates a streaming source (since the real NYC TLC feed is batch-only), processes it with Spark, runs quality validation, and loads aggregates to Snowflake. A Streamlit dashboard sits on top of the warehouse.

```
NYC TLC Data  →  File Stream  →  Spark Structured Streaming
                                        ↓
                              Bronze → Silver → Gold
                                        ↓
                              Great Expectations (QC)
                                        ↓
                              Snowflake Warehouse
                                        ↓
                              Streamlit Dashboard
```

Airflow orchestrates the batch backfill jobs and the daily aggregation runs. Streaming is handled by a long-running Spark job that Airflow health-checks but doesn't directly trigger.

## Why I built it this way

A few design choices worth calling out:

**Medallion architecture (bronze/silver/gold)** — gives clear separation between raw landing data, cleaned business entities, and aggregated facts. It's also how most real teams structure things.

**File-based streaming instead of Kafka** — original plan was Kafka. I built it with Kafka first, then realized the operational overhead wasn't worth it for a solo project. Rewrote ingestion to use Spark's `readStream` with the file source, which is what a lot of teams actually do for slower-moving streaming use cases. The Kafka version is still in `spark_jobs/kafka_version/` for reference.

**Snowflake over Redshift/BigQuery** — generous free trial and the Python connector is pleasant. Also it's what most of the jobs I'm applying to use. The pipeline design doesn't really depend on which warehouse — only `snowflake_loader.py` would change.

**Great Expectations for data quality** — I tried writing custom validation functions first and quickly realized I was reinventing a worse version of GE. Switched after about a day of pain.

## Project structure

```
nyc-taxi-pipeline/
├── dags/                      # Airflow DAGs
│   ├── taxi_ingestion_dag.py
│   ├── taxi_transform_dag.py
│   └── taxi_quality_dag.py
├── spark_jobs/
│   ├── stream_ingestion.py    # Structured Streaming job
│   ├── bronze_to_silver.py    # Cleaning + type coercion + quarantine
│   ├── silver_to_gold.py      # Aggregations, window functions
│   └── kafka_version/         # Earlier Kafka-based version
├── scripts/
│   ├── generate_stream.py     # Simulates streaming from TLC data
│   ├── snowflake_loader.py
│   └── download_tlc_data.py
├── sql/
│   ├── ddl_snowflake.sql      # Table creation
│   └── analytics_queries.sql  # Business queries for dashboard
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

You'll need Python 3.11, Java 11 (for PySpark), and a Snowflake account (free trial is fine). Docker is optional — only needed for the Airflow-based runs.

```bash
# Clone and set up
git clone https://github.com/Lokesh1566/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline

python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Fill in Snowflake creds
cp config/snowflake_config.example.yaml config/snowflake_config.yaml
# edit that file with your credentials

# Pull sample data (~52MB, one month of yellow taxi trips)
python scripts/download_tlc_data.py --year 2023 --month 06

# Run the pipeline end to end
mkdir -p data/processed/bronze
cp data/raw/yellow_tripdata_2023-06.parquet data/processed/bronze/
python spark_jobs/bronze_to_silver.py
python spark_jobs/silver_to_gold.py
python -m tests.great_expectations.run_validation --dataset silver
python scripts/snowflake_loader.py --setup --load

# Launch the dashboard
streamlit run dashboards/streamlit_app.py
```

See `docs/SETUP.md` for the full walkthrough. I hit some annoying issues with PySpark version mismatches on Apple Silicon that I documented there.

## Results & benchmarks

Running on my laptop (Apple Silicon MacBook Pro, 16GB RAM) with one month of yellow taxi data:

| Stage | Input rows | Output rows | Runtime |
|-------|-----------:|------------:|--------:|
| Bronze ingestion | 3,307,234 | 3,307,234 | ~2s (parquet read) |
| Silver transform | 3,307,234 | 3,231,373 | ~13s |
| Gold aggregations | 3,231,373 | ~16,500 across 5 tables | ~30s |
| GE validation | 3,231,373 | — | <5s |
| Snowflake load | 16,500 | 16,500 | ~30s |

Silver transform dropped 75,861 rows (2.29% of bronze) to the quarantine layer. Detailed notes in `docs/BENCHMARKS.md`.

## Data quality findings

The validation layer surfaced three interesting patterns in the June 2023 yellow taxi data during development:

- **One row with a fare over $100,000** — almost certainly a meter error or bulk charter that slipped through TLC's own validation.
- **Two trips lasting longer than 48 hours** — likely forgotten meters. One clocked in around 72 hours.
- **1,506 rows with a pickup borough of `"N/A"`** — turned out to be a known quirk of TLC's zone lookup CSV, where `LocationID 264` is mapped to a placeholder zone.

None of these indicated pipeline bugs. I tuned the expectations to accept these known source-data patterns while keeping the rules tight enough to catch genuine corruption. Rows that fail the hard rules (negative distances, null timestamps, reverse-ordered timestamps, impossible passenger counts) get routed to a quarantine directory instead of being silently dropped, so they can be reviewed later. This is a good example of why I like the quarantine pattern over hard drops — if my rules turn out to be wrong, nothing is lost.

## Current expectations

Silver layer checks (see `tests/great_expectations/run_validation.py`):

- `trip_distance` between 0 and 200 miles
- `fare_amount` between 0 and $1,000,000 (generous cap to catch corruption only)
- `passenger_count` between 0 and 9 (nulls allowed, defaulted to 1 upstream)
- `pickup_datetime` and `dropoff_datetime` must be non-null
- `trip_duration_minutes` between 0 and 10,080 (one week)
- `pickup_borough` must be in the known borough set (including `N/A` for zone 264)

Rules enforced at the quarantine stage (in `bronze_to_silver.py`) are stricter — negative distances, zero distances, reverse-ordered timestamps, etc. all get routed to quarantine.

## Dashboard

The Streamlit dashboard shows:

- Trip volume by hour (last 24h, rolling)
- Revenue by pickup borough
- Average trip duration and distance trends
- Top 10 busiest pickup zones
- Payment type distribution

It pulls directly from Snowflake's gold tables with a 30-second cache. Not the prettiest thing, but functional.

## What I'd do differently / next steps

A few things I didn't get to that a production version would need:

- **Real Kafka**, not file-based streaming. The Kafka job works locally but I pulled it from the main pipeline to keep setup simple.
- **dbt** for the gold layer transformations. Right now they're raw PySpark which works but isn't as testable as proper dbt models.
- **Proper monitoring**. Airflow's email-on-failure is fine for personal use but I'd want Grafana + Prometheus for anything real.
- **Schema registry**. Schema changes would currently break things silently.
- **Better backfill strategy**. The DAG doesn't handle partial failures gracefully yet.
- **Incremental silver/gold builds**. Current approach is full refresh — fine for this data size but wouldn't scale to years of data without CDC.

## Troubleshooting

A few things that tripped me up and might trip you up too:

- **PySpark on Apple Silicon**: I had to install Java 11 specifically (not 17) via Homebrew and set `JAVA_HOME` manually. `docs/SETUP.md` has the full steps.
- **Python 3.12 incompatibility**: Airflow 2.8 doesn't support 3.12, hence the 3.11 requirement. If you already have 3.12 as your default, create the venv with `python3.11 -m venv venv` explicitly.
- **Snowflake connection timeouts**: If you're on a VPN, the Python connector sometimes hangs. Setting `login_timeout=30` in the config helped.
- **Airflow scheduler not picking up DAGs**: Restart the scheduler after changing DAG code. Hot reload is unreliable.

## Credits

Data from the [NYC TLC Trip Record Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Zone lookup CSV is also from TLC.

Medallion architecture pattern borrowed from [Databricks' lakehouse documentation](https://www.databricks.com/glossary/medallion-architecture).

## Contact

Questions or suggestions? Open an issue or email me: lokeshreddye@iu.edu

---

Built while applying to DS/ML/DE roles. If you're a recruiter reading this, hi 👋
