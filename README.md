# NYC Taxi Real-Time Data Pipeline

An end-to-end streaming data pipeline I built to process NYC taxi trip data using PySpark, Airflow, and Snowflake. The pipeline ingests trip records, runs distributed transformations, validates data quality, loads to a warehouse, and powers a Streamlit dashboard.

I started this because I wanted to actually *use* the tools on my resume instead of just listing them. Turns out building a real pipeline teaches you way more than any tutorial.

## What it does

The pipeline simulates a streaming source (since the real NYC TLC feed is batch-only), runs it through a Spark processing layer, orchestrates everything with Airflow, and writes aggregates to Snowflake. There's a Streamlit dashboard on top that refreshes every 30 seconds.

Flow looks roughly like this:

```
NYC TLC Data  →  Kafka/File Stream  →  Spark Structured Streaming
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

A few design choices I want to call out:

**Medallion architecture (bronze/silver/gold)** — I went with this because it gives you clear separation between raw landing data, cleaned business entities, and aggregated facts. It's also how most real teams structure things, so it felt like the right pattern to learn.

**File-based streaming instead of Kafka** — The original plan was Kafka. I built it with Kafka first, then realized that for a personal project the operational overhead wasn't worth it. So I rewrote the ingestion to use Spark's `readStream` with the file source, which is what a lot of teams actually do for slow-moving streaming use cases. The Kafka version is still in `spark_jobs/kafka_version/` if you want to see it.

**Snowflake over Redshift/BigQuery** — Snowflake has a generous free trial and the Python connector is pleasant. Also it's what most of the jobs I'm applying to use.

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
│   ├── bronze_to_silver.py    # Cleaning + type coercion
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

You'll need Docker, Python 3.10+, and a Snowflake account (free trial is fine).

```bash
# Clone and set up
git clone https://github.com/Lokesh1566/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline
cp config/snowflake_config.example.yaml config/snowflake_config.yaml
# fill in your Snowflake creds in that file

# Install Python deps
pip install -r requirements.txt

# Pull sample data (about 150MB, one month of yellow taxi trips)
python scripts/download_tlc_data.py --year 2023 --month 06

# Set up Snowflake tables
python scripts/snowflake_loader.py --setup

# Spin up Airflow + Spark
docker-compose up -d

# Airflow UI is at localhost:8080 (admin/admin)
# Spark UI is at localhost:4040 when a job is running
```

See `docs/SETUP.md` for the full walkthrough. I hit some annoying issues with PySpark version mismatches on M1 Macs that I documented there.

## Results & benchmarks

Running on my laptop (M2 MacBook Pro, 16GB RAM) with a month of trip data (~3M rows):

| Stage | Rows | Runtime | Throughput |
|-------|------|---------|------------|
| Bronze ingestion | 3,104,267 | 1m 42s | ~30k rows/sec |
| Silver transform | 3,104,267 | 2m 18s | ~22k rows/sec |
| Gold aggregations | 3,104,267 → 8,412 | 47s | — |
| GE validation | 3,104,267 | 31s | ~100k rows/sec |

After partitioning silver by `pickup_date` and caching the dim_zone lookup, I got the silver stage down to 1m 34s. Detailed notes in `docs/BENCHMARKS.md`.

## Data quality

Great Expectations runs on the silver layer before anything hits gold. Current expectations:

- `trip_distance` must be > 0 and < 200 miles (saw some 10,000-mile rows in raw data, clearly sensor errors)
- `fare_amount` must be >= 0
- `passenger_count` must be between 0 and 6
- `pickup_datetime` must be before `dropoff_datetime`
- `pickup_location_id` must join to dim_zone

Rows that fail go to a quarantine table, they don't just get dropped. I lost about 0.3% of rows to quality issues, which felt reasonable.

## Dashboard

The Streamlit dashboard shows:

- Trip volume by hour (last 24h, rolling)
- Revenue by pickup borough
- Average trip duration and distance trends
- Top 10 busiest pickup zones
- Payment type distribution

It pulls directly from Snowflake's gold tables with a 30-second cache. Not the prettiest thing, but it works.

![dashboard screenshot](docs/dashboard.png)

## What I'd do differently / next steps

A few things I didn't get to that a production version would need:

- **Real Kafka**, not file-based streaming. I have the Kafka job working locally but pulled it from the main pipeline to keep setup simple.
- **dbt** for the gold layer transformations. Right now they're raw SQL which works but isn't testable.
- **Proper monitoring**. Airflow's email-on-failure is fine for personal use but I'd want Grafana + Prometheus for anything real.
- **Schema registry**. Schema changes would currently break things silently.
- **Better backfill strategy**. My DAG uses `catchup=True` but doesn't handle partial failures well.

## Troubleshooting

A few things that tripped me up and might trip you up too:

- **PySpark on Apple Silicon**: I had to set `JAVA_HOME` manually and use Java 11 (not 17). See SETUP.md.
- **Snowflake connection timeouts**: If you're on a VPN, the Snowflake python connector sometimes hangs. Setting `login_timeout=30` helped.
- **Airflow scheduler not picking up DAGs**: Restart the scheduler after changing DAG code. Hot reload is unreliable.

## Credits

Data from the [NYC TLC Trip Record Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Zone lookup CSV is also from TLC.

I borrowed the medallion architecture pattern from Databricks' documentation and the streaming simulator approach from a blog post by Daniel Beach (couldn't find the original link to credit, sorry).

## Contact

Questions or suggestions? Open an issue or email me — [lokeshreddye@iu.edu].

---

Built while applying to DS/ML roles. If you're a recruiter reading this, hi 👋
