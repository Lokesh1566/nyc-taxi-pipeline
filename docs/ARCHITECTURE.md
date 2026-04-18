# Architecture

Notes on how the pieces fit together and why I made the choices I did.

## High level

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ TLC parquet     │────▶│ stream generator │────▶│ landing dir     │
│ (batch source)  │     │ (python script)  │     │ (data/raw/      │
└─────────────────┘     └──────────────────┘     │  streaming/)    │
                                                  └────────┬────────┘
                                                           │
                                           ┌───────────────▼──────────────┐
                                           │ Spark Structured Streaming   │
                                           │ (stream_ingestion.py)        │
                                           └───────────────┬──────────────┘
                                                           │
                                                  ┌────────▼────────┐
                                                  │ BRONZE          │
                                                  │ (raw + metadata)│
                                                  └────────┬────────┘
                                                           │
                                ┌──────────────────────────┴──────────────────┐
                                │ bronze_to_silver.py                          │
                                │  - type coercion                             │
                                │  - quality filters (quarantine bad rows)     │
                                │  - zone enrichment (broadcast join)          │
                                └──────────────────────────┬──────────────────┘
                                                           │
                                                 ┌─────────▼─────────┐       ┌──────────────┐
                                                 │ SILVER             │──────▶│ QUARANTINE   │
                                                 │ (clean + enriched) │       │ (bad rows)   │
                                                 └─────────┬─────────┘       └──────────────┘
                                                           │
                                         ┌─────────────────▼─────────────────┐
                                         │ Great Expectations validation      │
                                         │ (fail pipeline if rules broken)    │
                                         └─────────────────┬─────────────────┘
                                                           │
                                    ┌──────────────────────▼──────────────────────┐
                                    │ silver_to_gold.py                            │
                                    │  - fct_trips_hourly  (groupBy)               │
                                    │  - fct_trips_daily   (groupBy)               │
                                    │  - agg_top_routes    (window rank)           │
                                    │  - agg_zone_stats    (7-day rolling window)  │
                                    │  - agg_payment_mix                           │
                                    └──────────────────────┬──────────────────────┘
                                                           │
                                                  ┌────────▼────────┐
                                                  │ GOLD            │
                                                  │ (aggregates)    │
                                                  └────────┬────────┘
                                                           │
                                                  ┌────────▼────────┐
                                                  │ Snowflake       │
                                                  │ (DDL + load)    │
                                                  └────────┬────────┘
                                                           │
                                                  ┌────────▼────────┐
                                                  │ Streamlit       │
                                                  │ dashboard       │
                                                  └─────────────────┘
```

## Medallion layers

**Bronze** — raw data with minimal changes. Just column renames (snake_case),
metadata columns (`_bronze_loaded_at`, `_source_file`), and partitioning by
`pickup_date`. The idea: bronze is cheap to replay from, so if silver or gold
has bugs, we just rerun downstream and the fix propagates.

**Silver** — the clean, trustworthy version. Type coercion, derived columns
(trip duration, pickup hour), zone enrichment via a broadcast join. This is
also where quality filters run — bad rows get written to a quarantine
directory instead of being dropped.

**Gold** — aggregations and denormalized tables optimized for BI queries.
Five tables, one per "widget" on the dashboard. These are small (thousands
to low millions of rows) and written to Snowflake.

## Why Spark (and not just pandas)?

For the size of data I'm working with (~3M rows/month), pandas would technically
work. I used Spark for two reasons:

1. It's what the roles I'm applying to actually use. Building this with
   pandas would defeat the purpose of the project.
2. The architecture scales. If I swap the file stream for a real Kafka topic
   and point it at a year of data instead of a month, nothing in the logic
   changes — just the infrastructure.

## Why structured streaming over a batch job?

I wanted the pipeline to reflect how real streaming systems work: stateful
checkpoints, exactly-once guarantees, incremental processing. Structured
Streaming gives you all of that with a DataFrame API that's almost identical
to batch, which makes the code easy to test.

The trade-off is extra complexity for the bronze layer — you have to manage
the checkpoint directory, handle schema evolution carefully, and Spark's
file source has some edge cases (it needs atomic file moves, which is why
the generator writes to `.tmp` first and renames).

## Why Airflow?

For orchestration, alerting, and retry semantics. The streaming job runs
continuously outside of Airflow — Airflow just monitors it via the freshness
DAG. The transform DAG handles the daily silver→gold→snowflake flow.

I considered Prefect and Dagster. Both are arguably nicer to use, but
Airflow is what most of the job listings want.

## Why Snowflake (not Redshift or BigQuery)?

Practical reasons:
- Snowflake's free trial gives you $400 of credits, enough to run this demo
  for months
- The python connector + `write_pandas` is painless
- Separation of storage and compute matters for the gold layer pattern

I'd use BigQuery if I were doing this on GCP, or Redshift on AWS. The
pipeline design doesn't really depend on which warehouse — only
`snowflake_loader.py` would change.

## Data quality philosophy

Three layers of defense:

1. **Schema pinning** — all Spark reads use explicit schemas. Schema drift
   in the source breaks things loudly, not silently.
2. **Row-level quarantine** — bad rows don't get dropped, they get moved to
   `data/processed/quarantine/` so we can investigate later. Dropping data
   is a last resort.
3. **Great Expectations** — runs against silver before gold, and against
   gold before Snowflake loads. Fails the pipeline if rules break.

## What I left out (intentionally)

- **CDC / true incremental processing.** The silver and gold jobs do full
  refresh on each run. For this data size (low millions/month) it's fine
  and way simpler. Real incremental would need MERGE statements and
  watermark tracking.
- **Data lineage tool (OpenLineage / Marquez).** Cool but adds operational
  weight for a solo project.
- **dbt.** The gold transformations are in PySpark, not dbt. For a team
  I'd split — Spark for streaming + silver, dbt for gold aggregations.
  Solo, one tool is simpler.
- **Dead letter queues.** Quarantine directory serves as a poor man's DLQ.
  In Kafka-land you'd have a real one.
- **Schema registry.** Same reasoning — overkill for solo dev.
