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

Snowflake
