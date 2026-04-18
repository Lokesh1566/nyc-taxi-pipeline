# Kafka version

This directory has the earlier Kafka-based version of the streaming pieces.
The main pipeline uses file-based streaming (see `spark_jobs/stream_ingestion.py`
and `scripts/generate_stream.py`) — this is kept around as reference and because
it's what you'd want if you were doing this with real-volume streaming data.

## Files

- `producer.py` — reads TLC parquet and sends records to Kafka
- `consumer.py` — Spark structured streaming consumer that reads from Kafka
  and writes to the bronze layer

## Running it

You need a Kafka broker running. Easiest way for local:

```bash
# add this to docker-compose.yml (or run separately)
docker run -d \
    -p 9092:9092 \
    --name kafka \
    -e KAFKA_CFG_NODE_ID=0 \
    -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
    -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093 \
    bitnami/kafka:3.6
```

Create the topic:
```bash
docker exec kafka kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic taxi_trips \
    --partitions 3 --replication-factor 1
```

Run the producer:
```bash
python spark_jobs/kafka_version/producer.py \
    --source data/raw/yellow_tripdata_2023-06.parquet \
    --topic taxi_trips \
    --interval 0.01
```

Run the consumer (note the packages flag — Spark needs the Kafka connector JAR):
```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    spark_jobs/kafka_version/consumer.py \
    --topic taxi_trips \
    --bootstrap localhost:9092
```

## Why I didn't use this as the default

Two reasons:

1. **Operational overhead.** Running a broker for a personal project felt like
   too much. Every time I wanted to test something I had to make sure Kafka was up,
   topics existed, etc.

2. **First-time reviewer experience.** Someone cloning this repo shouldn't need
   to install Docker + Kafka just to see the pipeline work. The file-based
   streaming works end-to-end with just Python and Spark.

For a real production pipeline you'd absolutely use Kafka (or Kinesis, or
Pub/Sub depending on cloud). But the processing logic is the same — only the
ingestion source changes.
