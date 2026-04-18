"""
Kafka producer — earlier version of the streaming ingestion.

Kept here for reference. The main pipeline uses file-based streaming because
it's simpler to run, but this is how you'd do it with Kafka if you needed
higher throughput or wanted a "real" message bus.

Works with kafka 3.x. Tested with the bitnami/kafka docker image.
"""

import argparse
import json
import logging
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        # durability settings — tune for your use case
        acks="all",
        retries=3,
        linger_ms=100,     # batch up to 100ms for throughput
        compression_type="snappy",
    )


def stream_parquet_to_kafka(
    source_path: str,
    topic: str,
    bootstrap_servers: str,
    interval_seconds: float = 0.01,
    key_column: str | None = "pickup_location_id",
) -> None:
    log.info("reading %s", source_path)
    df = pd.read_parquet(source_path)
    log.info("loaded %d rows", len(df))

    producer = build_producer(bootstrap_servers)
    sent = 0
    errors = 0

    def on_send_error(excp):
        nonlocal errors
        errors += 1
        log.error("send failed: %s", excp)

    try:
        for _, row in df.iterrows():
            record = row.to_dict()
            # make sure timestamps are json-serializable
            record["_ingested_at"] = datetime.utcnow().isoformat()

            key = str(record[key_column]) if key_column else None

            future = producer.send(topic, key=key, value=record)
            future.add_errback(on_send_error)

            sent += 1
            if sent % 1000 == 0:
                log.info("sent %d records (%d errors)", sent, errors)

            if interval_seconds:
                time.sleep(interval_seconds)

    except KeyboardInterrupt:
        log.info("stopped by user")
    finally:
        producer.flush(timeout=30)
        producer.close()
        log.info("done. sent=%d errors=%d", sent, errors)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--topic", default="taxi_trips")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--interval", type=float, default=0.01)
    parser.add_argument("--key-column", default="pickup_location_id",
                        help="column to use as kafka key (for partitioning)")
    args = parser.parse_args()

    try:
        stream_parquet_to_kafka(
            source_path=args.source,
            topic=args.topic,
            bootstrap_servers=args.bootstrap_servers,
            interval_seconds=args.interval,
            key_column=args.key_column,
        )
    except KafkaError as e:
        log.error("kafka error: %s", e)
        raise


if __name__ == "__main__":
    main()
