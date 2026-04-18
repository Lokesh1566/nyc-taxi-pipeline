"""
Kafka -> Spark structured streaming consumer.

Alternative to stream_ingestion.py for when you're using a real Kafka
source instead of the file-based stream.

Needs the spark-sql-kafka jar at runtime. Submit with:

  spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      spark_jobs/kafka_version/consumer.py
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType,
    DoubleType, StringType, TimestampType,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# schema matches the producer's json payload
TAXI_JSON_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),    # parsed downstream
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("_ingested_at", StringType(), True),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("taxi-kafka-consumer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.schemaInference", "false")
        .getOrCreate()
    )


def consume_kafka(spark: SparkSession, topic: str, bootstrap: str,
                  output_path: str, checkpoint_path: str) -> None:
    log.info("subscribing to topic=%s", topic)

    # read raw bytes from kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")  # change to "earliest" for replay
        .option("failOnDataLoss", "false")
        .load()
    )

    # kafka gives us value as bytes — cast and parse json
    parsed = (
        raw
        .selectExpr("CAST(key AS STRING) as kafka_key",
                    "CAST(value AS STRING) as json_value",
                    "timestamp as kafka_timestamp",
                    "partition as kafka_partition",
                    "offset as kafka_offset")
        .withColumn("data", F.from_json("json_value", TAXI_JSON_SCHEMA))
        .select("kafka_key", "kafka_timestamp", "kafka_partition",
                "kafka_offset", "data.*")
    )

    # cast date strings to proper timestamps
    bronze = (
        parsed
        .withColumn("pickup_datetime",
                    F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_datetime",
                    F.to_timestamp("tpep_dropoff_datetime"))
        .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("_bronze_loaded_at", F.current_timestamp())
    )

    query = (
        bronze.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("pickup_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("streaming query started, id=%s", query.id)
    query.awaitTermination()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="taxi_trips")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--output", default="data/processed/bronze")
    parser.add_argument("--checkpoint", default="data/checkpoints/bronze_kafka")
    args = parser.parse_args()

    spark = build_spark()
    try:
        consume_kafka(spark, args.topic, args.bootstrap,
                      args.output, args.checkpoint)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
