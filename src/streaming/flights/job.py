"""Streaming pipeline for flight data analysis from Kafka using PySpark."""

from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, from_json, window, to_date, lit
from pyspark.sql.streaming import StreamingQuery


def create_spark_session(app_name: str) -> SparkSession:
    """Creates and configures a Spark session.

    When running in Jupyter add:
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")

    When running in the terminal:
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 job.py
    """
    return SparkSession.builder.appName(app_name).config("spark.log.level", "WARN").getOrCreate()


def create_kafka_stream(spark: SparkSession, server: str, topic: str) -> DataFrame:
    """Creates a streaming DataFrame from Kafka source."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", server)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def create_flights_schema() -> StructType:
    """Creates the schema for flight data."""
    return StructType(
        [
            StructField("flight_number", StringType(), nullable=True),
            StructField("destination", StringType(), nullable=True),
            StructField("scheduled_departure", TimestampType(), nullable=True),
            StructField("scheduled_arrival", TimestampType(), nullable=True),
            StructField("status", StringType(), nullable=True),
        ]
    )


def parse_flight_data(df: DataFrame, schema: StructType) -> DataFrame:
    """Parses JSON flight data using the provided schema."""
    return df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


def filter_flights_by_date(df: DataFrame, target_date: date) -> DataFrame:
    """Filters flights for a specific date and selects relevant columns."""
    return df.filter(to_date(col("scheduled_departure")) == lit(target_date)).select(
        "flight_number", "scheduled_departure", "scheduled_arrival", "status"
    )


def create_time_window_count(df: DataFrame, window_duration: str, slide_duration: str) -> DataFrame:
    """Creates a windowed count of all flights."""
    return (
        df.groupBy(window(col("scheduled_departure"), window_duration, slide_duration)).count().alias("total_flights")
    )


def create_landed_flights_count(df: DataFrame, window_duration: str) -> DataFrame:
    """Creates a windowed count of landed flights."""
    return (
        df.filter(col("status") == "L")
        .groupBy(window(col("scheduled_departure"), window_duration))
        .count()
        .orderBy(col("window").desc())
        .alias("landed_flights")
    )


def output_results(df: DataFrame, output_mode: str) -> StreamingQuery:
    """Starts a streaming query with console output."""
    return df.writeStream.outputMode(output_mode).format("console").start()


def run_streaming_pipeline(
    kafka_server: str, kafka_topic: str, target_date: date, app_name: str = "FlightSparkStreaming"
) -> None:
    """Runs the complete streaming pipeline for flight data analysis."""
    spark: SparkSession = create_spark_session(app_name)

    try:
        kafka_stream: DataFrame = create_kafka_stream(spark, kafka_server, kafka_topic)

        schema: StructType = create_flights_schema()
        parsed_stream: DataFrame = parse_flight_data(kafka_stream, schema)

        filtered_flights: DataFrame = filter_flights_by_date(parsed_stream, target_date)
        landed_flights: DataFrame = create_landed_flights_count(parsed_stream, window_duration="10 minutes")
        windowed_flights: DataFrame = create_time_window_count(
            parsed_stream, window_duration="5 minutes", slide_duration="1 minute"
        )

        # Start streaming queries
        output_results(filtered_flights, output_mode="append")
        output_results(landed_flights, output_mode="complete")
        output_results(windowed_flights, output_mode="complete")

        spark.streams.awaitAnyTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    run_streaming_pipeline(kafka_topic="flights", kafka_server="localhost:9092", target_date=date(2024, 10, 21))
