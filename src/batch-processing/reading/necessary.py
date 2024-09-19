"""Spark application for batch module."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import logging

log = logging.getLogger("Aws Big Data")


def get_spark_session():
    """Get spark session."""
    log.info("Creating spark session...")

    return SparkSession.builder.appName("ReadingData").getOrCreate()


def read_data(session: SparkSession, path: str):
    """Read data."""
    log.info("Reading data...")

    return session.read.json(path, multiLine=True)


def log_stats(data):
    """Log basic stats about the data."""
    log.info(f"""
    Number of rows: {data.count()}
    Number of columns: {len(data.columns)}
    """)


def get_cancelled_creators(data):
    """Retrieve all names of created_by with the status Cancelled."""
    log.info("Retrieving cancelled creators...")

    return data.filter(col("status") == "Canceled").select(explode("created_by.name").alias("creator_name")).distinct()


def get_popular_countries(data):
    """Retrieve all origin_country with popularity higher than 5.0."""
    log.info("Retrieving popular countries...")

    return data.filter(col("popularity") > 5.0).select(explode("origin_country").alias("country")).distinct()


def get_short_series(data):
    """Retrieve all names of series with the number_of_episodes less than 100."""
    log.info("Retrieving short series...")

    return data.filter(col("number_of_episodes") < 100).select("name")


def write_to_parquet(data, path: str) -> None:
    """Write data frame to parquet file."""
    log.info(f"Writing data to {path}...")
    data.write.mode("overwrite").parquet(path)


if __name__ == "__main__":
    spark = get_spark_session()
    series = read_data(spark, "data/tvs/tvs.json")

    log_stats(series)

    cancelled_creators = get_cancelled_creators(series)
    popular_countries = get_popular_countries(series)
    short_series = get_short_series(series)

    write_to_parquet(cancelled_creators, "data/transformed/cancelled_creators")
    write_to_parquet(popular_countries, "data/transformed/popular_countries")
    write_to_parquet(short_series, "data/transformed/short_series")

    spark.stop()
