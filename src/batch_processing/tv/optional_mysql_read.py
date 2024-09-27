"""
This script demonstrates how to read data from a MySQL database and a JSON file using Spark.
It also shows how to join the data and collect the result.
"""

import os

from pyspark.sql import DataFrame, Row, SparkSession


def create_spark_session() -> SparkSession:
    """Create and return a SparkSession with MySQL configurations."""

    return SparkSession.builder.appName("mysql-spark").config("spark.jars", "mysql-connector-j-9.0.0.jar").getOrCreate()


def get_mysql_connection_properties() -> dict[str, str]:
    """Retrieve MySQL connection properties from environment variables."""

    return {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver",
    }


def read_mysql_data(spark: SparkSession, jdbc_url: str, table: str, properties: dict) -> DataFrame:
    """Read data from MySQL database using Spark."""

    return spark.read.jdbc(url=jdbc_url, table=table, properties=properties)


def read_json_data(spark: SparkSession, file_path: str) -> DataFrame:
    """Read JSON data from a file using Spark."""

    return spark.read.json(file_path, multiLine=True)


def join_and_collect_data(df1: DataFrame, df2: DataFrame) -> list[Row]:
    """Join two DataFrames and collect the result."""

    return df1.select("id", "name").join(df2.select("id", "original_name"), "id", "left").collect()


def main() -> None:
    """Main function to orchestrate the data processing workflow."""

    spark: SparkSession = create_spark_session()

    jdbc_url: str = "jdbc:mysql://localhost:3306/aws_big_data"
    connection_properties: dict[str, str] = get_mysql_connection_properties()

    mysql_df: DataFrame = read_mysql_data(spark, jdbc_url, "tv_shows", connection_properties)
    json_data: DataFrame = read_json_data(spark, "data/tvs/tvs.json")

    result: list[Row] = join_and_collect_data(mysql_df, json_data)

    print(result[0:1])

    spark.stop()


if __name__ == "__main__":
    main()
