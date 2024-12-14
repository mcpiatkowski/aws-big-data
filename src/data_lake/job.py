"""Data Lake ingestion job."""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def create_spark_session(app_name: str) -> SparkSession:
    """Creates and configures a Spark session with AWS S3 support."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .getOrCreate()
    )


def get_abalone_schema() -> StructType:
    """Returns the schema definition for the abalone dataset."""
    return StructType(
        [
            StructField("Type", StringType(), True),
            StructField("LongestShell", DoubleType(), True),
            StructField("Diameter", DoubleType(), True),
            StructField("Height", DoubleType(), True),
            StructField("WholeWeight", DoubleType(), True),
            StructField("ShuckedWeight", DoubleType(), True),
            StructField("VisceraWeight", DoubleType(), True),
            StructField("ShellWeight", DoubleType(), True),
            StructField("Rings", IntegerType(), True),
        ]
    )


def read_csv_with_schema(spark: SparkSession, file_path: str, schema: Optional[StructType] = None) -> DataFrame:
    """Reads a CSV file using the specified schema and returns a Spark DataFrame."""
    reader = spark.read.option("header", "true")
    if schema:
        reader = reader.schema(schema)
    return reader.csv(file_path)


def write_to_data_lake(df: DataFrame, output_path: str, mode: str = "overwrite") -> None:
    """Writes the DataFrame to S3 in Parquet format with the specified writing mode."""
    df.write.mode(mode).parquet(output_path)


def main() -> None:
    """Orchestrates the data lake ingestion process."""
    spark = create_spark_session("Peex Lake Data Ingestion")

    input_path = "data/abalone/abalone.csv"
    output_path = "s3a://peexlakestack-bronzelayerbucket608a9fe7-o21a6q6onjfi/abalone/"

    schema = get_abalone_schema()
    df = read_csv_with_schema(spark, input_path, schema)

    write_to_data_lake(df, output_path)


if __name__ == "__main__":
    main()
