"""Processing words using Spark in cluster mode."""

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    lower,
    regexp_replace,
    split,
    substring,
    when,
)

formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%y/%m/%d %H:%M:%S")
log = logging.getLogger("Words Data Analysis")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler("logs/words/words.log")
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def create_spark_session(app_name: str) -> SparkSession:
    """Create and return a SparkSession."""
    log.debug("Creating SparkSession...")

    return SparkSession.builder.appName(app_name).getOrCreate()


def read_text_file(file_path: str, session: SparkSession) -> DataFrame:
    """Read a text file into a Spark DataFrame."""
    log.debug(f"Reading text file from {file_path} into a Spark DataFrame...")

    return session.read.text(file_path)


def explode_words(df: DataFrame) -> DataFrame:
    """Explode multiple words in a row into individual rows."""
    log.debug("Exploding words into multiple rows...")

    return df.select(explode(split(col("value"), "\s+")).alias("word"))


def count_words_starting_with_abs(df: DataFrame) -> int:
    """Count the number of words that start with 'abs'."""
    log.debug("Counting the number of words starting with 'abs'...")

    return df.filter(lower(col("word")).startswith("abs")).count()


def count_words_with_third_letter_o(df: DataFrame) -> int:
    """Count the number of words where the third letter is 'o'."""
    log.debug("Counting the number of words where the third letter is 'o'...")

    return df.filter(lower(substring(col("word"), 3, 1)) == "o").count()


def modify_words(df: DataFrame) -> DataFrame:
    """Modify words by replacing 'ou' with 'uou' if they end with 's'."""
    log.debug("Modifying words by replacing 'ou' with 'uou' if they end with 's'...")

    return df.withColumn(
        "new_word",
        when(
            lower(col("word")).endswith("s"), regexp_replace(col("word"), pattern="(?i)ou", replacement="uou")
        ).otherwise(col("word")),
    )


if __name__ == "__main__":
    spark = create_spark_session("WordsAnalysis")

    words: DataFrame = (
        read_text_file(file_path="data/words/words.txt", session=spark).transform(explode_words).transform(modify_words)
    )
    abs_count: int = count_words_starting_with_abs(words)
    o_count: int = count_words_with_third_letter_o(words)

    log.info(f"Number of words starting with 'abs': {abs_count}")
    log.info(f"Number of words where the third letter is 'o': {o_count}")

    spark.stop()
