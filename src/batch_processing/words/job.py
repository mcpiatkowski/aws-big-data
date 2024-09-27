"""Processing words using Spark in cluster mode."""

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


def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession.

    :param app_name: The name of the Spark application.
    :return: SparkSession object
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_text_file(file_path: str, session: SparkSession) -> DataFrame:
    """
    Read a text file into a Spark DataFrame.

    :param file_path: The path to the text file.
    :param session: The SparkSession object.
    :return: DataFrame containing the text file data.
    """
    return session.read.text(file_path)


def explode_words(df: DataFrame) -> DataFrame:
    """
    Explode multiple words in a row into individual rows.

    :param df: Input DataFrame.
    :return: DataFrame with exploded words.
    """
    return df.select(explode(split(col("value"), "\s+")).alias("word"))


def count_words_starting_with_abs(df: DataFrame) -> int:
    """
    Count the number of words that start with 'abs'.

    :param df: DataFrame containing words.
    :return: Number of words starting with 'abs'.
    """
    return df.filter(lower(col("word")).startswith("abs")).count()


def count_words_with_third_letter_o(df: DataFrame) -> int:
    """
    Count the number of words where the third letter is 'o'.

    :param df: Input DataFrame.
    :return: Number of words with the third letter 'o'.
    """
    return df.filter(lower(substring(col("word"), 3, 1)) == "o").count()


def modify_words(df: DataFrame) -> DataFrame:
    """
    Modify words by replacing 'ou' with 'uou' if they end with 's'.

    :param df: Input DataFrame.
    :return: DataFrame with modified words.
    """
    return df.withColumn(
        "new_word",
        when(lower(col("word")).endswith("s"), regexp_replace(col("word"), "(?i)ou", "uou")).otherwise(col("word")),
    )


if __name__ == "__main__":
    spark = create_spark_session("WordsAnalysis")

    words = read_text_file("data/english_words/words.txt", spark).transform(explode_words).transform(modify_words)
    abs_count = count_words_starting_with_abs(words)
    o_count = count_words_with_third_letter_o(words)

    spark.stop()
