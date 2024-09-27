import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count

formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%y/%m/%d %H:%M:%S")
log = logging.getLogger("Disease Data Analysis")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler("logs/disease/disease.log")
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def initialize_spark_session(app_name: str) -> SparkSession:
    """Initialize a SparkSession with the given application name."""
    log.info("Initializing SparkSession")

    return SparkSession.builder.appName(app_name).getOrCreate()


def load_dataset(spark: SparkSession, path: str) -> DataFrame:
    """Load the dataset from the given path into a DataFrame."""
    log.info(f"Loading dataset from path: {path}")

    return spark.read.csv(path, header=True, inferSchema=True)


def count_30_year_old_males_with_asthma(df: DataFrame) -> int:
    """Count the total number of 30-year-old men having Asthma."""
    log.info("Counting 30-year-old males with Asthma")

    return df.filter((col("Disease") == "Asthma") & (col("Age") == 30) & (col("Gender") == "Male")).count()


def count_females_with_hyperthyroidism_no_fever(df: DataFrame) -> int:
    """Count the total number of females with Hyperthyroidism with no Fever symptoms."""
    log.info("Counting females with Hyperthyroidism and no Fever symptoms")

    return df.filter(
        (col("Disease") == "Hyperthyroidism") & (col("Gender") == "Female") & (col("Fever") == "No")
    ).count()


def count_sinusitis_with_cough_fatigue(df: DataFrame) -> DataFrame:
    """Identify whether the Sinusitis with Cough and Fatigue symptoms is predominant for males or females."""
    log.info("Counting Sinusitis cases with Cough and Fatigue symptoms")

    return (
        df.filter((col("Disease") == "Sinusitis") & (col("Cough") == "Yes") & (col("Fatigue") == "Yes"))
        .groupBy("Gender")
        .agg(count("*").alias("Volume"))
    )


def identify_predominant_gender(df: DataFrame) -> str:
    """Identify the predominant gender for Sinusitis with Cough and Fatigue symptoms."""
    log.info("Identifying predominant gender for Sinusitis with Cough and Fatigue")
    predominant_gender = df.orderBy(col("Volume").desc()).first()

    return predominant_gender["Gender"]


def main():
    spark: SparkSession = initialize_spark_session("Disease Data Analysis")
    data_path: str = "data/disease/disease.csv"

    df: DataFrame = load_dataset(spark, data_path)
    df.show()

    asthma_30_male_count: int = count_30_year_old_males_with_asthma(df)
    log.info(f"Total number of 30-year-old Males having Asthma: {asthma_30_male_count}")

    hyperthyroidism_female_no_fever_count: int = count_females_with_hyperthyroidism_no_fever(df)
    log.info(f"Number of Females with Hyperthyroidism and No Fever symptoms: {hyperthyroidism_female_no_fever_count}")

    sinusitis_cough_fatigue_count_df: DataFrame = count_sinusitis_with_cough_fatigue(df)
    sinusitis_cough_fatigue_count_df.show()

    predominant_gender: str = identify_predominant_gender(sinusitis_cough_fatigue_count_df)
    log.info(f"The predominant gender for Sinusitis with Cough and Fatigue is: {predominant_gender}")

    spark.stop()
    log.info("Data analysis script finished")


if __name__ == "__main__":
    main()
