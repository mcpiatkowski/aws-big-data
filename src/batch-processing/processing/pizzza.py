"""
This module processes pizza sales data using PySpark.
It provides functions to analyze pizza orders, ingredients, and sales categories.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count
from dataclasses import dataclass


@dataclass
class PizzaData:
    """
    A data class representing pizza-related data frames.

    Attributes:
        order_details (DataFrame): Contains details of individual pizza orders.
        orders (DataFrame): Contains information about overall orders.
        pizza_types (DataFrame): Contains information about different pizza types.
        pizzas (DataFrame): Contains information about individual pizzas.
    """

    order_details: DataFrame
    orders: DataFrame
    pizza_types: DataFrame
    pizzas: DataFrame


def create_spark_session() -> SparkSession:
    """
    Create and return a SparkSession for pizza sales analysis.
    """
    return SparkSession.builder.appName("PizzaSales").getOrCreate()


def read_csv_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read CSV data from the specified file path.
    """
    return spark.read.csv(file_path, encoding="latin1", header=True)


def load_pizza_data(spark: SparkSession) -> PizzaData:
    """
    Load all pizza-related data into a PizzaData instance.
    """
    return PizzaData(
        order_details=read_csv_data(spark, "data/pizza_sales/order_details.csv"),
        orders=read_csv_data(spark, "data/pizza_sales/orders.csv"),
        pizza_types=read_csv_data(spark, "data/pizza_sales/pizza_types.csv"),
        pizzas=read_csv_data(spark, "data/pizza_sales/pizzas.csv"),
    )


def count_cali_ckn_pizzas(pizza_data: PizzaData, date: str) -> int:
    """
    Count the number of California Chicken pizzas ordered on a specific date.
    """
    return (
        pizza_data.order_details.join(pizza_data.orders, "order_id")
        .filter((col("date") == date) & (col("pizza_id").like("cali_ckn%")))
        .agg(count("*").alias("cali_ckn_count"))
        .collect()[0]["cali_ckn_count"]
    )


def get_pizza_ingredients(pizza_data: PizzaData, date: str, time: str) -> str:
    """
    Get the ingredients of a pizza ordered on a specific date and time.
    """
    return (
        pizza_data.order_details.join(pizza_data.orders, "order_id")
        .join(pizza_data.pizzas, "pizza_id")
        .join(pizza_data.pizza_types, "pizza_type_id")
        .filter((col("date") == date) & (col("time") == time))
        .select("ingredients")
        .collect()[0]["ingredients"]
    )


def get_most_sold_category(pizza_data: PizzaData, start_date: str, end_date: str) -> str:
    """
    Get the most sold pizza category between two dates.
    """
    return (
        pizza_data.order_details.join(pizza_data.orders, "order_id")
        .join(pizza_data.pizzas, "pizza_id")
        .join(pizza_data.pizza_types, "pizza_type_id")
        .filter(col("date").between(start_date, end_date))
        .groupBy("category")
        .agg(count("*").alias("category_count"))
        .orderBy(col("category_count").desc())
        .select("category")
        .first()["category"]
    )


def main():
    """
    Main function to execute pizza sales analysis.
    """
    spark: SparkSession = create_spark_session()
    pizza_data: PizzaData = load_pizza_data(spark)

    # 1. How many cali_ckn pizzas were ordered on 2015-01-04?
    cali_ckn_count: int = count_cali_ckn_pizzas(pizza_data, "2015-01-04")
    print(f"Number of cali_ckn pizzas ordered on 2015-01-04: {cali_ckn_count}")

    # 2. What ingredients does the pizza ordered on 2015-01-02 at 18:27:50 have?
    ingredients: str = get_pizza_ingredients(pizza_data, "2015-01-02", "18:27:50")
    print(f"Ingredients of the pizza ordered on 2015-01-02 at 18:27:50: {ingredients}")

    # 3. What is the most sold category of pizza between 2015-01-01 and 2015-01-08?
    most_sold_category: str = get_most_sold_category(pizza_data, "2015-01-01", "2015-01-08")
    print(f"Most sold category of pizza between 2015-01-01 and 2015-01-08: {most_sold_category}")

    spark.stop()


if __name__ == "__main__":
    main()
