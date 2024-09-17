import os
from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("mysql-spark")
    .config("spark.jars", "mysql-connector-j-9.0.0.jar")
    .config("spark.hadoop.home.dir", "hadoop")
    .getOrCreate()
)

jdbc_url = "jdbc:mysql://localhost:3306/aws_big_data"
connection_properties = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table="tv_shows", properties=connection_properties)

json_data = spark.read.json("data/tvs/tvs.json", multiLine=True)

df.select("id", "name").join(json_data.select("id", "original_name"), "id", "left").collect()

spark.stop()
