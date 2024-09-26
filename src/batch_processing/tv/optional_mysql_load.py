import os
import mysql.connector
import pandas as pd
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger("MySQL Data Loading")


def read_data(path: str) -> pd.DataFrame:
    """Read data."""
    log.info(f"Reading data from {path}...")
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    return pd.json_normalize(pd.DataFrame(data).to_dict("records"))


def read_sql_query(file_path: str) -> str:
    """Read SQL query from a .sql file."""
    log.info(f"Reading SQL query from {file_path}...")
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()


def create_connection():
    """Create a database connection."""
    log.info("Creating database connection...")
    return mysql.connector.connect(
        host="localhost", user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"), database="aws_big_data"
    )


def create_tvs_table(_cursor) -> None:
    """Create TV series table."""
    log.info("Creating TV series table...")
    _cursor.execute(read_sql_query("src/batch_processing/sql/drop_table_tv.sql"))
    _cursor.execute(read_sql_query("src/batch_processing/sql/create_table_tv.sql"))


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    """Clean data."""
    log.info("Cleaning data...")
    return (
        data[data["id"] != 60606]
        .assign(last_air_date=pd.to_datetime(data["last_air_date"], errors="coerce").dt.date)
        .assign(first_air_date=pd.to_datetime(data["first_air_date"], errors="coerce").dt.date)
        .assign(name=data["name"].replace("", None))
    ).dropna(subset=["name"])


def insert_data(_cursor, data: pd.DataFrame) -> None:
    """Insert data."""
    insert_query: str = read_sql_query("src/batch_processing/sql/insert_tv.sql")
    log.info("Inserting data into TV series table...")
    for _, row in data.iterrows():
        values = (
            row["id"],
            row["name"],
            row["original_name"],
            row["overview"] if not pd.isna(row["overview"]) else None,
            row["in_production"],
            row["status"],
            row["original_language"],
            row["first_air_date"] if not pd.isna(row["first_air_date"]) else None,
            row["last_air_date"] if not pd.isna(row["last_air_date"]) else None,
            row["number_of_episodes"] if not pd.isna(row["number_of_episodes"]) else None,
            row["number_of_seasons"],
            row["vote_average"],
            row["vote_count"],
            row["popularity"],
        )
        _cursor.execute(insert_query, values)


if __name__ == "__main__":
    series: pd.DataFrame = read_data("data/tvs/tvs.json").pipe(clean_data)

    connection = create_connection()
    cursor = connection.cursor()

    create_tvs_table(cursor)
    insert_data(cursor, series)

    connection.commit()
    connection.close()
    log.info("Process completed successfully")
