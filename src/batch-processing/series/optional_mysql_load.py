import os
import mysql.connector
import pandas as pd
import json


def read_data(path: str) -> pd.DataFrame:
    """Read data."""
    with open(path, "r", encoding="utf-8") as file:
        data = json.load(file)

    return pd.json_normalize(pd.DataFrame(data).to_dict("records"))


def create_connection():
    """Create a database connection."""

    return mysql.connector.connect(
        host="localhost", user=os.getenv("MYSQL_USER"), password=os.getenv("MYSQL_PASSWORD"), database="aws_big_data"
    )


def create_tvs_table(_cursor) -> None:
    """Create TV series table."""

    _cursor.execute("DROP TABLE IF EXISTS tv_shows;")

    _cursor.execute("""
    CREATE TABLE IF NOT EXISTS tv_shows (
        id INT PRIMARY KEY,
        name VARCHAR(255),
        original_name VARCHAR(255),
        overview TEXT,
        in_production BOOLEAN,
        status VARCHAR(50),
        original_language VARCHAR(10),
        first_air_date DATE,
        last_air_date DATE,
        number_of_episodes INT,
        number_of_seasons INT,
        vote_average FLOAT,
        vote_count INT,
        popularity FLOAT
    )
    """)


def clean_data(data: pd.DataFrame) -> pd.DataFrame:
    """Clean data."""

    return (
        data[data["id"] != 60606]
        .assign(last_air_date=pd.to_datetime(data["last_air_date"], errors="coerce").dt.date)
        .assign(first_air_date=pd.to_datetime(data["first_air_date"], errors="coerce").dt.date)
        .assign(name=data["name"].replace("", None))
    ).dropna(subset=["name"])


def insert_data(_cursor, data: pd.DataFrame) -> None:
    """Insert data."""

    insert_query = """
    INSERT INTO tv_shows (id, name, original_name, overview, in_production, status, 
                          original_language, first_air_date, last_air_date, number_of_episodes, 
                          number_of_seasons, vote_average, vote_count, popularity)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

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
