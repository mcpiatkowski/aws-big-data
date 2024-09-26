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