INSERT INTO tv_shows (
    id,
    name,
    original_name,
    overview,
    in_production,
    status,
    original_language,
    first_air_date,
    last_air_date,
    number_of_episodes,
    number_of_seasons,
    vote_average,
    vote_count,
    popularity
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
