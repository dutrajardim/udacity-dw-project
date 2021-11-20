import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS usesr"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create = """
CREATE TEMPORARY TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR(50),
    firstName VARCHAR(50),
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length NUMERIC(10,6),
    level VARCHAR(10),
    location VARCHAR(150),
    method VARCHAR(7),
    page VARCHAR(50),
    registration NUMERIC(13,0),
    sessionId INTEGER,
    song VARCHAR,
    status SMALLINT,
    ts NUMERIC(13,0),
    userAgent VARCHAR,
    userId INTEGER
)
"""

staging_songs_table_create = """
CREATE TEMPORARY TABLE IF NOT EXISTS staging_songs (
    artist_id VARCHAR(18),
    artist_latitude NUMERIC(8,6),
    artist_location VARCHAR,
    artist_longitude NUMERIC(9,6),
    artist_name VARCHAR,
    duration NUMERIC(10,6),
    num_songs SMALLINT,
    song_id VARCHAR(18),
    title VARCHAR,
    year SMALLINT
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1),
    start_time TIMESTAMP REFERENCES times (start_time) DISTKEY,
    user_id INTEGER REFERENCES users (user_id),
    level VARCHAR(10),
    song_id VARCHAR(18) REFERENCES songs (song_id),
    artist_id VARCHAR(18) REFERENCES artists (artist_id),
    session_id INTEGER,
    location VARCHAR(150),
    user_agent VARCHAR
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender CHAR(1),
    level VARCHAR(10)
) DISTSTYLE ALL
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(18) PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR(18),
    year SMALLINT,
    duration NUMERIC(10,6)
) DISTSTYLE ALL
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(18) PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude NUMERIC(8,6),
    longitude NUMERIC(9,6)
) DISTSTYLE ALL
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS times (
    start_time TIMESTAMP PRIMARY KEY DISTKEY,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday SMALLINT
)
"""

# STAGING TABLES

staging_events_copy = (
    """
COPY staging_events
FROM '{}'
iam_role '{}'
JSON '{}'
"""
).format(
    config["S3"]["LOG_DATA"],
    config["IAM_ROLE"]["ARN"],
    config["S3"]["LOG_JSONPATH"],
)

staging_songs_copy = (
    """
COPY staging_songs
FROM '{}'
iam_role '{}'
JSON '{}'
"""
).format(
    config["S3"]["SONG_DATA"],
    config["IAM_ROLE"]["ARN"],
    config["S3"]["SONG_JSONPATH"],
)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + ts / 1000 * interval '1 second' as start_time,
    userId as user_id,
    level,
    staging_songs.song_id,
    staging_songs.artist_id,
    sessionId as session_id,
    location,
    userAgent as user_agent
FROM staging_events 
LEFT JOIN staging_songs ON 
    staging_events.song = staging_songs.title AND
    ROUND(staging_events.length, 3) = ROUND(staging_songs.duration, 3)
EXCEPT
SELECT 
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent 
FROM songplays
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL
EXCEPT
SELECT * FROM users
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT 
    song_id, 
    title,
    artist_id, 
    year, 
    duration 
FROM staging_songs
EXCEPT
SELECT * FROM songs
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT 
    artist_id, 
    artist_name as name, 
    artist_location as location, 
    artist_latitude as latitude, 
    artist_longitude as longitude
FROM staging_songs
EXCEPT
SELECT * FROM artists
"""

time_table_insert = """
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT
    start_time,
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(dayofweek from start_time) as weekday
FROM songplays
EXCEPT
SELECT * FROM times
"""

# QUERY LISTS

create_table_queries = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]

drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

create_staging_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    song_table_insert,
    songplay_table_insert,
    user_table_insert,
    artist_table_insert,
    time_table_insert,
]
