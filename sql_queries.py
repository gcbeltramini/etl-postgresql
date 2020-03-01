table_names = ('songs', 'artists', 'time', 'users', 'songplays')

# CREATE TABLES

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
  song_id VARCHAR(64) PRIMARY KEY,
  title VARCHAR,
  artist_id VARCHAR,
  duration REAL,
  year SMALLINT
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR(64) PRIMARY KEY,
  artist_name VARCHAR,
  artist_location VARCHAR,
  artist_latitude REAL,
  artist_longitude REAL
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
  timestamp TIMESTAMP WITH TIME ZONE PRIMARY KEY,
  year SMALLINT,
  month SMALLINT,
  day SMALLINT,
  hour SMALLINT,
  weekofyear SMALLINT,
  weekday SMALLINT
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  first_name VARCHAR,
  last_name VARCHAR,
  gender CHAR(1),
  level CHAR(4)
);
"""
# gender = "M" or "F"
# level = "free" or "paid"

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
  songplay_id SERIAL PRIMARY KEY,
  timestamp TIMESTAMP WITH TIME ZONE,
  song_id VARCHAR(64),
  artist_id VARCHAR(64),
  user_id INTEGER,
  session_id INTEGER,
  level CHAR(4),
  location VARCHAR,
  user_agent VARCHAR
);
"""

# INSERT RECORDS

SONG_TABLE_INSERT = """
INSERT INTO songs (song_id, title, artist_id, duration, year)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING;
"""

ARTIST_TABLE_INSERT = """
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude,
                     artist_longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
"""

TIME_TABLE_INSERT = """
INSERT INTO time (timestamp, year, month, day, hour, weekofyear, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (timestamp) DO NOTHING;
"""

USER_TABLE_INSERT = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO NOTHING;
"""

SONGPLAY_TABLE_INSERT = """
INSERT INTO songplays (timestamp, song_id, artist_id, user_id, session_id,
                       level, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

# FIND SONGS

SONG_SELECT = """
SELECT songs.song_id, songs.artist_id
FROM songs
INNER JOIN artists ON artists.artist_id = songs.artist_id
WHERE
      songs.title = %s
  AND artists.artist_name = %s
  AND songs.duration = %s;
"""

# QUERY LISTS

CREATE_TABLE_QUERIES = [song_table_create, artist_table_create,
                        time_table_create, user_table_create,
                        songplay_table_create]
DROP_TABLE_QUERIES = [f'DROP TABLE IF EXISTS {table:s}'
                      for table in table_names]
