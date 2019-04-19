# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE IF NOT EXISTS \
                          songplays ( \
                          start_time VARCHAR \
                        , user_id INT \
                        , level TEXT \
                        , song_id VARCHAR \
                        , artist_id VARCHAR \
                        , session_id INT \
                        , location VARCHAR \
                        , user_agent VARCHAR \
                        , PRIMARY KEY (start_time, user_id));")

user_table_create = ("CREATE TABLE IF NOT EXISTS \
                      users ( \
                      user_id INT \
                    , first_name TEXT \
                    , last_name TEXT \
                    , gender TEXT \
                    , level TEXT \
                    , PRIMARY KEY (user_id));")

song_table_create = ("CREATE TABLE IF NOT EXISTS \
                      songs ( \
                      song_id VARCHAR \
                    , title VARCHAR \
                    , artist_id VARCHAR \
                    , year INT \
                    , duration REAL \
                    , PRIMARY KEY (song_id));")

artist_table_create = ("CREATE TABLE IF NOT EXISTS \
                        artists ( \
                        artist_id VARCHAR \
                      , name VARCHAR \
                      , location VARCHAR \
                      , latitude NUMERIC \
                      , longitude NUMERIC \
                      , PRIMARY KEY (artist_id));")

time_table_create = ("CREATE TABLE IF NOT EXISTS \
                      time ( \
                      start_time VARCHAR \
                    , hour INT \
                    , day INT \
                    , week INT \
                    , month INT \
                    , year INT \
                    , weekday TEXT \
                    , PRIMARY KEY (start_time));")

# INSERT RECORDS

songplay_table_insert = ("INSERT INTO songplays (start_time, user_id, level, song_id, \
                                                 artist_id, session_id, location, user_agent) \
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                            ON CONFLICT (start_time, user_id) DO NOTHING;")

user_table_insert = ("INSERT INTO users (user_id, first_name, last_name, gender, level) \
                      VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (user_id) DO NOTHING;") # Added due to duplicate users

song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration) \
                      VALUES (%s, %s, %s, %s, %s) \
                      ON CONFLICT (song_id) DO NOTHING;") # Added due to duplicate songs

artist_table_insert = ("INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (artist_id) DO NOTHING;") # Added due to duplicate artists


time_table_insert = ("INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                      VALUES (%s, %s, %s, %s, %s, %s, %s) \
                      ON CONFLICT (start_time) DO NOTHING;") # Added due to duplicate start times

# FIND SONGS
# Query selects song_id and artist_id as those are needed for the songplays table.
# The conditional checks the song title, artist name, and song duration against
# the values drawn from log dataset.

song_select = ("""SELECT s.song_id, a.artist_id FROM songs s, artists a WHERE s.title=(%s) AND a.name=(%s);""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]