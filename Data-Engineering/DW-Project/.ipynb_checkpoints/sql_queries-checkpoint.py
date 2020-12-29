import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Global variables
SONG_PATH = config.get("S3", "SONG_DATA")
LOG_PATH = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
IAM_ROLE = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songPlays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS times CASCADE"

# CREATE TABLES

############## Raw data ##############

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stage_events
(
artist          text,
auth            text, 
firstName       text,
gender          text,   
itemInSession   int,
lastName        text,
length          float,
level           text, 
location        text,
method          text,
page            text,
registration    bigint,
sessionId       int,
song            text,
status          int,
ts              timestamp,
userAgent       text,
userId          int
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stage_songs 
(song_id         text        PRIMARY KEY,
num_songs        int,
title            text,
artist_name      text,
artist_latitude  float,
year             int,
duration         float,
artist_id        text,
artist_longitude float,
artist_location  text
)
""")

############## Raw data ##############

############## Fact Table ##############

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songPlays 
(
songplay_id       int        IDENTITY(0,1) PRIMARY KEY, 
start_time        timestamp  SORTKEY       DISTKEY, 
user_id           int        REFERENCES    users(user_id), 
level             text, 
song_id           text       REFERENCES    songs(song_id), 
artist_id         text       REFERENCES    artists(artist_id), 
session_id        int, 
location          text, 
user_agent        text
)
""")

############## Fact Table ##############

############## Dimensions Tables ##############

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
(
user_id           int        PRIMARY KEY SORTKEY,
first_name        text,
last_name         text,
gender            text,
level             text
)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
(
song_id           text       PRIMARY KEY SORTKEY,
title             text, 
artist_id         text, 
year              int, 
duration          float
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
(
artist_id         text       PRIMARY KEY SORTKEY,
name              text,
location          text,
latitude          float,
longitute         float)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS times 
(
start_time        text       PRIMARY KEY SORTKEY DISTKEY,
hour              int,
day               int,
week              int,
month             int,
year              int,
weekday           text
)
""")

############## Dimensions Tables ##############

############## Stage Tables ##############

# STAGING TABLES - Retrieve files from S3, into Redshift

staging_events_copy = """
    COPY stage_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    TIMEFORMAT 'epochmillisecs'
    JSON {}
    
""".format(LOG_PATH, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = """
    COPY stage_songs FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    region 'us-west-2'
    JSON 'auto'
""".format(SONG_PATH, IAM_ROLE)

############## Stage Tables ##############

# FINAL TABLES

songplay_table_insert = ("""
INSERT into songPlays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
    se.userId as user_id,
    se.level  as level,
    ss.song_id as song_id,
    ss.artist_id as artist_id,
    se.sessionID as session_id,
    se.location as location,
    se.userAgent as user_agent
FROM stage_events se 
JOIN stage_songs ss 
    ON se.song = ss.title 
    AND se.artist = ss.artist_name

""")

user_table_insert = ("""
INSERT into users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
    firstName as first_name,
    lastName as last_name,
    gender as gender,
    level as level
FROM stage_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
    title as title,
    artist_id as artist_id,
    year as year,
    duration as duration
FROM stage_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitute)
SELECT DISTINCT artist_id as artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM stage_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
    EXTRACT(hour from ts),
    EXTRACT(day from ts),
    EXTRACT(week from ts),
    EXTRACT(month from ts),
    EXTRACT(year from ts),
    EXTRACT(weekday from ts)
FROM stage_events
WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
