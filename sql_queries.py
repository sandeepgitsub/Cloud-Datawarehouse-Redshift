############################################################################################################
#             CHANGE LOG
# This script consists of different sql stattements to drop , create tables and insert data in to the tables.
############################################################################################################
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA              = config.get('S3', 'LOG_DATA')
LOG_JSONPATH          = config.get('S3', 'LOG_JSONPATH')
SONG_DATA             = config.get('S3', 'SONG_DATA')
ARN                   = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES
# stage tables

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
artist varchar,
auth varchar,
first_name varchar,
gender varchar,
item_in_session int,
last_name varchar,
length NUMERIC,
level varchar,
location varchar,
method varchar,
page varchar,
registration FLOAT,
session_id int,
song varchar,
status int, 
ts BIGINT,
user_agent varchar,
user_id int);

""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs(
num_songs BIGINT,
artist_id varchar,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration numeric,
year int);
""")
# Fact table
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
songplay_id BIGINT IDENTITY(0,1), 
start_time TIMESTAMP  REFERENCES time(start_time) distkey, 
user_id int REFERENCES users(user_id), 
level varchar, 
song_id varchar REFERENCES songs(song_id), 
artist_id varchar REFERENCES artists(artist_id),
session_id int, 
location varchar, 
user_agent text);
""")

# Dimension tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
user_id int PRIMARY KEY sortkey,
first_name varchar, 
last_name varchar, 
gender varchar, 
level varchar)
DISTSTYLE ALL;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
song_id varchar PRIMARY KEY distkey,
title varchar, 
artist_id varchar, 
year int, 
duration numeric);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
artist_id varchar PRIMARY KEY distkey, 
name varchar, 
location varchar, 
latitude float, 
longitude float);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP PRIMARY KEY distkey, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int);
""")

# COPY STAGING TABLES FROM S3 Buckets

staging_events_copy = (""" COPY staging_events FROM {} CREDENTIALS 'aws_iam_role={}' region 'us-west-2'
json {};
""").format(LOG_DATA, ARN,LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {} credentials 'aws_iam_role={}' 
region 'us-west-2' json 'auto';""").format(SONG_DATA, ARN)

# INSERT FACT TABLE and DIMENSION TABLES

# Insert sonplays table from staging events and staging songs
songplay_table_insert = (""" INSERT INTO songplays(start_time,
user_id,
level,
song_id,
artist_id,
session_id,
location,
user_agent) 
(select TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_ts,
se.user_id,
se.level,
ss.song_id,
ss.artist_id,
se.session_id,
se.location,
se.user_agent
from staging_events se left join  staging_songs ss on se.artist =ss.artist_name and se.song = ss.title and se.length = ss.duration
where page = 'NextSong');
""")

# Insert users table from staging events 
user_table_insert = ("""INSERT INTO users(select 
distinct user_id, 
first_name,
last_name,
gender,
level 
from staging_events
where user_id is not null);
""")
# Insert songs table from staging songs 
song_table_insert = (""" INSERT INTO songs(
select DISTINCT 
song_id , 
title, 
artist_id, 
year, 
duration from  staging_songs);
""")

# Insert artists table from staging songs 
artist_table_insert = (""" INSERT INTO artists(
select DISTINCT artist_id , 
artist_name, 
artist_location, 
artist_latitude, 
artist_longitude 
from  staging_songs );
""")

# Insert time  table from staging events
time_table_insert = (""" INSERT INTO time(select distinct start_ts,
extract(HOUR  from start_ts) as hour,
extract(DAY from start_ts)as day,
extract(WEEK from start_ts) as week,
extract(MONTH from start_ts)as month,
extract(YEAR FROM start_ts)as year,
extract(DOW from start_ts)as weekday from (
SELECT distinct TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_ts
from 
staging_events));
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,  user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create,]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
