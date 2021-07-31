import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")
# TABLE NAMES
STAGING_EVENTS = "staging_events"
STAGING_SONGS = "staging_songs"
SONGPLAYS = "songplays"
USERS = "users"
SONGS = "songs"
ARTISTS = "artists"
TIME = "time"


# DROP TABLES
def drop_table(table) -> str:
    return "drop table if exists {};".format(table)


staging_events_table_drop = drop_table(STAGING_EVENTS)
staging_songs_table_drop = drop_table(STAGING_SONGS)
songplay_table_drop = drop_table(SONGPLAYS)
user_table_drop = drop_table(USERS)
song_table_drop = drop_table(SONGS)
artist_table_drop = drop_table(ARTISTS)
time_table_drop = drop_table(TIME)


# CREATE TABLES
staging_events_table_create = f"""create table {STAGING_EVENTS} (
    event_id int identity(0,1),
    artist_name varchar(255),
    auth varchar(50),
    user_first_name varchar(255),
    user_gender char(1),
    item_in_session	integer,
    user_last_name varchar(255),
    song_length	float,
    user_level varchar(50),
    location varchar(255),
    method varchar(50),
    page varchar(50),
    registration varchar(50),
    session_id	bigint,
    song_title varchar(255),
    status integer,
    ts varchar(50),
    user_agent text,
    user_id varchar(50),
    primary key (event_id)
)
;"""

staging_songs_table_create = f"""create table {STAGING_SONGS} (
    song_id varchar(50),
    num_songs integer,
    artist_id varchar(50),
    artist_latitude float,
    artist_longitude float,
    artist_location varchar(255),
    artist_name varchar(255),
    title varchar(255),
    duration float,
    year integer,
    primary key (song_id)
)
;"""

songplay_table_create = f"""create table {SONGPLAYS} (
    songplay_id int identity(0,1),
    start_time timestamp,
    user_id varchar(50),
    level varchar(50),
    song_id varchar(50),
    artist_id varchar(50),
    session_id bigint,
    location varchar(255),
    user_agent text,
    primary key (songplay_id)
)
;"""

user_table_create = f"""create table {USERS} (
    user_id varchar,
    first_name varchar(255),
    last_name varchar(255),
    gender char(1),
    level varchar(50),
    primary key (user_id)
)
;"""

song_table_create = f"""create table {SONGS} (
    song_id varchar,
    title varchar(255),
    artist_id char(50) NOT NULL,
    year integer,
    duration float,
    primary key (song_id)
)
;"""

artist_table_create = f"""create table {ARTISTS} (
    artist_id varchar,
    name varchar(255),
    location varchar(255),
    latitude float,
    longitude float,
    primary key (artist_id)
)
;"""

time_table_create = f"""create table {TIME} (
    start_time timestamp,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer,
    primary key (start_time)
)
;"""


# STAGING TABLES
s3_log_data = config.get("S3", "LOG_DATA")
s3_song_data = config.get("S3", "SONG_DATA")
s3_log_jsonpath = config.get("S3", "LOG_JSONPATH")
iam_role = config.get("IAM_ROLE", "ARN")
staging_events_copy = ("""copy {} from {} iam_role {} json {};""").format(
    STAGING_EVENTS,
    s3_log_data,
    iam_role,
    s3_log_jsonpath,
)
staging_songs_copy = ("""copy {} from {} iam_role {} json 'auto';""").format(
    STAGING_SONGS, s3_song_data, iam_role
)

# FINAL TABLES
songplay_table_insert = f"""insert into {SONGPLAYS} (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
select
    timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
    e.user_id,
    e.user_level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
from {STAGING_EVENTS} e, {STAGING_SONGS} s
where e.page = 'NextSong'
  and e.song_title = s.title
  and e.artist_name = s.artist_name
  and e.song_length = s.duration
;"""

user_table_insert = f"""insert into {USERS} (user_id, first_name, last_name, gender, level)
select distinct
  user_id, user_first_name, user_last_name, user_gender, user_level
from staging_events
where page = 'NextSong'
;"""

song_table_insert = f"""insert into {SONGS} (song_id, title, artist_id, year, duration)
select distinct
  song_id, title, artist_id, year, duration
from {STAGING_SONGS}
where song_id is not null
;"""

artist_table_insert = f"""insert into {ARTISTS} (artist_id, name, location, latitude, longitude)
select distinct
  artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from {STAGING_SONGS}
where artist_id is not null
;"""

time_table_insert = f"""insert into {TIME} (start_time, hour, day, week, month, year, weekday)
select
  start_time,
  extract(hour from start_time),
  extract(day from start_time),
  extract(week from start_time),
  extract(month from start_time),
  extract(year from start_time),
  extract(dayofweek from start_time)
from {SONGPLAYS}
;"""


# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]

if __name__ == "__main__":
    print(songplay_table_drop)
    print()
    print(user_table_create)
    print()
    print(staging_events_copy)
    print(staging_songs_copy)
    print()
    print(artist_table_insert)
