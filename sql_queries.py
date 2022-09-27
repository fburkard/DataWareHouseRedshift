import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP Table If Exists staging_event"
staging_songs_table_drop = "DROP Table If Exists staging_song"
songplay_table_drop = "DROP Table If Exists songplay"
user_table_drop = "DROP Table If Exists users"
song_table_drop = "DROP Table If Exists song"
artist_table_drop = "DROP Table If Exists artist"
time_table_drop = "DROP Table If Exists time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE Table If not Exists staging_event(
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        iteminsession INT,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INT,
        song varchar,
        status INT,
        ts bigint,
        userAgent VARCHAR,
        userID INT);
""")

staging_songs_table_create = ("""
Create Table If not Exists staging_song(
num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
year int);
""")

songplay_table_create = ("""
Create Table If not Exists songplay(songplay_id IDENTITY(0,1), \
    start_time timestamp NOT NULL, user_id int NOT NULL, \
    level varchar, song_id varchar, \
    artist_id varchar, \
    session_id int, location varchar, user_agent varchar);
""")

user_table_create = ("""
Create Table If not Exists users(user_id int PRIMARY KEY, first_name varchar, \
last_name varchar, gender varchar, level varchar);
""")

song_table_create = ("""
Create Table If not Exists song(song_id varchar PRIMARY KEY, title varchar NOT NULL, \
artist_id varchar, year int, duration float NOT NULL);
""")

artist_table_create = ("""
Create Table If not Exists artist(artist_id varchar PRIMARY KEY, 
name varchar NOT NULL, location varchar, latitude float, longitude float);
""")

time_table_create = ("""
Create Table If not Exists time(start_time TIMESTAMP PRIMARY KEY, hour int, \
day int, week int, month int, year int, weekday varchar);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_event from s3://udacity-dend/log_data
credentials 'aws_iam_role={}'
gzip delimiter ';' compupdate off region 'us-east-1';
""").format(*config['IAM_ROLE'])

staging_songs_copy = ("""
copy staging_song from s3://udacity-dend/song_data
credentials 'aws_iam_role={}'
gzip delimiter ';' compupdate off region 'us-east-1';
""").format(*config['IAM_ROLE'])

# FINAL TABLES

#https://knowledge.udacity.com/questions/276119
songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
    e.start_time, 
    s.user_id, 
    s.level, 
    e.song_id, 
    e.artist_id, 
    e.session_id, 
    s.location, 
    s.user_agent
    FROM staging_event s
    JOIN staging_song e ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)
    WHERE page = 'NextSong';
""")
#WHERE start_time, user_id NOT IN (SELECT DISTINCT start_time, user_id from songplay);
#VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
#ON CONFLICT (songplay_id) DO NOTHING;

user_table_insert = ("""
    INSERT INTO user(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_event
    WHERE page = 'NextSong' 
    WHERE user_id NOT IN (SELECT DISTINCT user_id from user);
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_song
    WHERE song_id NOT IN (SELECT DISTINCT song_id from song);
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, name, location, latitude, longitude
    FROM staging_song
    WHEERE artist_id NOT IN (SELECT DISTINCT artist_id from artist);
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
start_time, 
extract(hour from start_time),
extract(day from start_time),
extract(week from start_time),
extract(year from start_time),
extract(weekday from start_time)
FROM staging_event
WHERE page = 'NextSong'
""")

time_table_insert_tmp = ("""INSERT INTO time 
SELECT DISTINCT ON (start_time) * FROM tmp_time ON CONFLICT DO NOTHING;
DROP TABLE IF EXISTS tmp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
