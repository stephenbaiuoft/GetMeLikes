# this doc is for correct syntax of table 1
============> TABLE 1 main storage=======================
					// yyyy-month-day
PRIMARY KEY (uid, creation_date)

DROP TABLE raw_table_0;
CREATE TABLE raw_table_0(
tid bigint,
uid bigint,
tweet text,
followers_count bigint, 
friends_count bigint, 
retweet_count int, 
favorite_count int,
word_token_set list<text>,
sentence_count int,
phrase_token list<text>, 
creation_date date, 
creation_time timestamp, 
time_zone text,
city_name text, 
country_name text,
media_attached boolean, 
hashtag_count int, 
PRIMARY KEY (uid, tid)
);

-- create materialized view by twitter_id
-- important as tweets partitioned by uid
DROP MATERIALIZED VIEW individual_view;
CREATE MATERIALIZED VIEW individual_view AS
       SELECT tid, uid, followers_count, retweet_count, favorite_count,
       word_token_set, phrase_token
       FROM raw_table_0
       WHERE tid IS NOT NULL AND uid IS NOT NULL
       PRIMARY KEY (uid, tid)
       WITH CLUSTERING ORDER BY (creation_date desc);

-- create materialized view by DATE
DROP MATERIALIZED VIEW creation_date_view;
CREATE MATERIALIZED VIEW creation_date_view AS
       SELECT tid, uid, followers_count, retweet_count, favorite_count,
       word_token_set, phrase_token
       FROM raw_table_0
       WHERE creation_date IS NOT NULL 
       PRIMARY KEY ((uid, creation_date), tid)
       WITH CLUSTERING ORDER BY (creation_date desc);

-- create materialized view by TIME
DROP MATERIALIZED VIEW creation_time_view;
CREATE MATERIALIZED VIEW creation_time_view AS
       SELECT tid, uid, followers_count, retweet_count, favorite_count,
       word_token_set, phrase_token
       FROM raw_table_0
       WHERE creation_time IS NOT NULL AND tid IS NOT NULL AND creation_time IS NOT NULL
       PRIMARY KEY (uid, creation_time, tid)
       WITH CLUSTERING ORDER BY (creation_time desc);

DROP MATERIALIZED VIEW geolocation_view;
CREATE MATERIALIZED VIEW geolocation_view AS
       SELECT uid, followers_count, retweet_count, favorite_count,
       word_token_set, phrase_token
       FROM raw_table_0
       WHERE city_name IS NOT NULL AND country_name IS NOT NULL 
       AND uid IS NOT NULL
       PRIMARY KEY (country_name, city_name, uid)
       WITH CLUSTERING ORDER BY (country_name desc);       



