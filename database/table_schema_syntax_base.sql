Old Schema ---> Let's build the new schema!!!!!!!!!
CREATE TABLE user_top_month_list(
uid bigint,
user_name text,
creation_date date,
rt_word_list list<frozen <tuple<text,int>>>,
rt_entity_list list<frozen <tuple<text,int>>>,
fav_word_list list<frozen <tuple<text,int>>>,
fav_entity_list list<frozen <tuple<text,int>>>,
rt_word_occur_list list<frozen <tuple<text,int>>>,
rt_entity_occur_list list<frozen <tuple<text,int>>>,
fav_word_occur_list list<frozen <tuple<text,int>>>,
fav_entity_occur_list list<frozen <tuple<text,int>>>,
PRIMARY KEY (uid, creation_date)
) WITH CLUSTERING ORDER BY (creation_date DESC);

CREATE TABLE user_top_year_list(
uid bigint,
user_name text,
creation_date date,
rt_word_list list<frozen <tuple<text,int>>>,
rt_entity_list list<frozen <tuple<text,int>>>,
fav_word_list list<frozen <tuple<text,int>>>,
fav_entity_list list<frozen <tuple<text,int>>>,
rt_word_occur_list list<frozen <tuple<text,int>>>,
rt_entity_occur_list list<frozen <tuple<text,int>>>,
fav_word_occur_list list<frozen <tuple<text,int>>>,
fav_entity_occur_list list<frozen <tuple<text,int>>>,
PRIMARY KEY (uid, creation_date)
) WITH CLUSTERING ORDER BY (creation_date DESC);

CREATE TABLE user_top_quarter_list(
uid bigint,
user_name text,
creation_date date,
rt_word_list list<frozen <tuple<text,int>>>,
rt_entity_list list<frozen <tuple<text,int>>>,
fav_word_list list<frozen <tuple<text,int>>>,
fav_entity_list list<frozen <tuple<text,int>>>,
rt_word_occur_list list<frozen <tuple<text,int>>>,
rt_entity_occur_list list<frozen <tuple<text,int>>>,
fav_word_occur_list list<frozen <tuple<text,int>>>,
fav_entity_occur_list list<frozen <tuple<text,int>>>,
PRIMARY KEY (uid, creation_date)
)WITH CLUSTERING ORDER BY (creation_date DESC);

CREATE TABLE user_top_list(
uid bigint,
user_name text,
rt_word_list list<frozen <tuple<text,int>>>,
rt_entity_list list<frozen <tuple<text,int>>>,
fav_word_list list<frozen <tuple<text,int>>>,
fav_entity_list list<frozen <tuple<text,int>>>,
rt_word_occur_list list<frozen <tuple<text,int>>>,
rt_entity_occur_list list<frozen <tuple<text,int>>>,
fav_word_occur_list list<frozen <tuple<text,int>>>,
fav_entity_occur_list list<frozen <tuple<text,int>>>,

PRIMARY KEY (user_name, uid)
);



# create batch_test
CREATE TABLE batch_test(
uid int PRIMARY KEY,
top_word_list list<frozen <tuple<text,int>>>
);

##### SYNTAX####

CREATE TABLE ttt (id varchar PRIMARY KEY, 
       address_mapping list<frozen<tuple<text,text>>>);

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





DROP TABLE user_top_month_list;
CREATE TABLE user_top_month_list(
uid bigint,
user_name text,
creation_date date,
rt_word_list list<frozen <tuple<text,int>>>,
rt_entity_list list<frozen <tuple<text,int>>>,
fav_word_list list<frozen <tuple<text,int>>>,
fav_entity_list list<frozen <tuple<text,int>>>,

rt_word_occur_list list<frozen <tuple<text,int>>>,
rt_entity_occur_list list<frozen <tuple<text,int>>>,
fav_word_occur_list list<frozen <tuple<text,int>>>,
fav_entity_occur_list list<frozen <tuple<text,int>>>,

PRIMARY KEY (uid, creation_date)
);


DROP TABLE user_top_year_list;
CREATE TABLE user_top_year_list(
uid bigint,
user_name text,
creation_date date,
rt_word_list list<frozen <tuple<text,int>>>,
rt_entity_list list<frozen <tuple<text,int>>>,
fav_word_list list<frozen <tuple<text,int>>>,
fav_entity_list list<frozen <tuple<text,int>>>,

rt_word_occur_list list<frozen <tuple<text,int>>>,
rt_entity_occur_list list<frozen <tuple<text,int>>>,
fav_word_occur_list list<frozen <tuple<text,int>>>,
fav_entity_occur_list list<frozen <tuple<text,int>>>,

PRIMARY KEY (uid, creation_date)
);


DROP TABLE user_top_quarter_list;
CREATE TABLE user_top_quarter_list(
uid bigint,
user_name text,
creation_date date,
rt_word_list list<frozen <tuple<text,int>>>,
rt_entity_list list<frozen <tuple<text,int>>>,
fav_word_list list<frozen <tuple<text,int>>>,
fav_entity_list list<frozen <tuple<text,int>>>,

rt_word_occur_list list<frozen <tuple<text,int>>>,
rt_entity_occur_list list<frozen <tuple<text,int>>>,
fav_word_occur_list list<frozen <tuple<text,int>>>,
fav_entity_occur_list list<frozen <tuple<text,int>>>,

PRIMARY KEY (uid, creation_date)
);


2018.02.01!!!!!

DROP TABLE user_top_list;
CREATE TABLE user_top_list(
uid bigint,
user_name text,
rt_word_list list<frozen <tuple<text,int>>>,
rt_entity_list list<frozen <tuple<text,int>>>,
fav_word_list list<frozen <tuple<text,int>>>,
fav_entity_list list<frozen <tuple<text,int>>>,

rt_word_occur_list list<frozen <tuple<text,int>>>,
rt_entity_occur_list list<frozen <tuple<text,int>>>,
fav_word_occur_list list<frozen <tuple<text,int>>>,
fav_entity_occur_list list<frozen <tuple<text,int>>>,

PRIMARY KEY (uid, user_name)
);





DROP TABLE trump_top;
CREATE TABLE trump_top(
uid int PRIMARY KEY,
rt_word_list list<frozen <tuple<text,int>>>,
fav_word_list list<frozen <tuple<text,int>>>
);


DROP TABLE batch_test;
CREATE TABLE batch_test(
uid int,
top_word_set list<tuple<text,int>>,
PRIMARY KEY uid
);


CREATE TABLE trump(
tid bigint,
uid bigint,
tweet text,
followers_count int, 
friends_count int, 
retweet_count int, 
favorite_count int,
word_token_set list<text>,
sentence_count int,
phrase_token list<text>, 
creation_date date, 
creation_timestamp timestamp, 
creation_hour int,
time_zone text,
city_name text, 
country_name text,
media_attached boolean, 
hashtag_count int, 
PRIMARY KEY (uid, tid)
);


DROP TABLE b0;
CREATE TABLE b0(
tid bigint,
uid bigint,
tweet text,
followers_count int, 
friends_count int, 
retweet_count int, 
favorite_count int,
word_token_set list<text>,
sentence_count int,
phrase_token list<text>, 
creation_date date, 
creation_timestamp timestamp, 
creation_hour int,
time_zone text,
city_name text, 
country_name text,
media_attached boolean, 
hashtag_count int, 
PRIMARY KEY (uid, tid)
);



====
creation_date: 'String': order in materialized view in case
creation_timestamp: 'timestamp', to search quickly advanced time series
creation_hour: 'int', to query amaterialized view