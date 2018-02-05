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