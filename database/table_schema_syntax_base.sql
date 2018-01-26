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