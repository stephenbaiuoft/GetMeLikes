->
===========Note======================
dataframe -> getting and querying data with limited counting followers_count
rdd --> where the data transformation comes in 
dataframe --> write back to database, in this case cassandra, where CQL comes in


# solution for nullable composite keys
# just pull empty or 0 or 'false' or none  --> to sub null
# so you may still query!

==> primary composite 
key(date? maybe set to '' and do not select those either)

==> city (as partition key)
==> country(in case of duplicate)



->
#A materialized view is a table that is built from
# another table's data with a new primary key and new properties.

# 0 for dataframe, 1 for preprocessed, 2 for second
# use text for future integration and enhancement, so countries of regions

# syntax for selecting from current data frame!
#
-> df_0 is the data_frame by which i need to perform map function on
tid   long (id)
uid bigint PRIMARY KEY(user.id), 

tweet: text, (text)
# need to parse into 0-24 hour clock
creation_time: text (user.created_at) #utc time parsing

time_zone: text, (user.time_zone)
followers_count: bigint, (user.followers_count)
friends_count: bigint, (user.friends_count)


metadata: map with the following:


#
entities.media: array of jsons --> to Boolean: count if uploaded with pics
entities.hashtags: array of jsons --> int: count of hashtags only (enuf)

# current we have array of tokens
word_token: map,
# later will have array of key words/bigrams, trigrams
phrase_token: map, (to be implemented)

place.name: (text), (city name if there) 
# need to perform a province count
place.country:(text) (country name if there)
==> 
convert everyting to
place: map ({city:text, state: text(if there),country: text})


============> TABLE 1: main storage=======================
					// yyyy-month-day
PRIMARY KEY (uid, creation_date)

CREATE TABLE raw_table_0(
tid, bigint,  
uid: bigint,
tweet: text,
followers_count: bigint, (user.followers_count)
friends_count: bigint, (user.friends_count)
retweet_count: int, 
favorite_count int,
word_token_set: list<text>, (tuples of words)
phrase_token: list<text>, ()
# Material views
creation_date: date, (user.created_at, required spark processing) #utc time parsing
creation_time: time, (require spark processing)
# materialized view on time_zone
time_zone: text, (user.time_zone)

# materialized view on region, if not null ==> aggregation 
place.name: (text), (city name if there) 
# need to perform a province count
place.country:(text) (country name if there)

# materialized view on whether media is along tweeted
entities.media: array of jsons --> to Boolean: count if uploaded with pics
# materialized view on # of hashtags and # of retweets
entities.hashtags: array of jsons, --> int: count of hashtags only (enuf)
PRIMARY KEY (uid, creation_date)
);

========note there is no order by for table 1============================ 

================Create Materialized Views=====================


================Make sure those selected columns are not null==


===================Second Step=========================================
-> here is the second step!


# creating the table
CREATE TABLE proceeded_tweets_1(
id bigint PRIMARY KEY, 
favorite_count bigint,
lang: string text,
time_zone: text,
tweet: text,
);

============Computing Step=====(Top List: database sql, probably not?)===============
-> give me the uid
-> query from the result database, if not exist, then 

-> i make the query (from relative materialized_views )and get the records

-> write a compute function that does rows() and return top lists
-> now write to the result database,
-> so when new data comes in, you can directly update that record. 
============Computing Step====================



-> here is the third table
# enhanced table
CREATE TABLE proceeded_tweets_2(
word_count,
sentence_count, 
)