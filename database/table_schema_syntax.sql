#This is for user!!!!
DROP TABLE user_top_list;
DROP TABLE user_top_quarter_list;
DROP TABLE user_top_year_list;
DROP TABLE user_top_month_list;


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
PRIMARY KEY (user_name, creation_date)
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
PRIMARY KEY (user_name, creation_date)
) WITH CLUSTERING ORDER BY (creation_date DESC);

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
PRIMARY KEY (user_name, creation_date)
) WITH CLUSTERING ORDER BY (creation_date DESC);



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



CREATE KEYSPACE twitter
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 2
  };

