# testing for only counting entity retweets

# testing for cassandra-cluster
# testing for optimization on reading
# testing for python jobs

import sys
import datetime
import re
from datetime import timedelta

sys.path.append('../../')

from pyspark.sql import SparkSession, Row
from pyspark.ml import Pipeline

# from sparknlp.annotator import *
# from sparknlp.common import RegexRule
# from sparknlp.base import DocumentAssembler, Finisher
from pyspark.sql.functions import explode

from dateutil.parser import parse
# for tokenizing
from nltk.tokenize import sent_tokenize, word_tokenize, RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from nltk import pos_tag, RegexpParser

# for schema
from pyspark.sql.types import *

# for testing
import random
import heapq
import re
from unidecode import unidecode
from sparkconfig import *

# instantiate a spark context object
appname = "Insight Lean Version"
# master_url = "localhost"

cassandra_url_remote = "ec2-34-217-203-51.us-west-2.compute.amazonaws.com"

# Create Spark Session
spark = SparkSession.builder.appName(appname) \
    .master(MASTER_URL) \
    .config("spark.cassandra.connection.host", CASSANDRA_URL_CLUSTER) \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.executor.memory", "5300m") \
    .getOrCreate()


def remove_non_ascii(text):
    try:
        rez = unidecode(text)
        return rez
    except Exception as e:
        return "failed conversion"


# lean version that extracts less info
def twitter_datadump_interface_lean(df):
    try:
        sql_condition = 'uid is NOT NULL ' + \
                        'AND tweet is NOT NULL AND creation_time is NOT NULL ' + \
                        ' AND user_name is NOT NULL'
        # Select interested col attributes
        main_df = df.selectExpr(
            'user.id AS uid', \
            'text AS tweet', \
            'created_at AS creation_time', \
            'retweet_count', \
            'favorite_count', \
            'user.name AS user_name'
        ).where(sql_condition)
    except Exception as e:
        print("twitter_datadump_interface_lean: failed to parse\n\n\n")

    return main_df


# Modfication for single word extraction as well
# Modfication for single word extraction as well
# Modfication for single word extraction as well

# extract word_list(exclusing noun+stopwords) and noun phrase list
def extract_entity(tokens):
    tokens_tag = pos_tag(tokens)
    # in case tokens_tags are emtpy, return empty list
    if (len(tokens_tag) == 0):
        return []

    # set up for entity extraction
    grammar = "NP: {<DT>?<JJ.*>*<NN.*>+}"
    cp = RegexpParser(grammar)
    np_ary = []

    root_tree = cp.parse(tokens_tag)
    for i in list(root_tree.subtrees(filter=lambda x: x.label() == 'NP')):
        st = ""
        for t in i.leaves():
            st = st + t[0] + " "
        np_ary.append(remove_non_ascii(st.strip()))
    # entity extracted

    # combine them together
    #    np_ary = np_ary + word_list
    return np_ary


# return 2 items: sentence_count,  <word_tuple>, <name_entity>
def process_tweet(description):
    # base case
    if description is None or description == "":
        return (0, [], [])
    # filter out http items
    description = \
        re.sub(r'https:[^\s]*', '', description, flags=re.IGNORECASE)
    tokenizer = RegexpTokenizer(r'\w+')

    # extract single word
    word_list = []
    words = tokenizer.tokenize(description)
    token_tag = pos_tag(words)
    stemmer = SnowballStemmer("english")
    stopWords = set(stopwords.words('english'))

    for word_pos in token_tag:
        stem_word = stemmer.stem(word_pos[0])
        if stem_word not in stopWords and word_pos[1] != 'NN':
            word_list.append(remove_non_ascii(stem_word))

    entity_list = extract_entity(words)
    return (len(sent_tokenize(description)), word_list, entity_list)


# parse twitter time string to (date, timestamp_str, hour int)
# note later only timestamp_str is changed to asTYpe
def parse_time(creation_time):
    # fcn that converts dt to date-str and time-str
    def cassandra_convert(dt):
        hour = dt.strftime("%H")
        return (dt.date(), str(dt), int(hour))

    dt = None
    try:
        dt = parse(creation_time)
    except Exception as e:
        # 1. log and
        # 2.use current system time instead
        dt = datetime.datetime.now()
    return cassandra_convert(dt)


# Mapping: 1. 13 cols to 15 cols (word_list, date,
#          timestamp, hour)
#          2. (media_ary-> media_attached; tag_ary -> tag_count)
def map_nlp_row_lean(row):
    # init those variables locally
    i_uid = 0
    i_tweet = 1
    i_creation_time = 2
    i_retweet_count = 3
    i_favorite_count = 4
    i_user_name = 5

    uid = row[i_uid]
    tweet = row[i_tweet]
    # Code for scraped analysis
    #     retweet_count = row[i_retweet_count]
    #     favorite_count = row[i_favorite_count]

    base = random.randint(100, 1000)
    offset = random.randint(-99, 300)
    retweet_count = base
    favorite_count = base + offset

    # Map token: sentence_count, word_list
    sentence_count, word_list, entity_list \
        = process_tweet(tweet)
    # Map Time
    creation_time = row[i_creation_time]
    date, timestamp, hour \
        = parse_time(creation_time)
    user_name = row[i_user_name]
    user_name = remove_non_ascii(user_name)

    #     except Exception as e:
    #         # 1. log e and 2.return default
    #         return Row(tid=-1, uid=-1,
    #             followers_count=-1,
    #             friends_count=-1,
    #             tweet='n/a', retweet_count=-1,
    #             favorite_count=-1,sentence_count=-1,
    #             word_list=[], entity_list=[],
    #             date=datetime.date(2001, 1, 1),
    #             timestamp='00:00:00',
    #             hour=0,
    #             time_zone='n/a',city_name='n/a',
    #             country_name='n/a',
    #             media_attached=False,tag_count=-1,
    #             user_name='n/a'
    #             )

    r = (uid, user_name, tweet, retweet_count, favorite_count,
         word_list, entity_list, date, timestamp)
    return r


# base_nlp_rdd = base_rdd.map(map_nlp_row)

def map_time_lean(row):
    i_uid = 0
    i_user_name = 1
    i_retweet_count = 3
    i_favorite_count = 4
    i_word_list = 5
    i_entity_list = 6
    i_date = 7

    return (row[i_uid], row[i_user_name], row[i_date],
            row[i_retweet_count], row[i_favorite_count],
            row[i_word_list], row[i_entity_list])


# get top_n for input_dic
# return a tuple of n entries
# default to 20, but can be customized
def get_top_n_dic(input_dic):
    n = 8
    top_list = heapq.nlargest(n, input_dic.items(), key=lambda x: int(x[1][0] / x[1][1]))
    return top_list


# rez_dic_set_rdd.mapValues(get_top_n_ti)
# time_interval dic row => mapValues
def get_top_n_ti(row):
    # put in user_name
    rez_ary = []
    user_name = row[0]
    time_dic = row[1]
    # key is date, v is dic_set
    for k, v in time_dic.iteritems():
        rt_top = get_top_n_dic(v[0])
        rt_entity_top = get_top_n_dic(v[1])
        fav_top = get_top_n_dic(v[2])
        fav_entity_top = get_top_n_dic(v[3])

        rez_ary.append((k, user_name, rt_top, \
                        rt_entity_top, fav_top, fav_entity_top))

    return rez_ary


# max for top_n for all_time
def get_top_n_alltime(row):
    rt_top = get_top_n_dic(row[1])
    rt_en_top = get_top_n_dic(row[2])
    fav_top = get_top_n_dic(row[3])
    fav_en_top = get_top_n_dic(row[4])

    return (row[0], rt_top, rt_en_top, fav_top, fav_en_top)


# merges 2 dic together --> because of double pointer crap
def merge_dic_large(large_dic, small_dic):
    for k in small_dic.keys():
        # if k in small_dic
        if k in large_dic:
            # merge values together
            kv1 = small_dic.get(k)
            kv2 = large_dic.get(k)
            value = (kv1[0] + kv2[0], kv1[1] + kv1[1])
            large_dic.update({k: value})
        # simply insert to large_dic
        else:
            large_dic.update({k: small_dic.get(k)})


# 30-day time interval split, c_date vs date_base (earliest date)
def get_relative_date(date_c, date_b, num_days=30):
    delta = date_c - date_b
    round_days = delta.days - delta.days % num_days
    relative_date = date_b + timedelta(days=round_days)
    # return string representation for storage
    return relative_date


# delta = datetime.date(2018,1,29) - datetime.date(2010,9,24)
# round_days = delta.days - delta.days % 30
# relative_date = datetime.date(2010,9,24) + timedelta(days=round_days)
# str(relative_date)
# '2018-01-15'

# helper-function for creating dictionary of {word_1: (count_int, occur_int) }
# helper-function for combineByKey time!

def add_to_dic(dic, word, count):
    if word in dic:
        k_record = dic.get(word)
        dic.update({word: (k_record[0] + count, k_record[1] + 1)})
    else:
        # word and tuple of count and # of times for word
        record = {word: (count, 1)}
        dic.update(record)


# Returning
def create_dic_set(row, rt_i=3, fav_i=4, word_list_i=5, entity_list_i=6):
    # helper function that updates, _list is either word_list or entity_list
    def perform_add_op(rt_c, fav_c, rt_dic, fav_dic, _list):
        #         if rt_c > 0 and fav_c > 0:
        #             for item in _list:
        #                 add_to_dic(rt_dic, item, rt_c)
        #                 add_to_dic(fav_dic, item, fav_c)
        if rt_c > 0:
            for item in _list:
                add_to_dic(rt_dic, item, rt_c)
                #         elif fav_c > 0 :
                #             for item in _list:
                #                 add_to_dic(fav_dic, item, fav_c)
                # End

    rt_dic = {}
    fav_dic = {}
    rt_entity_dic = {}
    fav_entity_dic = {}

    rt_c = row[rt_i]
    fav_c = row[fav_i]

    word_list = row[word_list_i]
    entity_list = row[entity_list_i]

    # Perform word_list ADD Operation
    #     perform_add_op(rt_c, fav_c, rt_dic, fav_dic, word_list)
    perform_add_op(rt_c, fav_c, \
                   rt_entity_dic, fav_entity_dic, entity_list)

    # return dic_set
    return (rt_dic, rt_entity_dic, fav_dic, fav_entity_dic)


# merges 2 dic together
def merge_dic(dic1, dic2):
    if len(dic1) < len(dic2):
        small_dic = dic1
        large_dic = dic2
    else:
        small_dic = dic2
        large_dic = dic1
    for k in small_dic.keys():
        # if k in small_dic
        if k in large_dic:
            # merge values together
            kv1 = small_dic.get(k)
            kv2 = large_dic.get(k)
            value = (kv1[0] + kv2[0], kv1[1] + kv1[1])
            large_dic.update({k: value})
        # simply insert to large_dic
        else:
            large_dic.update({k: small_dic.get(k)})
    # return large_dic
    return large_dic


# Will change to static class methods later!!
# GetTopByInterval()

###### uid_0, tid_1, date_2, rt_c_3,
###### fav_c_4, word_list_5, entity_list_6, min_date = -1

# new value is of format dic{ k_date0: (rt_dic, fav_dic),
#                             k_date1: (rt_dic, fav_dic)}

# create_dic_set REQUIRES
#  (rt_i, fav_i, word_list_i, entity_list_i)

# process info for cCombiner_t
def parse_row_t(row):
    date = row[2]
    m_date = row[-1]
    # get k_date for dic
    k_date = get_relative_date(date, m_date)
    user_name = row[1]
    rt_dic, rt_entity_dic, \
    fav_dic, fav_entity_dic = \
        create_dic_set(row, rt_i=3, fav_i=4, \
                       word_list_i=5, entity_list_i=6)

    return (user_name, k_date, rt_dic, rt_entity_dic, \
            fav_dic, fav_entity_dic)


# create the base combiner for this
def cCombiner_t(row):
    # handle row
    user_name, k_date, rt_dic, rt_entity_dic, \
    fav_dic, fav_entity_dic = parse_row_t(row)

    # create dic by time ==> word_list and entity_list
    # note 4 entries as a value (in python: pointer/memory address)
    dic_set_by_time = {k_date: (rt_dic, rt_entity_dic, \
                                fav_dic, fav_entity_dic)}

    # create a list of tid
    return (user_name, dic_set_by_time)


# which merges V into C
def mValue_t(new_row, row):
    # update_dic operation between two dictionaries
    # large_dic_set --> (k_date, (rt_dic, rt_entity_dic, fav_dic, fav_entity_dic))
    def update_dic_op(k_date, dic_set_by_time, \
                      rt_dic, rt_entity_dic, fav_dic, fav_entity_dic):
        if k_date in dic_set_by_time:
            # large_dic ==> 4 dictionaries
            large_dic = dic_set_by_time.get(k_date)
            # merge values
            merge_dic_large(large_dic[0], rt_dic)
            merge_dic_large(large_dic[1], rt_entity_dic)
            merge_dic_large(large_dic[2], fav_dic)
            merge_dic_large(large_dic[3], fav_entity_dic)
        else:
            value_set = (rt_dic, rt_entity_dic, \
                         fav_dic, fav_entity_dic
                         )
            dic_set_by_time.update({k_date: value_set})

            # handle row

    user_name, k_date, rt_dic, rt_entity_dic, \
    fav_dic, fav_entity_dic = parse_row_t(row)

    # get dic_set_by_time
    dic_set_by_time = new_row[1]

    update_dic_op(k_date, dic_set_by_time, \
                  rt_dic, rt_entity_dic, \
                  fav_dic, fav_entity_dic)
    # add tid to the tid list

    # tid_list.append(tid)
    # return the result
    return (user_name, dic_set_by_time)


# combine two C's (new row)
def mCombiners_t(r1, r2):
    # list_merge = r1[0] + r2[0]

    # format: {date_k: (rt_dic, rt_entity_dic, fav_dic, fav_entity_dic)}
    dic_set_by_time1 = r1[1]
    dic_set_by_time2 = r2[1]

    # merge dic_set_by_time based on # of times each partition has
    if len(dic_set_by_time1) < len(dic_set_by_time2):
        small_t_dic = dic_set_by_time1
        large_t_dic = dic_set_by_time2
    else:
        small_t_dic = dic_set_by_time2
        large_t_dic = dic_set_by_time1

    for k, v in small_t_dic.iteritems():
        if k in large_t_dic:
            # get corresponding dictionaries
            value_set = large_t_dic.get(k)
            for idx, dic_item in enumerate(value_set):
                # also merge the small_t_dic dictionary to dic_item
                # in the large item
                merge_dic_large(dic_item, v[idx])
        else:
            large_t_dic.update({k: v})

    return (r1[0], large_t_dic)


### Test Data for Aggregation on Data Intervals!!
### Based on the first processed data
### After compute_time_rdd!!!!!!!!!!
# (uid, ([tid],  large_dic: { d1: (d1,d2,..d4) }  )  )
# ==> i only need to map values

# dic are rt_word_dic, rt_entity_dic, fav_word_dic, fav_entity_dic

# base is 30 days --> 90 days ( mapValues)
def group_time_quarter(row):
    user_name = row[0]
    date_dic = row[1]
    min_date = min(date_dic.keys())

    # combine to new_date_dic
    new_date_dic = {}

    for date, v in date_dic.iteritems():
        k_date = get_relative_date(date, min_date, 90)
        if k_date in new_date_dic:
            value_set = new_date_dic.get(k_date)
            # merge dictionaries together
            # dic_item is the large value_set
            for idx, dic_item in enumerate(value_set):
                merge_dic_large(dic_item, v[idx])
        else:
            # add to new_date_dic
            new_date_dic.update({k_date: v})

    # return newly combined new_date_dic
    # [] legacy reason--> now don't care tid_list
    return (user_name, new_date_dic)


# base is 30 days --> 90 days ( mapValues)
def group_time_year(row):
    user_name = row[0]
    date_dic = row[1]
    min_date = min(date_dic.keys())

    # combine to new_date_dic
    new_date_dic = {}

    for date, v in date_dic.iteritems():
        k_date = get_relative_date(date, min_date, 365)
        if k_date in new_date_dic:
            value_set = new_date_dic.get(k_date)
            # merge dictionaries together
            # dic_item is the large value_set
            for idx, dic_item in enumerate(value_set):
                merge_dic_large(dic_item, v[idx])
        else:
            # add to new_date_dic
            new_date_dic.update({k_date: v})

    # return newly combined new_date_dic
    return (user_name, new_date_dic)


# from NLP rdd
# Do total count computing here: map selected ones only

# order:
# uid, tid, rt_c, fav_c, word_list, entity_list
def map_total_count(row):
    uid_i = i_uid
    tid_i = i_tid
    rt_c_i = i_retweet_count
    fav_c_i = i_favorite_count
    word_list_i = i_word_list
    entity_list_i = i_entity_list

    return (row[uid_i], row[tid_i], row[rt_c_i],
            row[fav_c_i], row[word_list_i], row[entity_list_i])


# given nlp_rdd, map to total_count_rdd
def total_count_interface(nlp_rdd):
    total_count_rdd = nlp_rdd.map(map_total_count)
    return total_count_rdd


# given nlp_rdd, map to time_rdd
def time_interface(nlp_rdd):
    time_rdd = nlp_rdd.map(map_time)
    return time_rdd


# given time_rdd, map each entry with minimum date
# (uid, (uid, tid, date, rt_count, fav_count,
#  word_list, entity_list, min_date) )

# def min_time_interface_old(time_rdd):
#     # change df to have key, value with min_date at the end
#     mtime_1 = time_rdd.keyBy(lambda x: x[0]).reduceByKey(lambda x, y: x if x[2] < y[2] else y)
#     # (key, min_date)
#     mtime_2 = mtime_1.mapValues(lambda x: x[2])
#     # group min_date together
#     min_time_rdd = time_rdd.keyBy(lambda x: x[0]).join(mtime_2).mapValues(lambda x: x[0]+(x[1],) )
#     return min_time_rdd

# compute time based on when twitter started!
def min_time_interface(time_rdd):
    def add_min_time(x):
        # add in the default twitter init time 2006.03.21
        min_date = datetime.date(2006, 3, 21)
        return (x[0], x[1], x[2], x[3], x[4], x[5], x[6], min_date)

    min_time_rdd = time_rdd.keyBy(lambda x: x[0]).mapValues(add_min_time)
    return min_time_rdd


def compute_time_rdd(min_time_rdd):
    # Input: {key = uid : (tid_list, date, rt_dic, rt_entity_dic,
    #                       fav_dic, fav_entity_dic ) }
    c_min_time_rdd = min_time_rdd.combineByKey(cCombiner_t, mValue_t, mCombiners_t)
    # Output: (key=uid,   (user_name, large_dic_by_date: {date,(dic_set,..,..)}))
    return c_min_time_rdd


# Input: rez_dic_set_rdd ==> (uid, (user_name, dictionary_set_4_dics))
# Output: (uid, ([-(date, user_name, rt_dic, rt_entity_dic, av_dic, fav_entity_dic),
#                 (set2),()...-) ]  )
# Note: ALL dics are of format: ( {word_str: (count, occur)} )
def rank_time_rdd(rez_dic_set_rdd):
    top_rdd = rez_dic_set_rdd.mapValues(get_top_n_ti)
    return top_rdd


# decouple top_list for each category
def decouple(_list):
    if (len(_list) > 0):
        w, v_pair = zip(*_list)
        count, occurence = zip(*v_pair)
        wc = list(zip(w, count))
        wo = list(zip(w, occurence))
        # return w_count, w_occurence
        return (wc, wo)
    return ([], [])


# Input top_rdd (uid,([--(time, username, d1,d2,d3,d4),(set2),--]) )
# Output: (uid, username, creation_time
#             rc_wc, rc_entity_wc, fav_wc, fav_entity_wc
#             rc_wo, rc_entity_wo, fav_wo, fav_entity_wo) -->not so interesting
def transform_rank_time_rdd(top_rdd):
    # main point is to decouple (count, occur)
    def flat_map(row):
        # code for flat_map
        uid = row[0]
        creation_time = row[1][0]
        user_name = row[1][1]

        rt_list = row[1][2]
        rt_entity_list = row[1][3]
        fav_list = row[1][4]
        fav_entity_list = row[1][5]

        rc_wc, rc_wo = decouple(rt_list)
        rc_entity_wc, rc_entity_wo = decouple(rt_entity_list)
        fav_wc, fav_wo = decouple(fav_list)
        fav_entity_wc, fav_entity_wo = decouple(fav_entity_list)

        return (uid, user_name, creation_time, \
                rc_wc, rc_entity_wc, fav_wc, fav_entity_wc, \
                rc_wo, rc_entity_wo, fav_wo, fav_entity_wo)

    # flat the value set!!!
    flat_top_rdd = top_rdd.flatMapValues(lambda x: x)

    # start by mapping with helper fcn
    final_rdd = flat_top_rdd.map(flat_map)
    return final_rdd


# Input: (uid, (username, rt_list, rt_en_list, fav_list, fav_en_list))
# Output: ready to be written into database
#         uid, username, \
#              rc_wc, rc_entity_wc, fav_wc, fav_entity_wc,\
#              rc_wo, rc_entity_wo, fav_wo, fav_entity_wo

def transform_alltime_rdd(row):
    uid = row[0]
    user_name = row[1][0]
    rt_list = row[1][1]
    rt_entity_list = row[1][2]
    fav_list = row[1][3]
    fav_entity_list = row[1][4]

    rc_wc, rc_wo = decouple(rt_list)
    rc_entity_wc, rc_entity_wo = decouple(rt_entity_list)
    fav_wc, fav_wo = decouple(fav_list)
    fav_entity_wc, fav_entity_wo = decouple(fav_entity_list)
    return (uid, user_name, \
            rc_wc, rc_entity_wc, fav_wc, fav_entity_wc, \
            rc_wo, rc_entity_wo, fav_wo, fav_entity_wo)


# Build Cassandra Database Schema
# take transform_rank_time_rdd output and converts to dataframe
# can be used to save to Cassandra
def to_time_top_df(rdd):
    schema = StructType([
        StructField("uid", LongType(), True),
        StructField("user_name", StringType(), True),
        StructField("creation_date", DateType(), True),

        StructField("rt_word_list", ArrayType(StructType([
            StructField("word", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("rt_entity_list", ArrayType(StructType([
            StructField("entity", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("fav_word_list", ArrayType(StructType([
            StructField("word", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("fav_entity_list", ArrayType(StructType([
            StructField("entity", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("rt_word_occur_list", ArrayType(StructType([
            StructField("word", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("rt_entity_occur_list", ArrayType(StructType([
            StructField("entity", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("fav_word_occur_list", ArrayType(StructType([
            StructField("word", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("fav_entity_occur_list", ArrayType(StructType([
            StructField("entity", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)
    ])

    df = spark.createDataFrame(rdd, schema)
    return df


# create schema for all time table schema!!!!
def to_alltime_df(rdd):
    schema = StructType([
        StructField("uid", LongType(), True),
        StructField("user_name", StringType(), True),

        StructField("rt_word_list", ArrayType(StructType([
            StructField("word", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("rt_entity_list", ArrayType(StructType([
            StructField("entity", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("fav_word_list", ArrayType(StructType([
            StructField("word", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("fav_entity_list", ArrayType(StructType([
            StructField("entity", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("rt_word_occur_list", ArrayType(StructType([
            StructField("word", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("rt_entity_occur_list", ArrayType(StructType([
            StructField("entity", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("fav_word_occur_list", ArrayType(StructType([
            StructField("word", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)

        ,
        StructField("fav_entity_occur_list", ArrayType(StructType([
            StructField("entity", StringType(), True),
            StructField("count", IntegerType(), False)])
            , True), True)
    ])

    df = spark.createDataFrame(rdd, schema)
    return df


def save_data_frame(df, table_name):
    df.write.format("org.apache.spark.sql.cassandra"). \
        mode('append').options(table=table_name, keyspace='twitter').save()


def write_to_data_base(rez_dic_set_rdd, table_name):
    top_rdd = rank_time_rdd(rez_dic_set_rdd)
    rez = transform_rank_time_rdd(top_rdd)
    df = to_time_top_df(rez)
    save_data_frame(df, table_name)
    return df


# computes the total count given the base 30_day rdd
# Input: (key=uid,   ([tid], large_dic_by_date: {date,(dic_set,..,..)}))
# Output: tuple of 4 dictionaries ({},....)
def compute_total_count(rez_dic_set_rdd):
    # mapping that groups all
    def group_time_all(row):
        date_dic = row[1]
        new_rt_dic = {}
        new_rt_en_dic = {}
        new_fav_dic = {}
        new_fav_en_dic = {}

        for date, v in date_dic.iteritems():
            # merge dictionaries together
            # dic_item is the large value_set
            merge_dic_large(new_rt_dic, v[0])
            merge_dic_large(new_rt_en_dic, v[1])
            merge_dic_large(new_fav_dic, v[2])
            merge_dic_large(new_fav_en_dic, v[3])
            # return new_dic_set with user_name + 4 dictionaries
        return (row[0], \
                new_rt_dic, new_rt_en_dic, \
                new_fav_dic, new_fav_en_dic)

    all_time_rdd = rez_dic_set_rdd.mapValues(group_time_all)

    return all_time_rdd


# wrapper to get from time_rdd to rez_dic_rdd
def exec_path_timerdd_rez_dic_rdd(time_rdd):
    min_rdd = min_time_interface(time_rdd)
    rez_dic_rdd = compute_time_rdd(min_rdd)
    return rez_dic_rdd


# --> exec_path_for_all time,
# Input: rez_dic_set_rdd
# Output: all time --> save to database, user_top_list
def exec_path_for_alltime(rez_dic_set_rdd):
    all_time_rdd = compute_total_count(rez_dic_set_rdd)
    rez = all_time_rdd.mapValues(get_top_n_alltime)
    # transform to the format for dataframe
    transformed_rdd = rez.map(transform_alltime_rdd)
    df = to_alltime_df(transformed_rdd)
    save_data_frame(df, "user_top_list")
    return df


# --> exec_path for time interval based for month, quater, and year
def exec_path_for_timeinterval(rez_dic_set_rdd):
    rez_dic_set_rdd.persist()
    df_month = write_to_data_base(rez_dic_set_rdd, "user_top_month_list")

    quater_rdd = rez_dic_set_rdd.mapValues(group_time_quarter)
    df_quarter = write_to_data_base(quater_rdd, "user_top_quarter_list")

    year_rdd = rez_dic_set_rdd.mapValues(group_time_year)
    df_year = write_to_data_base(year_rdd, "user_top_year_list")
    return (df_month, df_quarter, df_year)

url_folder = 's3a://twitter-data-dump/2017-10'
df = spark.read.json(url_folder)

# Region for testing celebrity data dumps!!
lean_df = twitter_datadump_interface_lean(df)
lean_nlp_rdd = lean_df.rdd.map(map_nlp_row_lean)
# V2: add in the filter part
lean_nlp_rdd = lean_nlp_rdd.filter(lambda x: x[1] > "")
try:
    # catch any exception
    lean_nlp_rdd.take(1)
except Exception as e:
    pass

# lean_nlp_rdd.take(1)

lean_time_rdd = lean_nlp_rdd.map(map_time_lean)

rez_dic_set_rdd = exec_path_timerdd_rez_dic_rdd(lean_time_rdd)

df_alltime = exec_path_for_alltime(rez_dic_set_rdd)

# df_alltime.take(1)

df_month, df_quarter, df_year = exec_path_for_timeinterval(rez_dic_set_rdd)