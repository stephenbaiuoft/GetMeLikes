# GetMeLikes Insight DE Project
This repository is a Data Engineering project that batch-processes personal tweets
and provide keywords/phrases to individual users.

#### Project Idea
The project is to analyse individual's tweets to show each user's top phrases
that are more likely to be retweeted.

#### What is the purpose, and most common use cases?
The purpose to is provide an enhanced and personalized service so that each user's post
may get more retweets

The service helps individual users understand their most retweeted keywords that
they can utilize to get more social network attention(similar to LPP).

Note the data is tweets can the project infrastructure can easily extend to
any other social networks such as Instagrams and Facebook.

#### Which technologies are well-suited to solve those challenges? (list all relevant)

- *HDFS*: this is for storing large amount of raw tweeter data, also for saving
processed tweeter data

- *Spark*: for fast keyword counting

- *Database* to store parsed certain keywords and data, sorted by time?

#### Proposed architecture

HDFS --> Spark --> Database
