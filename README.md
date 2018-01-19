# GetMeLikes Insight DE Project
This repository is a Data Engineering project that batch-processes personal tweets
and provide keywords/phrases to individual users.

# Table of Contents

* [About](#about)
* [Use cases](#use-cases)
* [Which technologies are well\-suited to solve those challenges? (list all relevant)](#which-technologies-are-well-suited-to-solve-those-challenges-list-all-relevant)
* [Proposed architecture (Lamda Architecture)](#proposed-architecture-lamda-architecture)


# About
The project is to analyse individual's tweets to show each user's top phrases
that are more likely to be retweeted.

# Use cases
- The purpose to is provide an enhanced and personalized service so that each user's post
may get more retweets

- The service helps individual users understand their most retweeted keywords that
they can utilize to get more social network attention(similar to LPP).

- Note the data is tweets can the project infrastructure can easily extend to
any other social networks such as Instagrams and Facebook.

# Which technologies are well-suited to solve those challenges? (list all relevant)

- *S3*: this is for storing large amount of raw tweeter data, also for saving
processed tweeter data

- *Spark*: for batch keyword counting

- *Cassandra*: to store parsed certain keywords and data, for time series data

- *Spark Streaming*: to handle real-time twitter data and update the database

# Proposed architecture (Lamda Architecture)

<img src=https://s3-us-west-2.amazonaws.com/stephen-image-storage/insight-project/model-01.jpg>
