#!/usr/bin/

from pyspark.sql import SparkSession

# note sparknlp is external package to pyspark
from sparknlp.annotator import *
from pyspark.ml import Pipeline
from sparknlp.base import DocumentAssembler, Finisher
from pyspark.sql import Row

from config.sparkconfig import *

# from cassandra.cluster import Cluster

class spark_service:
    def __init__(self):
        self.cluster_name = CLUSTER_NAME
        self.conf = SparkConf().setAppName(APP_NAME).setMaster(MASTER)
        # sc -> spark context object
        self.sc = SparkContext(conf=self.conf)
        self.sqlContext = (self.sc)

        # use PARTIAL_DATA for now
        self.twitter_df = self.sqlContext.\
            read.json(DATA_DUMP)

        # Displays the content of the DataFrame to stdout
        # df.show()

    # get the s3 data, default to json.bz2 ONLY
    def import_data(self, data_source):
        # data_source = PARTIAL_DATA

        self.sc.textFile(data_source)
        pass

    # dump --> only id for now
    def dump_data(self):
        df_id = self.twitter_df.select('id')
        df_id.createGlobalTempView('id_view')
        # and now the view is global_temp.tmp

        sql_cmd = "SELECT id, count(*) from global_temp.id_view group by id"
        df_valid = self.sqlContext.sql(sql_cmd)

        df_valid.write.format("org.apache.spark.sql.cassandra").\
            mode('append').options(table='t2',keyspace='twitter').save()


    # connect to cassandra service
    def connect_cassandra(self):
        # self.cluster = Cluster(['10.0.0.7', '10.0.0.4'])
        # session = cluster.connect('twitter')
        pass

    # perform count on keywords --> needs digging?
    def get_top_keywords(self):
        pass


    # save to processed data_frame to storage
    def save_location(self, data_frame, url):
        # defaults to HDFS for now
        pass



def main():
    my_service = spark_service()
    my_service.dump_data()

    pass

# function for init cluster_name
def init_spark(cluster_name):
    a = []
    if len(a) is 0:
        print("hello")

    pass


# import data from S3
def import_s3data():


    pass


if __name__ == '__main__':

    pass
    # main()
