import configparser
from pyspark.sql import SparkSession
from utils.queries import analytical_queries
import matplotlib.pyplot as plt
import os
from time import time
import pandas as pd


def create_spark_session():
    """
        Creates a spark session, reads all data needed for analytics queries and creates temp views
        for sql queries

        Returns:
        object: spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
    spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

    return spark


def query_tables(spark, queries, saveto):
    """
         Takes spark session and our analytical_queries
         and prints results of each query for testing
         Parameters:
         spark: spark session
         queries: the project's analytical_queries
         names: the names of our queries for identification purposes  during testing
      """
    queryTimes = []
    queryNames = []
    for query in queries:
        t0 = time()
        query = query(spark)
        print("======= Writing: ** {} **  =======".format(query.query_name))
        query.execute().save("{}/{}".format(saveto, query.query_name))



        queryTime = time() - t0
        queryTimes.append(queryTime)
        queryNames.append(query.query_name)

    return pd.DataFrame({"query": queryNames, "querytime_": queryTimes}).set_index('query')


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()

    stats = query_tables(spark, analytical_queries, config['QUERIES']['DESTINATION'])
    stats.plot.bar()
    plt.show()
    spark.stop()


if __name__ == "__main__":
    main()
