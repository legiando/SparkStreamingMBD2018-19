#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Streaming Individual Assignment 
Anggie Legiando Pratama
IE HST MBS O2 2018/2019

@author: legiando
"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.functions import expr
from pyspark_hyperloglog import hll  # necessary module for approximate distinct count
from pyspark.streaming.kafka import KafkaUtils 
import sys
import operator


def tags_count_aggregated(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


def start_sql_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']


def rdd_context(time, rdd):
    print("----------- %s -----------" % str(time))
    try:

        # Get spark sql singleton context from the current context
        sql_context = start_sql_instance(rdd.context)

        # convert RDD to Row RDD
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))

        # create a Dataframe from the Row RDD
        hashtags_df = sql_context.createDataFrame(row_rdd)

        # Register Dataframe as a table
        hashtags_df.registerTempTable("hashtags")
        
        # use of HyperLogLog to calculate the approximate numbers of distinct hashtags seen
        print("----Aproximate Count of Distinct Hashtags using HyperLogLog----")
        hll.register()
        countAprx = hashtags_df.select(expr("hll_create(hashtag, 12) as hll"))\
                .groupBy()\
                .agg(expr("hll_cardinality(hll_merge(hll)) as count"))
        countAprx.show()

    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from the above spark context with interval size 5 seconds
ssc = StreamingContext(sc, 5)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 6666
# dataStream = ssc.socketTextStream("localhost", 6666)
dataStream = KafkaUtils.createDirectStream(ssc, ['tweets'], {"metadata.broker.list": "localhost:6666"})

# split each tweet into words
words = dataStream.map(lambda x: x[1]).flatMap(lambda line: line.split())

# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).map(lambda x: (x, 1))

# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(tags_count_aggregated)
# This next line will count the exact numbers and the approximate
# ammount of distinct hashtags using hyperloglog
tags_totals.foreachRDD(process_rdd)

# I start the streaming computation
ssc.start()

# I wait for the streaming to finish
ssc.awaitTermination()
