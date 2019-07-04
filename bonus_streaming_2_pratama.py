#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Streaming Individual Assignment 
Anggie Legiando Pratama
IE HST MBS O2 2018/2019

@author: legiando
"""

from pyspark import SparkConf,SparkContext 
from pyspark.streaming import StreamingContext 
from pyspark.sql import Row,SQLContext 
import sys
import requests
from pyspark.sql import SparkSession
import os


def heavy_hitters(word, topWords):
	if len(topWords) <= 10 and word not in topWords.keys():
		topWords.update({word : 1})
	else:

		if word in topWords.keys():
			topWords[word] = topWords[word] + 1
		else:
			min_key = min(topWords, key=topWords.get)
			topWords[word] = topWords.pop(min_key) + 1

	return topK



def aggregate_tags_count(new_values, total_sum): 
	# aggregation
	return sum(new_values) + (total_sum or 0)

def process_rdd(time, rdd):
	global topWords
	print("----------- %s -----------" % str(time)) 
	for word in rdd.collect():
		try:
			topWords = heavy_hitters(word,topWords)
		except:
			e = sys.exc_info()
			print('Error:', e)
			pass
	print(topWords)



# I created a spark configuration
conf = SparkConf() 
conf.setAppName("TwitterStreamApp")

# I created a spark context with the above configuration 
# sc = create_sparkcontext()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# Streaming Context with the interval size of 10 seconds 
topWords = {}
ssc = StreamingContext(sc, 10)

# I set a checkpoint to allow RDD recovery 
ssc.checkpoint("checkpoint_TwitterApp")

# I read the data from the port 
dataStream = ssc.socketTextStream("localhost",6666)

# sI decided to split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))


# This is our rdd to apply the function
words.foreachRDD(process_rdd)


# Now I start the streaming computation
ssc.start()

# In this step I wait for the streaming to finish
ssc.awaitTermination()
