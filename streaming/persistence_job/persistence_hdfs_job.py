#!/usr/bin/env python

"""
[Customer Support Percolation] Persistence to HDFS Job
This script is part of the "Customer Support Percolation" project developped
during my Insight Fellowship program (NYC Jan 2018).
It can persist customer click-stream data into a master dataset on HDFS.
"""
import os
import yaml
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

zkQuorum = os.environ['ZK_QUORUM']

with open("config.yml", 'r') as ymlfile:
	cfg = yaml.load(ymlfile)

if __name__ == "__main__":

    sc = SparkContext(appName="spark_streaming_hdfs")
    ssc = StreamingContext(sc, 1)
    
    topic = cfg['topic']

    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
