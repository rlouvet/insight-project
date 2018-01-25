#!/usr/bin/env python

"""
[Customer Support Percolation] Persistence to HDFS Job
This script is part of the "Customer Support Percolation" project developped
during my Insight Fellowship program (NYC Jan 2018).
It can persist customer click-stream data into a master dataset on HDFS.
"""
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":

	with open("config.txt", 'r') as f:
		zkQuorum, topic = f.readline().split('')

    sc = SparkContext(appName="spark_streaming_hdfs")
    ssc = StreamingContext(sc, 1)

    kafka_stream = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    parsed = kafka_stream.map(lambda v: json.loads(v[1]))
    count = parsed.count()
    count.pprint()

    ssc.start()
    ssc.awaitTermination()
