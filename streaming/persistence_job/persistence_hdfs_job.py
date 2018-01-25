#!/usr/bin/env python

"""
[Customer Support Percolation] Persistence to HDFS Job
This script is part of the "Customer Support Percolation" project developped
during my Insight Fellowship program (NYC Jan 2018).
It can persist customer click-stream data into a master dataset on HDFS.
"""
import os, json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#with open("config.txt", 'r') as f:
#    brokers_dns_str= f.readline()

if __name__ == "__main__":

    sc = SparkContext(appName="spark_streaming_hdfs")
    ssc = StreamingContext(sc, 1)

    #kafka_stream = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    brokers_dns_str = 'ec2-35-168-217-31.compute-1.amazonaws.com:9092,ec2-52-70-15-239.compute-1.amazonaws.com:9092,ec2-52-7-98-10.compute-1.amazonaws.com:9092'
    kafka_stream = KafkaUtils.createDirectStream(ssc, ['clickstreams-topic'], {"metadata.broker.list": brokers_dns_str})

    parsed = kafka_stream.map(lambda v: json.loads(v[1]))
    count = parsed.count()
    count.pprint()

    ssc.start()
    ssc.awaitTermination()
