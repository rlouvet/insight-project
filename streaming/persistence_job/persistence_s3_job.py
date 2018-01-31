#!/usr/bin/env python

"""
[Customer Support Percolation] Persistence to HDFS Job
This script is part of the "Customer Support Percolation" project developped
during my Insight Fellowship program (NYC Jan 2018).
It can persist customer click-stream data into a master dataset on AWS S3.
"""
import os, json
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

brokers_dns_str = os.environ['KAFKA_SERVERS']
topic = os.environ['TOPIC']
bucket_name = os.environ['BUCKET_NAME']

if __name__ == "__main__":
    start_time = datetime.datetime.now()

    sc = SparkContext(appName="spark_streaming")
    ssc = StreamingContext(sc, 1)

    kafka_stream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers_dns_str})

    messages = kafka_stream.map(lambda v: v[1])
    now = datetime.datetime.now()
    messages.saveAsTextFiles('s3a://' + bucket_name + '/clickstreams-' + start_time.strftime("%Y%m%dT%H%M%S"))

    count = messages.count()
    count.pprint()

    ssc.start()
    ssc.awaitTermination()
