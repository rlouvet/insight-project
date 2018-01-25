#!/usr/bin/env python

"""
[Customer Support Percolation] Persistence to HDFS Job
This script is part of the "Customer Support Percolation" project developped
during my Insight Fellowship program (NYC Jan 2018).
It can process the master click-stream dataset to compute user paths.
"""
import os, json
from pyspark import SparkContext, SparkConf

bucket_name = os.environ['BUCKET_NAME']

if __name__ == "__main__":

    conf = SparkConf().setAppName('Batch - Compute User Path')
    sc = SparkContext(conf=conf)
    cs = sc.textFile('s3a://' + bucket_name + '/parsed*')
    print('Count: ' + cs.take(100))
