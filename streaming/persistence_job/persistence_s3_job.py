#!/usr/bin/env python

"""
[Customer Support Percolation] Persistence to HDFS Job
This script is part of the "Customer Support Percolation" project developped
during my Insight Fellowship program (NYC Jan 2018).
It can persist customer click-stream data into a master dataset on AWS S3.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

kafka_servers = os.environ['KAFKA_SERVERS']
topic = os.environ['TOPIC']
bucket_name = os.environ['BUCKET_NAME']

if __name__ == "__main__":

    spark = (
        SparkSession
        .builder
        .appName("Job that persists clickstream data from Kafka to CSV files on S3") \
        .getOrCreate()
    )

    kafka_stream = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .load()
    )

    messages = kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    schema = StructType(
        [
        StructField("epochtime", StringType()),
        StructField("userid", StringType()),
        StructField("pageid_origin", StringType()),
        StructField("pageid_target", StringType()),
        StructField("case_status", StringType())
        ]
    )

    parsed_messages = (
        messages
        .select(from_json(messages.value, schema).alias("json"))
        .select(
            col("json").getItem("epochtime").alias("epochtime"),
            col("json").getItem("userid").alias("userid"),
            col("json").getItem("pageid_origin").alias("pageid_origin"),
            col("json").getItem("pageid_target").alias("pageid_target"),
            col("json").getItem("case_status").alias("case_status")
            )
    )

    query = (
        parsed_messages
        .writeStream
        .outputMode("append")
        .format("console")
        #.option("path", "s3a://" + bucket_name + "/clickstreams")
        .start()
    )

    query.awaitTermination()
