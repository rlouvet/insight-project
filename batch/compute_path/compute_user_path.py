#!/usr/bin/env python

"""
[Customer Support Percolation] Persistence to HDFS Job
This script is part of the "Customer Support Percolation" project developped
during my Insight Fellowship program (NYC Jan 2018).
It can process the master click-stream dataset to compute user paths.
"""
import sys, os, json, datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, countDistinct

target_time = sys.argv[1]
read_bucket_name = os.environ['READ_BUCKET_NAME']
write_bucket_name = os.environ['WRITE_BUCKET_NAME']


if __name__ == "__main__":
    start_time = datetime.datetime.now()

    #Setting up spark context
    conf = SparkConf().setAppName('Batch - Compute User Path')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    #Targetting master dataset
    records = sc.textFile('s3a://' + read_bucket_name
        + '/clickstreams-' + target_time + '*')

    #Spark transformation: parsing json lines
    parsed_records = records.map(lambda m: json.loads(m))
    #Spark transformation: working with key-value pairs with key=userid
    user_records = parsed_records.map(lambda x: (x['userid'], x))

    #Spark transformation: combineByKey to build a time-ordered list of records per userid
    def record_combiner(v):
        return [v]

    def record_merge_value(c, v):
        c.extend([v])
        return sorted(c, key= lambda v: int(v['epochtime']))

    def record_merge_combiners(c1, c2):
        c1.extend(c2)
        return sorted(c1, key= lambda v: int(v['epochtime']))

    combined_user_records = user_records.combineByKey(
        record_combiner,
        record_merge_value,
        record_merge_combiners
        )

    #Spark transformation: combineByKey to build a list of paths per userid
    def path_combiner(records):
        paths = [[]]
        for record in records:
            paths[-1].extend([int(record['pageid_target'])])
            if record['case_status'] == "True":
                paths.append([])
        return paths

    def path_merge_value(paths, records):
        for record in records:
            paths[-1].extend([int(record['pageid_target'])])
            if record['case_status'] == "True":
                paths.append([])
        return paths

    def path_merge_combiners(paths1, paths2):
        return paths1 + paths2

    user_paths = combined_user_records.combineByKey(
        path_combiner,
        path_merge_value,
        path_merge_combiners
        )

    print('=== User Paths: ===\n' + str(user_paths.first()))

    #Converting RDD into Spark dataframe for more performant aggregation operations
    paths = user_paths.flatMapValues(lambda x: x).values().map(lambda x: Row(** {'path': json.dumps(x)}))

    def build_schema():
        schema = StructType([
                StructField("path", StringType(), True)
            ])
        return schema

    paths_df = sql.createDataFrame(paths, schema=build_schema())
    paths_df.show()

    paths_rank_df = paths_df.groupBy('path').count().sort(col("count").desc())

    limited_paths_rank = paths_rank_df.limit(1000)
    limited_paths_rank.show()

    limited_paths_rank.write.csv('s3a://' + write_bucket_name
        + '/results-' + target_time + '-' + start_time.strftime("%Y%m%dT%H%M%S"))
