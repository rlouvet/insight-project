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
    #Setting up spark context
    conf = SparkConf().setAppName('Batch - Compute User Path')
    sc = SparkContext(conf=conf)

    #Targetting master dataset
    cs = sc.textFile('s3a://' + bucket_name + '/parsed*')

    #Spark transformation: parsing json lines
    parsed_cs = cs.map(lambda m: json.loads(m))
    #Spark transformation: working with key-value pairs with key=userid
    user_kv = parsed_cs.map(lambda x: (x['userid'], x))

    #Spark transformation: combineByKey to build a time-ordered list of records per userid
    def record_combiner(v):
        return [v]

    def record_merge_value(c, v):
        c.extend([v])
        return sorted(c, key= lambda v: int(v['epochtime']))

    def record_merge_combiners(c1, c2):
        c1.extend(c2)
        return sorted(c1, key= lambda v: int(v['epochtime']))

    combined_records = user_kv.combineByKey(record_combiner, record_merge_value, record_merge_combiners)

    #Spark transformation: combineByKey to build a list of paths per userid
    def path_combiner(records):
        paths = [[]]
        for record in records:
            paths[-1].extend(record['pageid_target'])
            if record['case_status'] == "True":
                paths.append([])
        return paths

    def path_merge_value(paths, records):
        for record in records:
            paths[-1].extend(record['pageid_target'])
            if record['case_status'] == "True":
                paths.append([])
        return paths

    def path_merge_combiners(paths1, paths2):
        return paths1 + paths2

    paths = combined_records.combineByKey(path_combiner, path_merge_value, path_merge_combiners)

    #For print purpose
    printable_paths = paths.mapValues(lambda x: list(x)).collect()
    print('=== Paths: ===\n' + str(printable_paths))
