#!/bin/bash
$SPARK_HOME/bin/spark-submit --master spark://$SPARK_MASTER:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1 persistence_s3_job.py
