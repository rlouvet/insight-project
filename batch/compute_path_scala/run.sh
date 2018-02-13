#!/bin/bash
spark-submit --master $SPARK_MASTER --class Main ./target/scala-2.11/computepath_2.11-1.0.jar --env aws --target "20180130T125955-1517319"
