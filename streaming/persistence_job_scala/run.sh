#!/bin/bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1 --class Main ./target/scala-2.11/persistencejob_2.11-1.0.jar
