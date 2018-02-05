#!/bin/bash
#Create Kafka topics
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic clickstreams-topic
