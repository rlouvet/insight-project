$SPARK_HOME/bin/spark-submit --master spark://$SPARK_MASTER:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 persistence_hdfs_job.py
