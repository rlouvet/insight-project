#!/bin/bash
$SPARK_HOME/bin/spark-submit --master spark://$SPARK_MASTER:7077 compute_user_path.py
