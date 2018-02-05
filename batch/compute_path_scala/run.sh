#!/bin/bash
spark-submit --master local --class Main --jars ./lib/CountMinSketchOnSpark-assembly-1.0.jar ./target/scala-2.11/testproject_2.11-1.0.jar --env local --target "/home/robin/Documents/insight/dev/insight-project/local/input/sample-data.json"
