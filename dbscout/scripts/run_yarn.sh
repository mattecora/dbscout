#!/bin/bash

# Environment configuration
jarpath="../target/thesis-code-1.0.jar"
sparkopts="--class dbscout.App --master yarn --driver-memory 8G --executor-memory 8G --conf spark.driver.maxResultSize=0 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"

# Submit job
spark-submit $sparkopts $jarpath $@