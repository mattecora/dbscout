#!/bin/bash

# Environment configuration
jarpath="../target/thesis-code-1.0.jar"
sparkopts="--class it.polito.s256654.thesis.App --master yarn --driver-memory 8G --executor-memory 8G --conf spark.driver.maxResultSize=0 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.network.timeout=300s"

# Submit job
spark-submit $sparkopts $jarpath $@