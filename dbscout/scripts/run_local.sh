#!/bin/bash

# Environment configuration
threads="24"
memory="256G"
jarpath="../target/thesis-code-1.0.jar"
sparkopts="--class dbscout.App --master local[$threads] --driver-memory $memory --conf spark.driver.maxResultSize=0 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"

# Submit job
spark-submit $sparkopts $jarpath $@