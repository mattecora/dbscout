#!/bin/bash

# Environment configuration
jarpath="target/thesis-code-1.0.jar"
sparkopts="--class it.polito.s256654.thesis.App --master yarn --driver-memory 8G --executor-memory 8G --conf spark.driver.maxResultSize=0 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.network.timeout=300s"

# Parameters configuration
dim=2
minpts=100

# Parse input path
fin=$1
shift

# Parse output path
fout=$1
shift

# Parse retries
retries=$1
shift

for i in $@
do
    for j in $(seq $retries)
    do
        echo "====================================="
        echo "Running with eps = $i for the $j time"
        echo "====================================="

        # Sumbit job
        spark-submit $sparkopts $jarpath --inputPath $fin --outputPath $fout --dim $dim --eps $i --minPts $minpts

        # Remove results
        hdfs dfs -rm -r $fout

        # Sleep and retry
        sleep 5
    done
done