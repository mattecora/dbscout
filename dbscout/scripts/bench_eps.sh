#!/bin/bash

# Environment configuration
jarpath="../target/thesis-code-1.0.jar"
sparkopts="--class dbscout.App --master yarn --num-executors 100 --executor-cores 1 --driver-memory 8G --executor-memory 8G --conf spark.driver.maxResultSize=0 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.dynamicAllocation.enabled=false"

# Parse input path
fin=$1
shift

# Parse output path
fout=$1
shift

# Parse dim
dim=$1
shift

# Parse minpts
minpts=$1
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
        spark-submit $sparkopts $jarpath \
            --algClass dbscout.algorithm.parallel.GroupedOutlierDetector \
            --inputPath $fin --outputPath $fout --dim $dim --eps $i --minPts $minpts

        # Remove results
        hdfs dfs -rm -r $fout

        # Sleep and retry
        sleep 5
    done
done