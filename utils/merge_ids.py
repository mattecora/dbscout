"""
    merge_ids.py
    Merge an ID file with the corresponding features file.

    Arguments:
    - The complete dataset
    - The ID file
    - The output file
"""

from sys import argv
from pyspark import SparkContext

# Create the Spark context
sc = SparkContext(appName="merge_ids")

# Read the input files
dataset = sc.textFile(argv[1])
ids = sc.textFile(argv[2])

# Zip with index first file
indexedDataset = dataset.zipWithIndex().map(lambda p : (p[1], p[0]))

# Parse second file contents
parsedIds = ids.map(lambda l : (int(l.split(" ")[0]), l))

# Join indices
joinedIds = parsedIds.join(indexedDataset).map(lambda p : p[1][1])

# Save output
joinedIds.saveAsTextFile(argv[3])

# Close the Spark context
sc.stop()