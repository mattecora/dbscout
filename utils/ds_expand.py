"""
    ds_expand.py
    Synthetically expand a dataset by duplicating (with some delta) its contents.

    Arguments:
    - The input dataset
    - The output dataset
    - The duplication factor
    - The maximum delta allowed for each dimension
"""

from random import random
from sys import argv
from pyspark import SparkContext

# Parse params
factor = int(argv[3])
delta = float(argv[4])

# Create the Spark context
sc = SparkContext(appName="ds_expand")

# Read the input file
inFile = sc.textFile(argv[1])

# Duplicate and add random delta
mappedFile = inFile.flatMap(lambda l : [
    [float(x) + delta * (2 * random() - 1) for x in l.split(",")]
    for i in range(factor)
])

# Save output
mappedFile.saveAsTextFile(argv[2])

# Close the Spark context
sc.stop()