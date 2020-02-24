"""
    csv_to_ds.py
    Transform a CSV-formatted dataset into a DS-formatted dataset.

    Arguments:
    - The input dataset
    - The output dataset
"""

from sys import argv
from pyspark import SparkContext

# Create the Spark context
sc = SparkContext(appName="csv_to_ds")

# Read the input file
inFile = sc.textFile(argv[1])

# Zip with index
zippedFile = inFile.zipWithIndex()

# Map to DS format
mappedFile = zippedFile.map(lambda p : f"{p[1]} {' '.join(p[0].split(','))}")

# Save output
mappedFile.saveAsTextFile(argv[2])

# Close the Spark context
sc.stop()