"""
    compare_ds.py
    Parse and compare two CSV-formatted datasets.

    Arguments:
    - The first dataset
    - The second dataset
"""

from sys import argv
from pyspark import SparkContext

# Create the Spark context
sc = SparkContext(appName="compare_ds")

# Read the input files
inFile1 = sc.textFile(argv[1])
inFile2 = sc.textFile(argv[2])

# Parse file contents
mappedFile1 = inFile1.map(lambda l : tuple([float(x) for x in l.split(",")]))
mappedFile2 = inFile2.map(lambda l : tuple([float(x) for x in l.split(",")]))

# Compute differences
diff12 = mappedFile1.subtract(mappedFile2)
diff21 = mappedFile2.subtract(mappedFile1)

# Save output
diff12.saveAsTextFile(argv[3] + "_12")
diff21.saveAsTextFile(argv[3] + "_21")

# Close the Spark context
sc.stop()