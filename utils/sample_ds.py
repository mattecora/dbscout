from sys import argv
from pyspark import SparkContext

# Create the Spark context
sc = SparkContext(appName="sample_ds")

# Read the input file
inFile = sc.textFile(argv[1])

# Sample the input lines
sampledFile = inFile.sample(False, float(argv[3]), 0)

# Save output
sampledFile.saveAsTextFile(argv[2])

# Close the Spark context
sc.stop()