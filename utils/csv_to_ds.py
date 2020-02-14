from sys import argv
from pyspark import SparkContext

# Create the Spark context
sc = SparkContext(appName="csv_to_ds")

# Read the input file
inFile = sc.textFile(argv[1])

# Zip with index
zippedFile = inFile.zipWithIndex()

# Map to DS format
mappedFile = zippedFile.map(lambda p : f"{p[1]} {p[0].split(',')[0]} {p[0].split(',')[1]}")

# Save output
mappedFile.saveAsTextFile(argv[2])

# Close the Spark context
sc.stop()