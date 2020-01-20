import os
import sys

# Open the output file
with open("geolife.csv", "w") as outf:
    # Write the CSV header
    outf.write("lat,long,alt\n")
    # Perform a walk in the input directories
    for root, dirs, files in os.walk(sys.argv[1]):
        # Check all files
        for filename in files:
            # If the file is a .plt file
            if filename.endswith(".plt"):
                # Construct the whole path
                path = os.path.join(root, filename)
                # Open the file for reading
                with open(path, "r") as inf:
                    # Write all the lines (except the header) to the output file
                    for line in inf.readlines()[6:]:
                        tokens = line.split(",")
                        outf.write(",".join([tokens[0], tokens[1], tokens[3]]) + "\n")