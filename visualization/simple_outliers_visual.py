"""
    simple_outliers_visual.py
    Simple visualization script for generic outliers.

    Arguments:
    - The complete dataset
    - The list of outliers
    - The value of eps
    - The value of minPts
    - The output file
"""

from os.path import basename
from sys import argv

import matplotlib.pyplot as plt
import pandas as pd

# Use LaTeX for rendering
plt.rc("text", usetex=True)

# Read the data
data = pd.read_csv(argv[1], header=None, names=["x", "y", "label"])
print("Dataset loaded.")

# Read the outliers
outl = pd.read_csv(argv[2], header=None, names=["x", "y"])
print("Outliers loaded.")

# Remove outliers
data_outl = pd.merge(data, outl, how="left", indicator="outlier")
data_no_outl = data[data_outl["outlier"] != "both"]

# Plot the dataset and outliers
plt.plot(data_no_outl["x"], data_no_outl["y"], linestyle="none", marker="x", fillstyle="none")
plt.plot(outl["x"], outl["y"], linestyle="none", marker="o", fillstyle="none")

# Decorate plot and save
plt.title(f"{basename(argv[1])}\neps = {argv[3]}, minPts = {argv[4]}")
plt.legend(["Data points", "Outliers"], loc="upper right")
plt.savefig(argv[5], bbox_inches="tight", dpi=300)