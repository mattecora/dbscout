"""
    labeled_outliers_visual.py
    Simple visualization script for generic outliers in labeled datasets.

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

# Join data and outliers
data_outl = pd.merge(data, outl, how="left", indicator="outlier")

# Select detected/undetected data/outliers
data_found = data_outl[(data_outl["outlier"] != "both") & (data_outl["label"] != "noise")]
data_not_found = data_outl[(data_outl["outlier"] == "both") & (data_outl["label"] != "noise")]
outl_found = data_outl[(data_outl["outlier"] == "both") & (data_outl["label"] == "noise")]
outl_not_found = data_outl[(data_outl["outlier"] != "both") & (data_outl["label"] == "noise")]

# Plot the dataset and outliers
plt.plot(data_found["x"], data_found["y"], linestyle="none", marker="x", fillstyle="none")
plt.plot(outl_found["x"], outl_found["y"], linestyle="none", marker="o", fillstyle="none")
plt.plot(data_not_found["x"], data_not_found["y"], linestyle="none", marker="s", fillstyle="none")
plt.plot(outl_not_found["x"], outl_not_found["y"], linestyle="none", marker="^", fillstyle="none")

# Decorate plot and save
plt.title(f"{basename(argv[1])}\neps = {argv[3]}, minPts = {argv[4]}")
plt.legend(["Detected data points", "Detected outliers", "Undetected data points", "Undetected outliers"], loc="upper right")
plt.savefig(argv[5], dpi=300)