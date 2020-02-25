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
data = pd.read_csv(argv[1], header=None)
print("Dataset loaded.")

# Read the outliers
outl = pd.read_csv(argv[2], header=None)
print("Outliers loaded.")

# Plot the dataset and outliers
plt.plot(data[data.columns[0]], data[data.columns[1]], linestyle="none", marker="o", markersize=1)
plt.plot(outl[outl.columns[0]], outl[outl.columns[1]], linestyle="none", marker="o", markersize=1)

# Decorate plot and save
plt.title(f"{basename(argv[1])}\neps = {argv[3]}, minPts = {argv[4]}")
plt.savefig(argv[5], bbox_inches="tight")