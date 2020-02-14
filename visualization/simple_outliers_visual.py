from os.path import basename
from sys import argv

import matplotlib.pyplot as plt
import pandas as pd

# Use LaTeX for rendering
plt.rc("text", usetex=True)

# Read the data
data = pd.read_csv(argv[1], header=None, names=["x0", "x1", "class"])
print("Dataset loaded.")

# Read the outliers
outl = pd.read_csv(argv[2], header=None, names=["x0", "x1"])
print("Outliers loaded.")

# Plot the dataset and outliers
plt.plot(data["x0"], data["x1"], linestyle="none", marker="o", markersize=1)
plt.plot(outl["x0"], outl["x1"], linestyle="none", marker="o", markersize=1)

# Decorate plot and save
plt.title(f"{basename(argv[1])}\neps = {argv[3]}, minPts = {argv[4]}")
plt.savefig(argv[5], bbox_inches="tight")