"""
    perf_plot.py
    Generate a performance plot (x-vs-running time).

    Arguments:
    - The plot scale (lin, xlog, ylog, xylog)
    - The type of plot (eps, n, np)
    - The CSV performance file
    - The output file
"""

from sys import argv
from math import floor, ceil, log2

import matplotlib.pyplot as plt
import pandas as pd

# Use LaTeX for rendering
plt.rc("text", usetex=True)

# Read data
data = pd.read_csv(argv[3], header=None, names=["alg", "x", "y"])

for alg in ["rp", "new", "new16"]:
    # Compute mean and stddev
    means = data[data["alg"] == alg].groupby(["x"], as_index=False).mean()
    stddev = data[data["alg"] == alg].groupby(["x"], as_index=False).std()

    # Plot data and error bars
    plt.errorbar(means["x"], means["y"], yerr=stddev["y"], marker="o", elinewidth=0.5, capsize=2)

# Set the logarithmic scale if needed
if argv[1] == "xlog":
    plt.xscale("log", basex=2)
elif argv[1] == "ylog":
    plt.yscale("log", basey=2)
elif argv[1] == "xylog":
    plt.xscale("log", basex=2)
    plt.yscale("log", basey=2)

# Decorate plot and save
plt.xticks(ticks=data["x"].unique(), labels=[f"${x}$" for x in data["x"].unique()])

if "eps" == argv[2]:
    plt.xlabel("$\epsilon$")
elif "n" == argv[2]:
    plt.xlabel("Sample fraction")
elif "np" == argv[2]:
    plt.xlabel("Number of partitions")

plt.ylabel("Running time (s)")
plt.legend(["RP-DBSCAN", "New algorithm (8 GB)", "New algorithm (16 GB)"])
plt.grid(color="#d3d3d3")
plt.savefig(argv[4], dpi=300)