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

algs = {
    "ddlof": {
        "name": "DDLOF (16 GB)",
        "color": "C4"
    },
    "rp": {
        "name": "RP-DBSCAN (8 GB)",
        "color": "C0"
    },
    "rp16": {
        "name": "RP-DBSCAN (16 GB)",
        "color": "C1"
    },
    "new": {
        "name": "DBSCOUT (8 GB)",
        "color": "C2"
    },
    "new16": {
        "name": "DBSCOUT (16 GB)",
        "color": "C3"
    }
}

# Use LaTeX for rendering and increase font size
plt.rc("text", usetex=True)
plt.rc("font", size=14)

# Figure setup
plt.figure(figsize=(6, 4.5))
plt.subplots_adjust(bottom=0.15, top=0.95, left=0.15, right=0.95)

# Read data
data = pd.read_csv(argv[3], header=None, names=["alg", "x", "y"])

# List algorithms to plot
algs_to_plot = [a for a in algs if len(data[data["alg"] == a]) > 0]

for alg in algs_to_plot:
    # Compute mean and stddev
    means = data[data["alg"] == alg].groupby(["x"], as_index=False).mean()
    stddev = data[data["alg"] == alg].groupby(["x"], as_index=False).std()

    # Plot data and error bars
    plt.errorbar(means["x"], means["y"], yerr=stddev["y"], marker="o", elinewidth=0.5, capsize=2, color=algs[alg]["color"])

# Set the logarithmic scale if needed
if argv[1] == "xlog":
    plt.xscale("log", base=2)
elif argv[1] == "ylog":
    plt.yscale("log", base=2)
elif argv[1] == "xylog":
    plt.xscale("log", base=2)
    plt.yscale("log", base=2)

# Decorate plot and save
plt.xticks(ticks=data["x"].unique(), labels=[f"${x}$" for x in data["x"].unique()])

if "eps" == argv[2]:
    plt.xlabel("$\epsilon$")
elif "n" == argv[2]:
    plt.xlabel("Sample fraction")
elif "np" == argv[2]:
    plt.xlabel("Number of partitions")

plt.ylabel("Running time (s)")
plt.legend([algs[alg]["name"] for alg in algs_to_plot])
plt.grid(color="#d3d3d3")
plt.savefig(argv[4], dpi=300)