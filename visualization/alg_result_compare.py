"""
    alg_result_compare.py
    Visualize and compare the results of multiple algorithms.

    Arguments:
    - The name of the dataset
    - The input folder
    - The output file
"""

from math import sqrt
from os.path import basename
from sys import argv

import matplotlib.pyplot as plt
import pandas as pd

from sklearn.metrics import adjusted_rand_score

algshort = ["new", "dbscan", "ee", "if", "lof", "svm"]
algnames = ["New algorithm", "DBSCAN", "Robust Covariance Estimation", "Isolation Forest", "Local Outlier Factor", "One-class SVM"]

# Use LaTeX for rendering
plt.rc("text", usetex=True)

# Create subplots
num_rows = 3
num_cols = 2
num_plots = num_rows * num_cols
fig, subplots = plt.subplots(num_rows, num_cols, figsize=(10, 15))

for i in range(0, num_rows):
    for j in range(0, num_cols):
        n = i * num_cols + j

        # Read the algorithm output
        data = pd.read_csv(f"{argv[2]}/{argv[1]}_{algshort[n]}.txt", header=None, names=["x", "y", "label", "prediction"])

        # Select detected/undetected data/outliers
        data_found = data[(data["label"] == 1) & (data["prediction"] == 1)]
        data_not_found = data[(data["label"] == 1) & (data["prediction"] == -1)]
        outl_found = data[(data["label"] == -1) & (data["prediction"] == -1)]
        outl_not_found = data[(data["label"] == -1) & (data["prediction"] == 1)]

        # Plot the dataset and outliers
        subplots[i][j].plot(data_found["x"], data_found["y"], linestyle="none", marker="x", fillstyle="none")
        subplots[i][j].plot(outl_found["x"], outl_found["y"], linestyle="none", marker="o", fillstyle="none")
        subplots[i][j].plot(data_not_found["x"], data_not_found["y"], linestyle="none", marker="s", fillstyle="none")
        subplots[i][j].plot(outl_not_found["x"], outl_not_found["y"], linestyle="none", marker="^", fillstyle="none")

        # Decorate subplot (title and Rand)
        subplots[i][j].set_title(algnames[n])
        subplots[i][j].annotate(f"Rand index: {adjusted_rand_score(data['label'], data['prediction']):.5f}", (0.5, -0.125), xycoords="axes fraction", ha="center")

# Decorate plot and save
fig.suptitle(f"{argv[1]}.csv")
fig.legend(["Detected data points", "Detected outliers", "Undetected data points", "Undetected outliers"])
fig.subplots_adjust(left=0.05, right=0.95, bottom=0.05, top=0.95, wspace=0.25, hspace=0.25)
plt.savefig(argv[3], dpi=300)