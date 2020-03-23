"""
    knn.py
    Draw the k-nearest neighbor distance graph for a dataset.

    Arguments:
    - The input dataset
    - The number of dimensions
    - The value of k
"""

from sys import argv

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from sklearn.neighbors import NearestNeighbors

dim = int(argv[2])
k = int(argv[3])

# Read input
data = pd.read_csv(argv[1], usecols=list(range(dim))).to_numpy()

# Compute nearest neighbors
knn = NearestNeighbors(n_neighbors=k).fit(data)

# Sort distances
dist = np.sort(knn.kneighbors(data)[0], axis=0)[:, k - 1]

# Plot distances
plt.plot(dist)
plt.show()