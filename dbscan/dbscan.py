from sys import argv
from time import time

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from sklearn.cluster import DBSCAN

# Get the start time
start_time = time()

# Read dataset
data = pd.read_csv(argv[1])

# Create DBSCAN instance
dbscan = DBSCAN(eps=float(argv[3]), min_samples=int(argv[4]), n_jobs=-1)

# Get labels
labels = dbscan.fit_predict(data)

# Select outliers
data_ol = data[labels == -1]
data_ol.to_csv(argv[2], index=False)

# Print the execution time
print(f"Execution time: {time() - start_time} seconds")