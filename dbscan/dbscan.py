import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import DBSCAN

# Read dataset
data = pd.read_csv(sys.argv[1])

# Create DBSCAN instance
dbscan = DBSCAN(eps=float(sys.argv[3]), min_samples=int(sys.argv[4]))

# Get labels
labels = dbscan.fit_predict(data)

# Select outliers
data_ol = data[labels == -1]
data_ol.to_csv(sys.argv[2], index=False)