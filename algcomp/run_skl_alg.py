"""
    run_skl_alg.py
    Run a scikit-learn algorithm on the given dataset.

    Arguments:
    - The algorithm to use (dbscan, ee, if, lof, svm)
    - The input dataset
    - The output dataset
    - The algorithm parameters (eps and minpts / contamination factor)
"""

from sys import argv
from time import time

import pandas as pd

from sklearn.cluster import DBSCAN
from sklearn.covariance import EllipticEnvelope
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.svm import OneClassSVM
from sklearn.metrics import f1_score

# Get the start time
start_time = time()

# Read dataset
data = pd.read_csv(argv[2], header=None)

# Instantiate algorithm
if argv[1] == "dbscan":
    alg = DBSCAN(eps=float(argv[4]), min_samples=int(argv[5]))
elif argv[1] == "ee":
    alg = EllipticEnvelope(contamination=float(argv[4]), random_state=100)
elif argv[1] == "if":
    alg = IsolationForest(contamination=float(argv[4]), random_state=100)
elif argv[1] == "lof":
    alg = LocalOutlierFactor(contamination=float(argv[4]), n_neighbors=int(argv[5]))
elif argv[1] == "svm":
    alg = OneClassSVM(nu=float(argv[4]))

# Get labels
data[data.columns[-1]] = data[data.columns[-1]].apply(lambda v : -1 if v == "noise" else 1)
true_labels = data[data.columns[-1]].values
pred_labels = alg.fit_predict(data[data.columns[:-1]].values)

# For DBSCAN, assign same label to all clusters
if argv[1] == "dbscan":
    pred_labels[pred_labels != -1] = 1

# Save outliers to file
data["predicted"] = pred_labels
data.to_csv(argv[3], header=None, index=False)

# Print the execution time
print(f"Execution time: {time() - start_time} seconds")
print(f"Number of outliers: {sum([1 for i in pred_labels if i == -1])}")
print(f"F1 score: {f1_score(true_labels, pred_labels, pos_label=-1)}")