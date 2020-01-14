import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_blobs

# Generate data
data = make_blobs(n_samples=10000, n_features=2, centers=5, cluster_std=1.5, random_state=100)

# Save data to a CSV file
np.savetxt("blobs.csv", data[0], delimiter=",", fmt="%.18f", header="x1,x2", comments="")

# Plot the data
plt.plot([x[0] for x in data[0]], [x[1] for x in data[0]], linestyle="none", marker="x", fillstyle="none")
plt.show()