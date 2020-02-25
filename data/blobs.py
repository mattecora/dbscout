import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_blobs

noise_frac = 0.01
n = 1000000

# Generate data
data = make_blobs(n_samples=int(n * (1 - noise_frac)), n_features=2, centers=5, cluster_std=0.5, random_state=100)

# Generate noise
noise = np.random.uniform(-15, 15, size=(int(n * noise_frac), 2))

# Add noise
noisy_data = np.append(data[0], noise, axis=0)

# Save data to a CSV file
np.savetxt("blobs.csv", noisy_data, delimiter=",", fmt="%.18f", header="x1,x2", comments="")

# Plot the data
plt.plot([x[0] for x in noisy_data], [x[1] for x in noisy_data], linestyle="none", marker="x", fillstyle="none")
plt.show()