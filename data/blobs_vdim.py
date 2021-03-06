import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_blobs

noise_frac = 0.01
n = 10000

for dim in range(2, 10):
    # Generate data
    data = make_blobs(n_samples=int(n * (1 - noise_frac)), n_features=dim, centers=5, cluster_std=0.5, random_state=100)

    # Generate noise
    np.random.seed(0)
    noise = np.random.uniform(-15, 15, size=(int(n * noise_frac), dim))

    # Add noise
    noisy_data = np.append(data[0], noise, axis=0)

    # Generate labels
    labels = np.append(data[1], ["noise"] * int(n * noise_frac), axis=0)

    # Save data to a CSV file
    with open(f"blobs_{dim}.csv", "w") as f:
        for i in range(len(noisy_data)):
            f.write(f"{noisy_data[i][0]},{noisy_data[i][1]},{labels[i]}\n")