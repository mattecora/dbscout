import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_moons

noise_frac = 0.01
n = 10000

# Generate data
data = make_moons(n_samples=int(n * (1 - noise_frac)), noise=0.01, random_state=100)

# Generate noise
np.random.seed(0)
noise = np.random.uniform(-1.5, 1.5, size=(int(n * noise_frac), 2))

# Add noise
noisy_data = np.append(data[0]  - np.array([0.5, 0.25]), noise, axis=0)

# Generate labels
labels = np.append(data[1], ["noise"] * int(n * noise_frac), axis=0)

# Save data to a CSV file
with open("moons.csv", "w") as f:
    for i in range(len(noisy_data)):
        f.write(f"{noisy_data[i][0]},{noisy_data[i][1]},{labels[i]}\n")

# Plot the data
plt.plot([x[0] for x in noisy_data], [x[1] for x in noisy_data], linestyle="none", marker="x", fillstyle="none")
plt.show()