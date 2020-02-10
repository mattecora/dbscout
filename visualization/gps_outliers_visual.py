from sys import argv

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# Parse the scaling factor
scaling_factor = float(argv[4])

# Read the data
data = pd.read_csv(argv[1], header=None, names=["lat", "long", "alt"])
print("Dataset loaded.")

# Rescale the coordinates
data["lat"] = data["lat"].apply(lambda l : l / scaling_factor)
data["long"] = data["long"].apply(lambda l : l / scaling_factor)

# Read the outliers
outl = pd.read_csv(argv[2], header=None, names=["lat", "long"])
print("Outliers loaded.")

# Rescale the coordinates
outl["lat"] = outl["lat"].apply(lambda l : l / scaling_factor)
outl["long"] = outl["long"].apply(lambda l : l / scaling_factor)

# Plot the world map
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
base = world.plot(figsize=(50,25), color='white', edgecolor='black')

# Plot the dataset and outliers
plt.plot(data["long"], data["lat"], linestyle="none", marker="o", markersize=0.1)
plt.plot(outl["long"], outl["lat"], linestyle="none", marker="o", markersize=0.1)

plt.axis([-180, 180, -90, 90])
plt.savefig(argv[3], bbox_inches="tight")