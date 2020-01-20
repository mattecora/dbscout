from sys import argv

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# Read the data
data = pd.read_csv(argv[1], header=None, names=["lat", "long", "alt"])
print("Dataset loaded.")

# Read the outliers
outl = pd.read_csv(argv[2], header=None, names=["lat", "long", "alt"])
print("Outliers loaded.")

# Plot the world map
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
base = world.plot(figsize=(50,25), color='white', edgecolor='black')

# Plot the dataset and outliers
plt.plot(data["long"], data["lat"], linestyle="none", marker="o", markersize=1)
plt.plot(outl["long"], outl["lat"], linestyle="none", marker="o", markersize=1)

plt.axis([-180, 180, -90, 90])
plt.savefig("out.png")