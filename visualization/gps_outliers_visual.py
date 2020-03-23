"""
    gps_outliers_visual.py
    Visualization script for GPS outliers.

    Arguments:
    - The complete dataset
    - The list of outliers
    - The scaling factor
    - The value of eps
    - The value of minPts
    - The output file
"""

from os.path import basename
from sys import argv

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# Use LaTeX for rendering
plt.rc("text", usetex=True)

# Parse the scaling factor
scaling_factor = float(argv[3])

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

# Decorate plot and save
plt.axis([-180, 180, -90, 90])
plt.title(f"geolife.csv\neps = {argv[4]}, minPts = {argv[5]}", fontsize=40)
legend = plt.legend(["Data points", "Outliers"], loc="upper right", fontsize=30)
plt.savefig(argv[6], bbox_inches="tight")