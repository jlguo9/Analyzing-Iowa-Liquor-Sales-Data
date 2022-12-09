import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import pandas as pd
stores = pd.read_csv('/Users/Alex/Documents/storeRFMGEO_c.csv')

# Extract the data we're interested in
lat = stores['latitude'].values
lon = stores['longitude'].values
prediction = stores['Cluster'].values
area = stores['Monetary'].values

# 1. Draw the map background
fig = plt.figure(figsize=(8, 8))
m = Basemap(projection='lcc', resolution='h', 
            lat_0=42, lon_0=-93,
            width=0.6E6, height=0.5E6)
m.shadedrelief()
m.drawcoastlines(color='gray')
m.drawcountries(color='gray')
m.drawstates(color='gray')

# 2. scatter city data, with color reflecting population
# and size reflecting area
m.scatter(lon, lat, latlon=True,
          c=(1/prediction)*16, s=(1/prediction)*16,
          cmap='Reds', alpha=0.6)

plt.show()