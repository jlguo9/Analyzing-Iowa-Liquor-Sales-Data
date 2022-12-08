import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import pandas as pd
stores = pd.read_csv('/Users/Alex/Documents/GitHub/3_datamen_CMPT_732_project/src/Q2_RFM_Cluster/storeRFMGEO.csv')

# Extract the data we're interested in
lat = stores['latitude'].values
lon = stores['longitude'].values
prediction = stores['prediction'].values
area = stores['Monetary'].values

# 1. Draw the map background
fig = plt.figure(figsize=(8, 8))
m = Basemap(projection='lcc', resolution='h', 
            lat_0=42, lon_0=-92,
            width=1.2E6, height=1E6)
m.shadedrelief()
m.drawcoastlines(color='gray')
m.drawcountries(color='gray')
m.drawstates(color='gray')

# 2. scatter city data, with color reflecting population
# and size reflecting area
m.scatter(lon, lat, latlon=True,
          c=prediction, s=area,
          cmap='Reds', alpha=0.5)

# 3. create colorbar and legend
plt.colorbar(label=r'({\rm prediction})$')
plt.clim(3, 7)

# make legend with dummy points
for a in [100, 300, 500]:
    plt.scatter([], [], c='k', alpha=0.5, s=a,
                label=str(a) + ' km$^2$')
plt.legend(scatterpoints=1, frameon=False,
           labelspacing=1, loc='lower left');
plt.show()