# Script to analyse the location of wikipedia activity

import sqlite3
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt

## Connect to SQL db
con = sqlite3.connect('Wiki_Edits_24h.db')
cur = con.cursor()

## Load counts of country in SQL db: English Wiki only?
cur.execute('''SELECT country_name, COUNT(country_name) FROM edits
            WHERE country_name IS NOT NULL
            GROUP BY country_name
            ORDER BY COUNT(country_name) DESC
            LIMIT 10''')
country_count = cur.fetchall()
print zip(*country_count)

## Load long, lat, change_size information from SQL db
cur.execute('''SELECT longitude, latitude, change_size FROM edits
            WHERE country_name IS NOT NULL
            AND change_size IS NOT NULL
            ORDER BY change_size''')
output = cur.fetchall()

lon = [row[0] for row in output]
lat = [row[1] for row in output]
change_size = [row[2] for row in output]

## Close SQL db connection
con.close()

## Create a world map and plot wiki data on top of it 
m = Basemap(projection='mill', llcrnrlat=-60, urcrnrlat = 90, \
            llcrnrlon=-180, urcrnrlon=180, resolution='c')
m.drawcoastlines()
m.drawcountries()
m.fillcontinents(color = 'lightblue', zorder =0.05)

## Size and colour to be related with change_size
s = [i*0.05 for i in change_size]
c = ['r' if i<0 else 'b' for i in change_size]

x, y = m(lon, lat)
m.scatter(x, y, marker='o',color= c, alpha = 0.04, s = s)
plt.title('Wikipedia Contributors World Map')

## Save and display map
#plt.savefig('Wiki_World_Map_1000.png', format='png', dpi=1000, bbox_inches="tight")
plt.show()