# Script to analyse 24h wikipedia activity

import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates

## Function to retrieve grouped data from SQL db
def country_bags(country):  
    cur.execute('''SELECT time, COUNT(country_name) FROM edits
                WHERE country_name = ''' +country+ ''' 
                GROUP BY 
                    strftime('%Y', time), 
                    strftime('%m', time),
                    strftime('%d', time),
                    strftime('%H', time)
                ORDER BY time''')
    country_bags = cur.fetchall()
    return country_bags

## Function to add data to plot 
def plot_data(time, edit_rel, country, color):
    ax.plot(time, edit_rel, label = country.strip("'"), color = color)

## Plot Settings
f = plt.figure(1,figsize=(7, 4))
ax = f.add_subplot(111)

plt.title('24h English Wikipedia Activity')
ax.set_xlabel("Time [HH:MM]")
ax.set_ylabel("Wikipedia Contribution\n[Edits per 1M People and per Hour]")
plt.xticks(rotation=45)

ts_start = datetime.strptime('2017-11-30 21:00:00', '%Y-%m-%d %H:%M:%S')
ts_end = datetime.strptime('2017-12-01 22:00:00', '%Y-%m-%d %H:%M:%S')
ax.set_xlim(ts_start, ts_end)
ax.set_ylim(0, 5)

plt.grid(True)

xfmt = mdates.DateFormatter('%H:%M')
ax.xaxis.set_major_formatter(xfmt)

## Country order and colour
country_list = ["'United States'", "'United Kingdom'", "'Australia'"]
colour_list = ['darkblue', 'orange', 'skyblue']

## Populations and time zone offsets for US, UK & AUS
pop_in_million = [323, 65, 24]
country_offset = [6, 0, 13]

## Connect to SQL Database
con = sqlite3.connect('Wiki_Edits_24h.db')
cur = con.cursor()

## Change time format and offset time to full hour
cur.execute('''UPDATE edits SET time = replace( time, '/', '-')''')
cur.execute('''UPDATE edits SET time = DATETIME(time, '+767 seconds')''')

## Retrieve data from SQL database and offset the results based on country
for i, country in enumerate(country_list):
    data = country_bags(country)
    time = [row[0] for row in data]
    
    ## Edits per 1Million people
    edit_count = [float(row[1])/pop_in_million[i] for row in data]
    
    ##Restructure for same time base
    edit_count_offset = edit_count[country_offset[i]:] + edit_count[:country_offset[i]]
    
    ## Add date to plot
    plot_data(time, edit_count_offset, country_list[i], colour_list[i])

## Close the SQL db connection
con.close()

## Save and show the graph
plt.tight_layout()
plt.legend()
#plt.savefig('Wiki_24h_Activity.png', format='png', dpi=1000, bbox_inches="tight")
plt.show()