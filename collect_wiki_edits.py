# Script to consume data and store in SQL Database

from json_encoder import json
from websocket import create_connection
import sqlite3
import time
import datetime

## Set the time to log data from the websocket in hrs
hrs_to_log = 24

## Create connection to the websocket
ws = create_connection("ws://wikimon.hatnote.com:9000")

## Definition of keys to be stored in a SQL db & type of keys
keys_sql = ['id', 'page_title', 'change_size', 'latitude', 'rev_id',\
            'parent_rev_id', 'country_name', 'user', 'longitude',\
            'time', 'action', 'is_anon', 'is_bot', 'is_minor', 'is_new', 'is_unpatrolled']
type_sql = ['INTEGER PRIMARY KEY', 'TEXT', 'INTEGER', 'NUMERIC', \
            'INTEGER', 'INTEGER', 'TEXT', 'TEXT', 'NUMERIC', 'TEXT',\
            'TEXT', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER']

## Create required string from keys and types for SQLite statement
string_sql = ""
for i, key in enumerate(keys_sql):
    string_sql = string_sql + key + ' ' + type_sql[i] + ', '
string_sql = string_sql[:-2]

## Create new SQL db
con = sqlite3.connect('Wiki_Edits_24h.db')
cur = con.cursor()

cur.execute('DROP TABLE IF EXISTS edits')
cur.execute('''CREATE TABLE IF NOT EXISTS edits(''' + string_sql + ''')''')

## Initialisations for acquisiton loop
## While Loop condition to compare time passed with target time
t_start = time.time()
t_diff = time.time() - t_start 
iteration = 0
                
while t_diff < (hrs_to_log*60*60):
    ## Get latest wiki edit from websocket and create python dictionary
    result =  ws.recv()
    result_dict = json.loads(result)
    
    ## Add time and index
    result_dict['time'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y/%m/%d %H:%M:%S')
    result_dict['id'] = iteration
    iteration = iteration + 1
    
    ## Transform bools into ints
    result_dict['is_anon'] = int(result_dict['is_anon'])
    result_dict['is_bot'] = int(result_dict['is_bot'])
    result_dict['is_minor'] = int(result_dict['is_minor'])
    result_dict['is_new'] = int(result_dict['is_new'])
    result_dict['is_unpatrolled'] = int(result_dict['is_unpatrolled'])
    
    ## Get geo data from dict and set country, lat and lon as keys in main dict
    geo_keys = ['country_name', 'longitude', 'latitude']
    if ('geo_ip' in result_dict) and (all (key in result_dict['geo_ip'] for key in geo_keys)):
        geo_dict = result_dict['geo_ip']
        result_dict['country_name']=geo_dict['country_name']
        result_dict['longitude']=float(geo_dict['longitude'])
        result_dict['latitude']=float(geo_dict['latitude'])
    
    ## Reduce dict for SQL db. If key not in results: Add key to dict with "None"
    dict_sql = {}
    for i, key in enumerate(keys_sql):
        if key in result_dict:
            dict_sql[key] = result_dict[key]
        else:
            dict_sql[key] = None
    
    ## Write dictionary keys and values into db
    columns = ', '.join(dict_sql.keys())
    placeholders = ', '.join('?' * len(dict_sql))
    sql = 'INSERT INTO edits ({}) VALUES ({})'.format(columns, placeholders)
    cur.execute(sql, dict_sql.values())    
    
    ## Save the db. No data lost when error etc.
    con.commit()
    t_diff = time.time() - t_start 
    print t_diff
    print (t_diff < hrs_to_log*60*60)

ws.close()