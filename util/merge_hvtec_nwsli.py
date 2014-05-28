"""
Script that merges the upstream HVTEC NWSLI information into the database. We
are called like so

python merge_hvtec_nwsli.py NWSLI_List_March_1_2013.txt

Whereby the argument to the script is the filename stored here:

http://www.nws.noaa.gov/os/vtec/hydro_vtec.shtml

hvtec_nwsli table:
 nwsli      | character(5)           | 
 river_name | character varying(128) | 
 proximity  | character varying(16)  | 
 name       | character varying(128) | 
 state      | character(2)           | 
 geom       | geometry               | 
"""
import os
import sys

import urllib2
import psycopg2
if len(sys.argv) < 2:
    print 'USAGE: python merge_hvtec_nwsli.py FILENAME'
    sys.exit(0)

dbconn = psycopg2.connect(database='postgis',
                           host='iemdb')
cursor = dbconn.cursor()
print ' - Connected to database: postgis'

fn = sys.argv[1]
uri = "http://www.nws.noaa.gov/os/vtec/hydro/%s" % (fn,)

print ' - Fetching file: %s' % (uri,)
req = urllib2.Request(uri)
response = urllib2.urlopen(req)
lines = response.readlines()
updated = 0
new = 0
for line in lines:
    if line.strip() == "":
        continue
    tokens = line.split("\t")
    if len(tokens) != 7:
        print ' + Bad Line found in file: '+ line
        continue
    (nwsli, river_name, proximity, name, state, lat, lon)  = tokens
    if len(nwsli) != 5:
        print ' + Line with bad NWSLI: '+ line
        continue
    cursor.execute("DELETE from hvtec_nwsli WHERE nwsli = '%s'" % (nwsli,))
    if cursor.rowcount == 1:
        updated += 1
    else:
        new += 1
    sql  = """INSERT into hvtec_nwsli (nwsli, river_name, proximity, name, 
         state, geom) values (%s, %s, %s, %s, %s, 
         'SRID=4326;POINT(%s %s)')""" 
    args = (nwsli, river_name, proximity, name, state, float(lon), 
            float(lat))
    cursor.execute(sql, args)

cursor.close()
dbconn.commit()
print ' - DONE! %s updated %s new' % (updated, new)

