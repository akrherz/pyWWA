"""
  Compare our PIREPs data against what is at aviation wx JSON service
"""
from __future__ import print_function
import datetime

import psycopg2
import requests

pgconn = psycopg2.connect(database='postgis', user='nobody', host='iemdb')
cursor = pgconn.cursor()

avwx = requests.get("http://aviationweather.gov/gis/scripts/AirepJSON.php")
avwx = avwx.json()

mine = {}
cursor.execute("""SELECT valid at time zone 'UTC', ST_x(geom::geometry),
ST_y(geom::geometry), report
from pireps WHERE valid > '2015-08-21 07:00'""")
for row in cursor:
    key = "/".join(row[3].replace(" ", "").split("/")[:3])
    mine[key] = row

floor = None
for feature in avwx['features']:
    if feature['properties']['airepType'] != 'PIREP':
        continue
    ts = datetime.datetime.strptime(feature['properties']['obsTime'],
                                    '%Y-%m-%dT%H:%M:%SZ')
    if floor is None:
        floor = ts
    lon, lat = feature['geometry']['coordinates']
    key = "/".join(feature['properties']['rawOb'].replace(" ",
                                                          "").split("/")[:3])
    if key not in mine:
        print('IEM  MISS %s %s' % (ts, feature['properties']['rawOb']))
    else:
        error = ((mine[key][1]-lon)**2 + (mine[key][2]-lat)**2)**.5
        if error > 0.1:
            location = "/".join(feature['properties']['rawOb'].split("/")[:2])
            print(("ERROR: %5.2f IEM: %8.3f %6.3f AWX: %7.2f %5.2f %s"
                   ) % (error, mine[key][1], mine[key][2],
                        lon, lat, location))
        del mine[key]

for key in mine.keys():
    if mine[key][0] < floor:
        continue
    print('AVWX MISS %s %s' % (mine[key][0], mine[key][3]))
