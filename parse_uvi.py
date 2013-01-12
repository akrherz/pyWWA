"""
Parse UVI data into something WXC can use
"""

import sys
import subprocess
import os
import datetime
now = datetime.datetime.now()

cdict = {'DES MOINES IA': [41.53, -93.65,'KDSM'],
 'CHICAGO IL': [41.98, -87.93,'KORD'],
}

out = open("/tmp/wxc_uvi.txt", 'w')
out.write("""Weather Central 001d0300 Surface Data
   6
   4 Station
  30 Location
   6 Lat
   7 Lon
   2 UVI
   2 BOGUS
""" )

def writer(city, st, uv):
    key = "%s %s" % (city, st)
    if not cdict.has_key(key):
        return

    out.write("%4s %-30s %6s %7s %2s 99\n" % (cdict[key][2], key, 
                                              cdict[key][0], cdict[key][1], 
                                              uv) )

d = sys.stdin.read()
data = d.replace("\\015\015\012", "\n")

lines = data.split("\n")

for line in lines[26:]:
    if len(line) < 50:
        continue
    city = line[:21].strip()
    st = line[21:24].strip()
    uv = line[27:30].strip()
    writer(city, st, uv)

    city = line[37:57].strip()
    st = line[57:60].strip()
    uv = line[63:].strip()
    writer(city, st, uv)

out.close()
subprocess.call("/home/ldm/bin/pqinsert -p 'wxc_uvi.txt' /tmp/wxc_uvi.txt",
                shell=True)

os.unlink('/tmp/wxc_uvi.txt')

