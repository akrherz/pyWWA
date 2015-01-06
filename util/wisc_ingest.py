import subprocess
import os
import glob
import mx.DateTime

sts = mx.DateTime.DateTime(2011,12,1)
ets = mx.DateTime.DateTime(2012,1,1)

WANT = ['EAST-CONUS','NHEM-COMP','SUPER-NATIONAL','NHEM-MULTICOMP','WEST-CONUS']

def dodate(now, dir):
    base = now.strftime("/mesonet/gini/%Y_%m_%d/sat/"+dir)
    for (d2,bogus,files) in os.walk(base):
        if len(files) == 0:
            continue
        for file in files:
            cmd = "cat %s/%s | /usr/bin/python gini2gis.py" % (d2, file)
            print cmd
            subprocess.call(cmd, shell=True)

now = sts
while now < ets:
    for dir in WANT:
        dodate(now, dir)

    now += mx.DateTime.RelativeDateTime(days=1)
