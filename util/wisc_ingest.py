import subprocess
import os
import mx.DateTime

sts = mx.DateTime.DateTime(2011, 12, 1)
ets = mx.DateTime.DateTime(2012, 1, 1)

WANT = ['EAST-CONUS', 'NHEM-COMP', 'SUPER-NATIONAL', 'NHEM-MULTICOMP',
        'WEST-CONUS']


def dodate(now, mydir):
    base = now.strftime("/mesonet/gini/%Y_%m_%d/sat/" + mydir)
    for (d2, _, files) in os.walk(base):
        if len(files) == 0:
            continue
        for fn in files:
            cmd = "cat %s/%s | /usr/bin/python gini2gis.py" % (d2, fn)
            print cmd
            subprocess.call(cmd, shell=True)

now = sts
while now < ets:
    for mydir in WANT:
        dodate(now, mydir)

    now += mx.DateTime.RelativeDateTime(days=1)
