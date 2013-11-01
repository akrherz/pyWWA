'''
 Feed archived NCR data to nexrad3_attr.py , should be run like
 
 python util/feed_archived_ncr.py NEXRAD YYYY MM | YYYY=2009 MM=01 python nexrad3_attr.py
'''

import subprocess
import datetime
import sys
import os
import time
import glob
if not os.path.isdir("/tmp/l3tmp"):
    os.makedirs("/tmp/l3tmp")

nexrad = sys.argv[1]

sts = datetime.datetime( int(sys.argv[2]), int(sys.argv[3]), 1)
ets = sts + datetime.timedelta(days=32)
ets = ets.replace(day=1)
interval = datetime.timedelta(days=1)

now = sts
while now < ets:
    afn = now.strftime("/mesonet/ARCHIVE/nexrad/%Y_%m/"+nexrad+"_%Y%m%d.tgz")
    if not os.path.isfile(afn):
        sys.stderr.write('Missing %s\n' % (afn,))
        now += interval
        continue
    
    subprocess.call("tar -x -z -C /tmp/l3tmp -f %s" % (afn,), shell=True)
    if not os.path.isdir("/tmp/l3tmp/NCR"):
        sys.stderr.write("Missing NCR data for %s\n" % (afn,))
        now += interval
        continue

    files = glob.glob("/tmp/l3tmp/NCR/NCR_*")
    for fn in files:
        # Need fake seq id
        sys.stdout.write('\001\r\r\n000\r\r\n')
        sys.stdout.write( open(fn, 'rb').read() )
        sys.stdout.write('\r\r\n\003')
        time.sleep(0.25)
    
    subprocess.call("rm -rf /tmp/l3tmp/???", shell=True)
    
    now += interval