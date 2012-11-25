"""
 Take the raw output from gpnids and parse it into the IEM PostgreSQL database
 I am called with the following arguments
     1. NEXRAD ID
     2. Year YYYY
     3. MM
     4. DD
     5. Minute MI
"""

import sys
import mx.DateTime
import os
#import time
#time.sleep(600)
import tempfile
import subprocess

os.putenv("GEMTBL", "/home/ldm/pyWWA/gempak/tables")

def write_data():
    """
    Do the GEMPAK workflow!
    """
    tmpfn = tempfile.mktemp().lower()
    o = open("%s.ncr" % (tmpfn,), 'wb')
    o.write( sys.stdin.read() )
    o.close()
    return tmpfn

def do_gempak(tmpfn):
    """
    Do the GEMPAK workflow
    """
    cmd = """  RADFIL   = %s.ncr
 RADTIM   =
 TITLE    = 1
 PANEL    = 0
 DEVICE   = GIF|%s.gif
 CLEAR    = YES
 TEXT     = 1
 COLORS   = 1
 WIND     = 
 LINE     = 3
 CLRBAR   =
 IMCBAR   =
 GAREA    = DSET
 MAP      = 1
 LATLON   =
 OUTPUT   = f/%s.out
 run

 exit
""" % (tmpfn, tmpfn, tmpfn)
    p = subprocess.Popen("/home/ldm/bin/gpnids_vg",
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    (se, so) = p.communicate(cmd)
    #sys.stdout.write(se)
    #sys.stdout.write(so)
    for suffix in ['gif','ncr']:
        if os.path.isfile('%s.%s' % (tmpfn,suffix)):
            os.unlink("%s.%s" % (tmpfn,suffix))

def main(nexrad, ts):
    """
    Actually do work!
    """
    tmpfn = write_data()
    do_gempak(tmpfn)
    fn = "%s.out" % (tmpfn,)
    if not os.path.isfile(fn):
        return
    sys.stdout.write( open(fn).read() )
    os.unlink(fn)
    #sys.stdout.write("%s %s %s" % (fn, nexrad, ts))
    
if __name__ == '__main__':
    nexrad = sys.argv[1]
    ts = mx.DateTime.strptime(sys.argv[2], '%Y%m%d%H%M')
    main(nexrad, ts)


