"""
 Run gpnids and send the result back to stdout
"""

import sys
import os
import tempfile
import subprocess


os.putenv("GEMTBL", "/home/ldm/pyWWA/gempak/tables")
os.putenv("GEMERR", "/home/ldm/pyWWA/gempak/error")
os.putenv("GEMPDF", "/home/ldm/pyWWA/gempak/pdf")

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
 MAP      = 1/1/2
 LATLON   =
 OUTPUT   = f/%s.out
 list
 run

 exit
""" % (tmpfn, tmpfn, tmpfn)
    p = subprocess.Popen("/home/ldm/bin/gpnids_vg",
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    p.communicate(cmd)
    #(so, se) = p.communicate(cmd)
    #p.stdin.write(cmd)
    #se = p.stderr.read()
    #so = p.stdout.read()
    #time.sleep(3)
    #l.write( se )
    #l.write(so)
    for suffix in ['gif','ncr']:
        if os.path.isfile('%s.%s' % (tmpfn,suffix)):
            os.unlink("%s.%s" % (tmpfn,suffix))

def main():
    """
    Actually do work!
    """
    tmpfn = write_data()
    do_gempak(tmpfn)
    fn = "%s.out" % (tmpfn,)
    if os.path.isfile(fn):
        sys.stdout.write( open(fn).read() )
        os.unlink(fn)
    
if __name__ == '__main__':
    main()

