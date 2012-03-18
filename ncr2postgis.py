"""
 Take the raw output from gpnids and parse it into the IEM PostgreSQL database
 I am called with the following arguments
     1. NEXRAD ID
     2. Year YYYY
     3. MM
     4. DD
     5. Minute MI
 $Id: $:
"""

import math, re, sys, mx.DateTime
from pyIEM import stationTable
import smtplib, StringIO, traceback
import common
import os
import tempfile
import glob
from twisted.python import log
from twisted.python import logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('ncr2postgis.log','logs'))

import iemdb
POSTGIS = iemdb.connect('postgis', bypass=False)
pcursor = POSTGIS.cursor()

st = stationTable.stationTable('/home/ldm/pyWWA/tables/nexrad.stns')

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
    cmd = """
/home/ldm/bin/gpnids_vg << EOF
 RADFIL   = %s.ncr
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
EOF
""" % (tmpfn, tmpfn, tmpfn)
    os.system( cmd )
    for suffix in ['gif','ncr']:
        if os.path.isfile('%s.%s' % (tmpfn,suffix)):
            os.unlink("%s.%s" % (tmpfn,suffix))

def main(nexrad, ts):
    """
    Actually do work!
    """
    tmpfn = write_data()
    do_gempak(tmpfn)
    if not os.path.isfile("%s.out" % (tmpfn,)):
        log.msg("Nothing came from GEMPAK! %s.out %s %s" % (tmpfn, nexrad, ts))
        return
    
    if not st.sts.has_key(nexrad):
        log.msg('Unknown NEXRAD! %s' % ( nexrad,))
        return
    
    pcursor.execute("SET TIME ZONE 'GMT'")
    pcursor.execute("DELETE from nexrad_attributes WHERE nexrad = '%s'" % (nexrad) )

    cenlat = float(st.sts[nexrad]['lat'])
    cenlon = float(st.sts[nexrad]['lon'])
    latscale = 111137.0
    lonscale = 111137.0 * math.cos( cenlat * math.pi / 180.0 )

    #   STM ID  AZ/RAN TVS  MESO POSH/POH/MX SIZE VIL DBZM  HT  TOP  FCST MVMT
    lines = open('%s.out' % (tmpfn,), 'r').readlines()
    co = 0
    for line in lines:
        if (line[1] != " "):
            continue
        line = line.replace(">", " ")
        tokens = line.replace("/", " ").split()
        if (len(tokens) < 14 or tokens[0] == "STM"):
            continue
        d = {}
        co += 1
        d["storm_id"] = tokens[0]
        d["azimuth"] = float(tokens[1])
        d["range"] = float(tokens[2]) * 1.852
        d["tvs"] = tokens[3]
        d["meso"] = tokens[4]
        d["posh"] = tokens[5]
        d["poh"] = tokens[6]
        if (tokens[7] == "<0.50"):
            tokens[7] = 0.01
        d["max_size"] = tokens[7]
        d["vil"] = tokens[8]
        d["max_dbz"] = tokens[9]
        d["max_dbz_height"] = tokens[10]
        d["top"] = tokens[11]
        if (tokens[12] == "NEW"):
            d["drct"], d["sknt"] = 0,0
        d["drct"] = tokens[12]
        d["sknt"] = tokens[13]
        d["nexrad"] = nexrad

        cosaz = math.cos( d["azimuth"] * math.pi / 180.0 )
        sinaz = math.sin( d["azimuth"] * math.pi / 180.0 )
        mylat = cenlat + (cosaz * (d["range"] * 1000.0) / latscale)
        mylon = cenlon + (sinaz * (d["range"] * 1000.0) / lonscale)
        d["geom"] = "SRID=4326;POINT(%s %s)" % (mylon, mylat)
        d["valid"] = ts.strftime("%Y-%m-%d %H:%M")

        d["table"] = "nexrad_attributes"
        sql = "INSERT into %(table)s (nexrad, storm_id, geom, azimuth,\
    range, tvs, meso, posh, poh, max_size, vil, max_dbz, max_dbz_height,\
    top, drct, sknt, valid) values ('%(nexrad)s', '%(storm_id)s', '%(geom)s',\
    %(azimuth)s, %(range)s, '%(tvs)s', '%(meso)s', %(posh)s,\
    %(poh)s, %(max_size)s, %(vil)s, %(max_dbz)s,\
    %(max_dbz_height)s,%(top)s, %(drct)s, %(sknt)s, '%(valid)s')" % d
        pcursor.execute( sql )

        d["table"] = "nexrad_attributes_log"
        sql = "INSERT into %(table)s (nexrad, storm_id, geom, azimuth,\
    range, tvs, meso, posh, poh, max_size, vil, max_dbz, max_dbz_height,\
    top, drct, sknt, valid) values ('%(nexrad)s', '%(storm_id)s', '%(geom)s',\
    %(azimuth)s, %(range)s, '%(tvs)s', '%(meso)s', %(posh)s,\
    %(poh)s, %(max_size)s, %(vil)s, %(max_dbz)s,\
    %(max_dbz_height)s,%(top)s, %(drct)s, %(sknt)s, '%(valid)s')" % d
        pcursor.execute( sql )
    #log.info("Process [%s] %s entries" % (nexrad, co))
    os.unlink('%s.out' % (tmpfn,))
    log.msg("%s %s Processed %s entries" % (nexrad, ts, co))


nexrad = sys.argv[1]
yyyy = int(sys.argv[2])
mm = int(sys.argv[3])
dd = int(sys.argv[4])
hh = int(sys.argv[5])
mi = int(sys.argv[6])
ts = mx.DateTime.DateTime(yyyy,mm,dd,hh,mi)
try:
    main(nexrad, ts)
    pcursor.close()
    POSTGIS.commit()
    POSTGIS.close()
except Exception, exp:
    common.email_error(exp, "%s %s" %(nexrad, ts))

