# Parse nexrad attributes from gpnids to postgis
# Daryl Herzmann 6 Aug 2004

import math, re, pg, sys, mx.DateTime
from pyIEM import stationTable
import smtplib, StringIO, traceback
from email.MIMEText import MIMEText


st = stationTable.stationTable('/mesonet/TABLES/nexrad.stns')
# ./gpnids2pgsql.py FILENAME 88D YYYY MM DD HH MM
filename = sys.argv[1]
nexrad = sys.argv[2]
if (not st.sts.has_key(nexrad)):
  sys.exit(0)
yyyy = int(sys.argv[3])
mm = int(sys.argv[4])
dd = int(sys.argv[5])
hh = int(sys.argv[6])
mi = int(sys.argv[7])
ts = mx.DateTime.DateTime(yyyy,mm,dd,hh,mi)
mydb = pg.connect("postgis", 'iemdb')

def main():
  mydb.query("SET TIME ZONE 'GMT'")
  mydb.query("BEGIN;")
  mydb.query("DELETE from nexrad_attributes WHERE nexrad = '%s'" % (nexrad) )

  cenlat = float(st.sts[nexrad]['lat'])
  cenlon = float(st.sts[nexrad]['lon'])
  latscale = 111137.0
  lonscale = 111137.0 * math.cos( cenlat * math.pi / 180.0 )

#   STM ID  AZ/RAN TVS  MESO POSH/POH/MX SIZE VIL DBZM  HT  TOP  FCST MVMT
  lines = open(filename, 'r').readlines()
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
    mydb.query(sql)

    d["table"] = "nexrad_attributes_log"
    sql = "INSERT into %(table)s (nexrad, storm_id, geom, azimuth,\
    range, tvs, meso, posh, poh, max_size, vil, max_dbz, max_dbz_height,\
    top, drct, sknt, valid) values ('%(nexrad)s', '%(storm_id)s', '%(geom)s',\
    %(azimuth)s, %(range)s, '%(tvs)s', '%(meso)s', %(posh)s,\
    %(poh)s, %(max_size)s, %(vil)s, %(max_dbz)s,\
    %(max_dbz_height)s,%(top)s, %(drct)s, %(sknt)s, '%(valid)s')" % d
    mydb.query(sql)

  if (co > 0):
    mydb.query("COMMIT;")

try:
  main()
except:
  r = open(filename, 'r').read()
  io = StringIO.StringIO()
  traceback.print_exc(file=io)
  #logger.error( io.getvalue() )
  msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),r) )
  msg['subject'] = 'ncr2postgis.py Traceback'
  msg['From'] = "ldm@mesonet.agron.iastate.edu"
  msg['To'] = "akrherz@iastate.edu"

  s = smtplib.SMTP()
  s.connect()
  s.sendmail(msg["From"], msg["To"], msg.as_string())
  s.close()



"""
 nexrad         | character(3)          |
 storm_id       | character(2)          |
 geom           | geometry              |
 azimuth        | smallint              |
 range          | smallint              |
 tvs            | character varying(10) |
 meso           | character varying(10) |
 posh           | smallint              |
 poh            | smallint              |
 max_size       | real                  |
 vil            | smallint              |
 max_dbz        | smallint              |
 max_dbz_height | real                  |
 top            | real                  |
 drct           | smallint              |
 sknt           | smallint              |
"""
