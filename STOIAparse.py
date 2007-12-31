# Need something to ingest Iowa Road Conditions

import sys, re, mx.DateTime, string, dbflib, shapelib, zipfile, os, logging
import shutil, StringIO, traceback
import smtplib
from email.MIMEText import MIMEText
from pyIEM import wellknowntext
import secret

# Changedir to /tmp
os.chdir("/tmp")

import pg
mydb = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser)

FORMAT = "%(asctime)-15s:: %(message)s"
logging.basicConfig(filename='/mesonet/data/logs/%s/ingestRC.log' \
           % (os.getenv("USER"), ), filemode='a', format=FORMAT)
logger=logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

errors = StringIO.StringIO()


# Called if we want to email any errors that occured....
def emailErrors(raw):
  errors.seek(0)
  errstr = errors.read()
  if ( len(errstr) == 0):
    return  # No runs, no hits, no errors

  logger.error( errstr )

  msg = MIMEText("%s\n\n>RAW DATA\n\n%s" % (errstr, raw) )
  msg['subject'] = 'STOIAparse.py Traceback'
  msg['From'] = "ldm@mesonet.agron.iastate.edu"
  msg['To'] = "akrherz@iastate.edu"

  s = smtplib.SMTP()
  s.connect()
  s.sendmail(msg["From"], msg["To"], msg.as_string())
  s.close()

def findString(cond, sstr):
  if (cond.find(sstr) > -1):
    return True
  return False

def figureCondition(condition, conditions):
  for typ in ["Closed", "Travel Advisory", "CC Ice", "CC Snow", "CC_Slush",
           "CC Mixed", "MC Ice", "MC Snow", "MC Slush", "MC Mixed",
           "PC Ice", "PC Snow", "PC Slush", "PC Mixed", "CC Frost",
           "MC Frost", "PC Frost", "Wet", "Normal", "No Conditions Reproted"]:
    if (findString(condition, typ)):
      if (not conditions.has_key(typ)):
        logger.info("Unknown Condition: %s\n" % (typ,) )
        rs = mydb.query("SELECT max(code) as m from roads_conditions").dictresult()
        newID = int( rs[0]["m"] ) + 1
        mydb.query("INSERT into roads_conditions VALUES (%s, '%s') " % \
          (newID, typ) )
        conditions[typ] = newID

      return conditions[typ]

  return conditions["Normal"]


def process(raw):
  # Load up dictionary of Possible Road Conditions
  conditions = {}
  condcodes = {}
  rs = mydb.query("SELECT * from roads_conditions").dictresult()
  for i in range(len(rs)):
    conditions[ rs[i]["label"] ] = rs[i]["code"]
    condcodes[ int(rs[i]["code"]) ] = rs[i]["label"]

  # Load up dictionary of roads...
  roads = {}
  rs = mydb.query("SELECT * from roads_base").dictresult()
  for i in range(len(rs)):
    roads["%s%s" % (rs[i]["major"], rs[i]["minor"])] = rs[i]["segid"]

  # Figure out when this report is valid
  tokens = re.findall("([0-1][0-9])([0-9][0-9]) ([A|P]M) C[D|S]T [A-Z][A-Z][A-Z] ([A-Z][A-Z][A-Z]) ([0-9]+) (2[0-9][0-9][0-9])\n", raw)
  # tokens is like [('08', '52', 'AM', 'NOV', '23', '2004')]
  hroffset = 0
  if (tokens[0][2] == "PM" and int(tokens[0][0]) < 12):
    hroffset = 12
  hr = int(tokens[0][0]) + hroffset
  mi = int(tokens[0][1])
  mod = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7,
      "AUG": 8, "SEP": 9, "OCT":10, "NOV":11, "DEC":12}
  mo = mod[ tokens[0][3] ]
  dy = int(tokens[0][4])
  year = int(tokens[0][5])
  ts = mx.DateTime.DateTime(year, mo, dy, hr, mi)
  logger.info("PROCESSING STOIA: %s" % (ts,))

  # Lets start our processing
  lines = re.split("\n", raw)
  for line in lines:
    if (len(line) < 20 or line[0] == "*" or line[6] != " " or line[7] == " "):
      continue
    if (line[0] != " "):
      major = (line[:6]).strip()
    minor = (line[7:40]).strip()
    condition = (line[40:]).strip()

    #----------------------------------------
    # Now we are going to do things by type!
    roadCondCode = figureCondition(condition, conditions)
    #print roadCondCode, condition, condcodes[roadCondCode]
    towingProhibited = findString(condition, "Towing Prohibited")
    limitedVis = findString(condition, "Limited Vis.")
  
    rkey = "%s%s" % (major, minor)
    if (not roads.has_key(rkey)):
      logger.info("Unknown Road: %s\n" % (rkey,) )
      continue
    segid = roads[rkey]


    mydb.query("UPDATE roads_current SET cond_code = %s, valid = '%s', \
     towing_prohibited = %s, limited_vis = %s, raw = '%s' \
     WHERE segid = %s " % (roadCondCode, \
     ts.strftime("%Y-%m-%d %H:%M"), towingProhibited, limitedVis, condition, segid) )



  # Copy the currents table over to the log...
  mydb.query("INSERT into roads_%s_log SELECT * from roads_current"%(ts.year,))

  # Now we generate a shapefile....
  dbf = dbflib.create("iaroad_cond")
  dbf.add_field("SEGID", dbflib.FTInteger, 4, 0)
  dbf.add_field("MAJOR", dbflib.FTString, 10, 0)
  dbf.add_field("MINOR", dbflib.FTString, 40, 0)
  dbf.add_field("US1", dbflib.FTInteger, 4, 0)
  dbf.add_field("ST1", dbflib.FTInteger, 4, 0)
  dbf.add_field("INT1", dbflib.FTInteger, 4, 0)
  dbf.add_field("TYPE", dbflib.FTInteger, 4, 0)
  dbf.add_field("VALID", dbflib.FTString, 12, 0)
  dbf.add_field("COND_CODE", dbflib.FTInteger, 4, 0)
  dbf.add_field("COND_TXT", dbflib.FTString, 120, 0)
  dbf.add_field("BAN_TOW", dbflib.FTString, 1, 0)
  dbf.add_field("LIM_VIS", dbflib.FTString, 1, 0)

  shp = shapelib.create("iaroad_cond", shapelib.SHPT_ARC)

  sql = "select b.*, c.*, astext(b.geom) as bgeom from \
         roads_base b, roads_current c WHERE b.segid = c.segid"
  rs = mydb.query(sql).dictresult()
  for i in range(len(rs)):
    s = rs[i]["bgeom"]
    f = wellknowntext.convert_well_known_text(s)
    valid = mx.DateTime.strptime(rs[i]["valid"][:16], "%Y-%m-%d %H:%M")
    d = {}
    d["SEGID"] = rs[i]["segid"]
    d["MAJOR"] = rs[i]["major"]
    d["MINOR"] = rs[i]["minor"]
    d["US1"] = rs[i]["us1"]
    d["ST1"] = rs[i]["st1"]
    d["INT1"] = rs[i]["int1"]
    d["TYPE"] = rs[i]["type"]
    d["VALID"] = valid.strftime("%Y%m%d%H%M")
    d["COND_CODE"] = rs[i]["cond_code"]
    d["COND_TXT"] = rs[i]["raw"]
    d["BAN_TOW"] = rs[i]["towing_prohibited"].upper()
    d["LIM_VIS"] = rs[i]["limited_vis"].upper()

    obj = shapelib.SHPObject(shapelib.SHPT_ARC, 1, f )
    shp.write_object(-1, obj)
    dbf.write_record(i, d)

    del(obj)

  del(shp)
  del(dbf)
  z = zipfile.ZipFile("iaroad_cond.zip", 'w')
  z.write("iaroad_cond.shp")
  z.write("iaroad_cond.shx")
  z.write("iaroad_cond.dbf")
  shutil.copyfile("/mesonet/data/gis/meta/26915.prj", "iaroad_cond.prj")
  z.write("iaroad_cond.prj")
  z.close()

  os.system("/home/ldm/bin/pqinsert -p 'zip ac %s gis/shape/26915/ia/iaroad_cond.zip GIS/iaroad_cond_%s.zip zip' iaroad_cond.zip" % (ts.gmtime().strftime("%Y%m%d%H%M"), ts.gmtime().strftime("%Y%m%d%H%M")) )

if (__name__ == "__main__"):
  raw = sys.stdin.read()
  try:
    process(raw)
  except:
    traceback.print_exc(file=errors)

  emailErrors(raw)

  sys.exit(0)
