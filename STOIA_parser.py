"""
 I parse the Iowa Road Condition reports AFOS PIL: STOIA
 I also generate a shapefile of the parsed data
"""

import sys
import re
import datetime
import pytz
import dbflib
import shapelib
import zipfile
import os
import logging
import shutil
import StringIO
import traceback

from pyiem import wellknowntext
import subprocess

import common
import iemdb
import psycopg2.extras
POSTGIS = iemdb.connect('postgis')
pcursor = POSTGIS.cursor(cursor_factory=psycopg2.extras.DictCursor)

FORMAT = "%(asctime)-15s:: %(message)s"
logging.basicConfig(filename='logs/ingestRC.log', filemode='a+', format=FORMAT)
logger=logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

errors = StringIO.StringIO()

# Changedir to /tmp
os.chdir("/tmp")

def figureCondition(condition, conditions):
    """ Find this condition within a dict of conditions """
    for typ in ["Closed", "Travel Advisory", "CC Ice", "CC Snow", "CC Slush",
           "CC Mixed", "MC Ice", "MC Snow", "MC Slush", "MC Mixed",
           "PC Ice", "PC Snow", "PC Slush", "PC Mixed", "CC Frost",
           "MC Frost", "PC Frost", "Wet", "Normal", "No Conditions Reproted"]:
        if condition.find( typ.upper() ) > -1:
            if (not conditions.has_key(typ.upper())):
                logger.info("Unknown Condition: %s\n" % (typ,) )
                pcursor.execute("SELECT max(code) as m from roads_conditions")
                row = pcursor.fetchone()
                if row['m'] is None:
                    newID = 1
                else:
                    newID = int( row['m'] ) + 1
                pcursor.execute("INSERT into roads_conditions VALUES (%s, %s) ",
                                (newID, typ) )
                conditions[typ.upper()] = newID

            return conditions[typ.upper()]

    return conditions["NORMAL"]


def process(raw):
    """ Actually do something """
    # Load up dictionary of Possible Road Conditions
    conditions = {}
    condcodes = {}
    pcursor.execute("SELECT label, code from roads_conditions")
    for row in pcursor:
        conditions[ row['label'].upper() ] = row['code']
        condcodes[ int(row['code']) ] = row['label'].upper()

    # Load up dictionary of roads...
    roads = {}
    pcursor.execute("SELECT major, minor, segid from roads_base")
    for row in pcursor:
        roads["%s%s" % (row['major'], row['minor'].upper())] = row['segid']
    
    # Figure out when this report is valid
    tokens = re.findall("([0-9]{1,2})([0-9][0-9]) ([AP]M) C[DS]T [A-Z][A-Z][A-Z] ([A-Z][A-Z][A-Z]) ([0-9]+) (2[0-9][0-9][0-9])\n", raw)
    # tokens is like [('08', '52', 'AM', 'NOV', '23', '2004')]
    hroffset = 0
    if tokens[0][2] == "PM" and int(tokens[0][0]) < 12:
        hroffset = 12
    if tokens[0][2] == "AM" and int(tokens[0][0]) == 12:
        hroffset = -12
    hr = int(tokens[0][0]) + hroffset
    mi = int(tokens[0][1])
    mod = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7,
      "AUG": 8, "SEP": 9, "OCT":10, "NOV":11, "DEC":12}
    mo = mod[ tokens[0][3] ]
    dy = int(tokens[0][4])
    year = int(tokens[0][5])
    ts = datetime.datetime(year, mo, dy, hr, mi).replace(
                                tzinfo=pytz.timezone("America/Chicago"))
    logger.info("PROCESSING STOIA: %s" % (ts,))

    # Lets start our processing
    lines = re.split("\n", raw[ raw.find("*"):])
    for line in lines:
        if (len(line) < 20 or line[0] == "*" or line[6] != " " or line[7] == " "):
            continue
        if (line[0] != " "):
            major = (line[:6]).strip()
        minor = (line[7:47]).strip().upper()
        condition = (line[47:]).strip().upper()

        #----------------------------------------
        # Now we are going to do things by type!
        roadCondCode = figureCondition(condition, conditions)
        #print roadCondCode, condition, condcodes[roadCondCode]
        towingProhibited = (condition.find("TOWING PROHIBITED") > -1)
        limitedVis = (condition.find("LIMITED VIS.") > -1)
  
        rkey = "%s%s" % (major, minor)
        if not roads.has_key(rkey):
            logger.info("Unknown Road: %s\n" % (rkey,) )
            continue
        segid = roads[rkey]


        pcursor.execute("""UPDATE roads_current SET cond_code = %s, valid = %s, 
         towing_prohibited = %s, limited_vis = %s, raw = %s 
         WHERE segid = %s """ , (roadCondCode, ts, 
                                 towingProhibited, limitedVis, condition, 
                                 segid) )

    # Copy the currents table over to the log... HARD CODED
    pcursor.execute("INSERT into roads_2012_log SELECT * from roads_current")
    return ts

def generate_shapefile(ts):
    """ Generate a shapefile of this data """
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

    pcursor.execute("""select b.*, c.*, astext(b.geom) as bgeom from 
         roads_base b, roads_current c WHERE b.segid = c.segid""")
    i = 0
    for row in pcursor:
        s = row["bgeom"]
        f = wellknowntext.convert_well_known_text(s)
        valid = row["valid"]
        d = {}
        d["SEGID"] = row["segid"]
        d["MAJOR"] = row["major"]
        d["MINOR"] = row["minor"]
        d["US1"] = row["us1"]
        d["ST1"] = row["st1"]
        d["INT1"] = row["int1"]
        d["TYPE"] = row["type"]
        d["VALID"] = valid.strftime("%Y%m%d%H%M")
        d["COND_CODE"] = row["cond_code"]
        d["COND_TXT"] = row["raw"]
        d["BAN_TOW"] = str(row["towing_prohibited"])[0]
        d["LIM_VIS"] = str(row["limited_vis"])[0]

        obj = shapelib.SHPObject(shapelib.SHPT_ARC, 1, f )
        shp.write_object(-1, obj)
        dbf.write_record(i, d)

        del(obj)
        i += 1

    del(shp)
    del(dbf)
    z = zipfile.ZipFile("iaroad_cond.zip", 'w')
    z.write("iaroad_cond.shp")
    z.write("iaroad_cond.shx")
    z.write("iaroad_cond.dbf")
    shutil.copyfile("/mesonet/data/gis/meta/26915.prj", "iaroad_cond.prj")
    z.write("iaroad_cond.prj")
    z.close()

    utc = ts.astimezone( pytz.timezone("UTC") )
    subprocess.call("/home/ldm/bin/pqinsert -p 'zip ac %s gis/shape/26915/ia/iaroad_cond.zip GIS/iaroad_cond_%s.zip zip' iaroad_cond.zip" % (
                                            utc.strftime("%Y%m%d%H%M"), 
                                            utc.strftime("%Y%m%d%H%M")), 
                    shell=True )

    for suffix in ['shp', 'shx', 'dbf', 'prj', 'zip']:
        os.unlink("iaroad_cond.%s" % (suffix,))

if (__name__ == "__main__"):
    raw = sys.stdin.read()
    try:
        ts = process(raw)
        generate_shapefile(ts)
    except:
        traceback.print_exc(file=errors)

    errors.seek(0)
    errstr = errors.read()
    if len(errstr) > 0:
        logger.error( errstr )
        common.email_error(errstr, raw)
    pcursor.close()
    POSTGIS.commit()
    POSTGIS.close()