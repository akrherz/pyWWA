"""
 I parse the Iowa Road Condition reports AFOS PIL: STOIA
 I also generate a shapefile of the parsed data
"""
# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
from twisted.python import log
from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.nws.product import TextProduct
from pyiem import wellknowntext

import re
import pytz
import dbflib
import shapelib
import zipfile
import os
import shutil
import subprocess

import common

syslog.startLogging(prefix='pyWWA/stoia_parser', facility=LOG_LOCAL2)
EPSG26915 = """PROJCS["NAD_1983_UTM_Zone_15N",GEOGCS["GCS_North_American_1983",DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",500000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-93.0],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]"""
DBPOOL = common.get_database("postgis", cp_max=1)
CONDITIONS = {}
ROADS = {}

# Changedir to /tmp
os.chdir("/tmp")


def shutdown():
    """ Down we go! """
    log.msg("Stopping...")
    reactor.callWhenRunning(reactor.stop)


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ called when the connection is lost """
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, shutdown)

    def process_data(self, buf):
        """ Process the product """
        defer = DBPOOL.runInteraction(real_parser, buf)
        defer.addErrback(common.email_error, buf)
        defer.addErrback(log.err)


def figureCondition(txn, condition):
    """Conversion of condition text into a code

    Args:
      txn (cursor): psycopg2 database cursor
      condition (str): condition string to convert to a code

    Returns:
      code (int): the converted code
    """
    # shortcut
    if condition in CONDITIONS:
        return CONDITIONS[condition]
    # If we find this string, we want this to be 51 (Important)
    if (condition.find("TRAVEL NOT ADVISED") > -1 or
            condition.find("TRAVEL ADVISORY") > -1):
        CONDITIONS[condition] = 51
        return 51
    # Now we prioritize what we can look for
    for typ in ["Closed", "CC Ice", "CC Snow", "CC Slush",
                "CC Mixed", "MC Ice", "MC Snow", "MC Slush", "MC Mixed",
                "PC Ice", "PC Snow", "PC Slush", "PC Mixed", "CC Frost",
                "MC Frost", "PC Frost", "Wet", "Normal",
                "No Conditions Reproted"]:
        if condition.find(typ.upper()) == -1:
            continue
        if typ.upper() not in CONDITIONS:
            log.msg("Unknown Condition: |%s| |%s|\n" % (typ, condition))
            txn.execute("SELECT max(code) as m from roads_conditions")
            row = txn.fetchone()
            if row['m'] is None:
                newID = 1
            else:
                newID = int(row['m']) + 1
            txn.execute("INSERT into roads_conditions VALUES (%s, %s) ",
                        (newID, typ))
            CONDITIONS[typ.upper()] = newID

        return CONDITIONS[typ.upper()]

    return CONDITIONS.get("NORMAL", 0)


def init_dicts(txn):
    """ Setup what we need to process this file """
    txn.execute("SELECT label, code from roads_conditions")
    for row in txn:
        CONDITIONS[row['label'].upper()] = row['code']
    log.msg("Loaded %s conditions" % (len(CONDITIONS),))

    # Load up dictionary of roads...
    txn.execute("""
        SELECT major, minor, longname, segid from roads_base
        WHERE longname is not null ORDER by segid ASC
    """)
    for row in txn:
        ROADS[row['longname'].upper()] = {'segid': row['segid'],
                                          'major': row['major'],
                                          'minor': row['minor']}
    log.msg("Loaded %s road segments" % (len(ROADS),))


def real_parser(txn, raw):
    """Actually do the heavy lifting of parsing this product

    Args:
      txn (cursor): psycopg2 database transaction
      raw (str): the raw text that needs parsing
    """
    # Load up dictionary of Possible Road Conditions
    if len(ROADS) == 0:
        log.msg("Initializing ROADS and CONDITIONS dicts...")
        init_dicts(txn)

    tp = TextProduct(raw)
    log.msg("PROCESSING STOIA: %s" % (tp.valid,))

    # Lets start our processing by looking for the first * and then
    # processing after finding it
    lines = re.split("\n", raw[raw.find("*"):])
    for line in lines:
        if len(line) < 40 or line[0] == "*" or line[30:40].strip() == '':
            continue
        data = line[7:]
        # Find the right most ) and chomp everything up until it
        pos = data.rfind(")")
        meat = data[:pos+1]
        condition = data[(pos+1):].upper().strip()
        if meat.strip() == '':
            continue
        if meat not in ROADS:
            log.msg("Unknown road: %s\n" % (meat, ))
            continue

        road_code = figureCondition(txn, condition)
        towingProhibited = (condition.find("TOWING PROHIBITED") > -1)
        limitedVis = (condition.find("LIMITED VIS.") > -1)
        segid = ROADS[meat]['segid']

        txn.execute("""
            UPDATE roads_current SET cond_code = %s, valid = %s,
            towing_prohibited = %s, limited_vis = %s, raw = %s
            WHERE segid = %s
            """, (road_code, tp.valid, towingProhibited, limitedVis,
                  condition, segid))

    # Copy the currents table over to the log... HARD CODED
    if tp.valid.month < 7:
        logtable = "roads_%s_%s_log" % (tp.valid.year - 1, tp.valid.year)
    else:
        logtable = "roads_%s_%s_log" % (tp.valid.year, tp.valid.year + 1)
    txn.execute("""
        INSERT into """+logtable+"""
        SELECT * from roads_current WHERE valid = %s
        """, (tp.valid, ))
    log.msg("Copied %s rows into %s table" % (txn.rowcount, logtable))

    # Now we generate a shapefile....
    dbf = dbflib.create("iaroad_cond")
    dbf.add_field("SEGID", dbflib.FTInteger, 4, 0)
    dbf.add_field("MAJOR", dbflib.FTString, 10, 0)
    dbf.add_field("MINOR", dbflib.FTString, 128, 0)
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

    txn.execute("""select b.*, c.*, ST_astext(b.geom) as bgeom from
         roads_base b, roads_current c WHERE b.segid = c.segid
         and valid is not null and b.geom is not null""")
    i = 0
    for row in txn:
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

        obj = shapelib.SHPObject(shapelib.SHPT_ARC, 1, f)
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
    o = open('iaroad_cond.prj', 'w')
    o.write(EPSG26915)
    o.close()
    z.write("iaroad_cond.prj")
    z.close()

    utc = tp.valid.astimezone(pytz.timezone("UTC"))
    subprocess.call(("/home/ldm/bin/pqinsert -p 'zip ac %s "
                     "gis/shape/26915/ia/iaroad_cond.zip "
                     "GIS/iaroad_cond_%s.zip zip' iaroad_cond.zip"
                     "") % (utc.strftime("%Y%m%d%H%M"),
                            utc.strftime("%Y%m%d%H%M")), shell=True)

    for suffix in ['shp', 'shx', 'dbf', 'prj', 'zip']:
        os.unlink("iaroad_cond.%s" % (suffix,))

if __name__ == "__main__":
    # main
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
