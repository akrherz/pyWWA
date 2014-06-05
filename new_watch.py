""" SPC Watch Ingestor """

from twisted.python import log
from twisted.python import logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('new_watch.log', 'logs/'))

import re
import os
import math
import datetime
import pytz
import common

# Non standard Stuff
from support import stationTable


KM_SM = 1.609347

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

# Database Connections
from pyldm import ldmbridge
from twisted.enterprise import adbapi
DBPOOL = adbapi.ConnectionPool("twistedpg", database="postgis", cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )

from twisted.internet import reactor

IEM_URL = config.get('urls', 'watch')

def cancel_watch(report, ww_num):
    """ Cancel a watch please """
    tokens = re.findall("KWNS ([0-3][0-9])([0-9][0-9])([0-9][0-9])", report)
    day1 = int(tokens[0][0])
    hour1 = int(tokens[0][1])
    minute1 = int(tokens[0][2])
    gmt = datetime.datetime.utcnow().replace(tzinfo=pytz.timezone("UTC"))
    ts = datetime.datetime(gmt.year, gmt.month, day1, hour1, minute1)
    ts = ts.replace(tzinfo=pytz.timezone("UTC"))
    for tbl in ('watches', 'watches_current'):
        sql = """UPDATE """+tbl+""" SET expired = %s WHERE num = %s and 
              extract(year from expired) = %s """
        args = (ts, ww_num, ts.year)
        deffer = DBPOOL.runOperation(sql, args)
        deffer.addErrback(common.email_error, report)

    msg = "SPC: SPC cancels WW %s http://www.spc.noaa.gov/products/watch/ww%04i.html" % ( ww_num, int(ww_num) )
    jabber.sendMessage( msg , msg)

dirs = {'NNE': 22.5, 'ENE': 67.5, 'NE':  45.0, 'E': 90.0, 'ESE': 112.5,
        'SSE': 157.5, 'SE': 135.0, 'S': 180.0, 'SSW': 202.5,
        'WSW': 247.5, 'SW': 225.0, 'W': 270.0, 'WNW': 292.5,
        'NW': 315.0, 'NNW': 337.5, 'N': 0, '': 0}


def loc2lonlat(stationTable, site, direction, displacement):
    """
Compute the longitude and latitude of a point given by a site ID
and an offset ex) 2 SM NE of ALO
    """
    # Compute Base location
    lon0 = stationTable.sts[site]['lon']
    lat0 = stationTable.sts[site]['lat']
    x = -math.cos( math.radians( dirs[direction] + 90.0) )
    y = math.sin( math.radians( dirs[direction] + 90.0) )
    lat0 += (y * displacement * KM_SM / 111.11 )
    lon0 += (x * displacement * KM_SM /(111.11*math.cos( math.radians(lat0))))

    return lon0, lat0


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.err( reason )
        reactor.callLater(7, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


    def process_data(self, raw):
        try:
            #raw = raw.replace("\015\015\012", "\n")
            real_process(raw)
        except Exception,exp:
            common.email_error(exp, raw)

def real_process(raw):

    report = raw.replace("\015\015\012", "").replace("\n", "").replace("'", " ")
    # Set up Station Table!
    st = stationTable.stationTable("/home/ldm/pyWWA/support/spcwatch.tbl")

    # Determine the Saw number
    saw = re.findall("SAW([0-9])", report)[0][0]

    # Determine the Watch type
    myre = "WW ([0-9]*) (TEST)? ?(SEVERE TSTM|TORNADO|SEVERE THUNDERSTORM)"
    tokens = re.findall(myre, report)
    if (tokens[0][1] == "TEST"):
        print 'TEST watch found'
        return
    ww_num = tokens[0][0]
    #ww_type = tokens[0][2]

    # Need to check for cancels
    tokens = re.findall("CANCELLED", report)
    if (len(tokens) > 0):
        cancel_watch(report, ww_num)
        return 

    jabberReplacesTxt = ""
    tokens = re.findall("REPLACES WW ([0-9]*)", report)
    if (len(tokens) > 0):
        for token in tokens:
            jabberReplacesTxt += " WW %s," % (token,)
            cancel_watch(report, token)

    # Figure out when this is valid for
    tokens = re.findall("([0-3][0-9])([0-2][0-9])([0-6][0-9])Z - ([0-3][0-9])([0-2][0-9])([0-6][0-9])Z", report)

    day1 = int(tokens[0][0])
    hour1 = int(tokens[0][1])
    minute1 = int(tokens[0][2])
    day2 = int(tokens[0][3])
    hour2 = int(tokens[0][4])
    minute2 = int(tokens[0][5])

    gmt = datetime.datetime.utcnow().replace(tzinfo=pytz.timezone("UTC"))
    sTS = gmt.replace(day=day1, hour=hour1, minute=minute1)
    eTS = gmt.replace(day=day2, hour=hour2, minute=minute2)

    # If we are near the end of the month and the day1 is 1, add 1 month
    if gmt.day > 27  and day1 == 1:
        sTS +=  datetime.timedelta(days=+35)
        sTS = sTS.replace(day=1)
    if gmt.day > 27  and day2 == 1:
        eTS +=  datetime.timedelta(days=+35)
        eTS = eTS.replace(day=1)

    # Brute Force it!
    tokens = re.findall("WW ([0-9]*) (SEVERE TSTM|TORNADO).*AXIS\.\.([0-9]+) STATUTE MILES (.*) OF LINE..([0-9]*)([A-Z]*)\s?(...)/.*/ - ([0-9]*)([A-Z]*)\s?(...)/.*/..AVIATION", report)

    types = {'SEVERE TSTM': 'SVR', 'TORNADO': 'TOR'}

    ww_num = tokens[0][0]
    ww_type = tokens[0][1]
    box_radius = float(tokens[0][2])
    orientation = tokens[0][3]

    t = tokens[0][4]
    loc1_displacement = 0
    if (t != ""):
        loc1_displacement = float(t)

    loc1_vector = tokens[0][5]
    loc1 = tokens[0][6]

    t = tokens[0][7]
    loc2_displacement = 0
    if (t != ""):
        loc2_displacement = float(t)
    loc2_vector = tokens[0][8]
    loc2 = tokens[0][9]


    # Now, we have offset locations from our base station locs
    (lon1, lat1) = loc2lonlat(st, loc1, loc1_vector, loc1_displacement)
    (lon2, lat2) = loc2lonlat(st, loc2, loc2_vector, loc2_displacement)

    log.msg("LOC1 OFF %s [%s,%s] lat: %s lon %s" % (loc1, loc1_displacement, loc1_vector, lat1, lon1))
    log.msg("LOC2 OFF %s [%s,%s] lat: %s lon %s" % (loc2, loc2_displacement, loc2_vector, lat2, lon2))

    # Now we compute orientations
    if lon2 == lon1: #same as EW
        orientation = "EAST AND WEST"
    if lat1 == lat2: #same as NS
        orientation = "NORTH AND SOUTH"

    if orientation == "EAST AND WEST":
        lat11 = lat1
        lat12 = lat1
        lat21 = lat2
        lat22 = lat2
        lon11 = lon1 - (box_radius * KM_SM) / (111.11 * math.cos(math.radians(lat1)))
        lon12 = lon1 + (box_radius * KM_SM) / (111.11 * math.cos(math.radians(lat1)))
        lon21 = lon2 - (box_radius * KM_SM) / (111.11 * math.cos(math.radians(lat2)))
        lon22 = lon2 + (box_radius * KM_SM) / (111.11 * math.cos(math.radians(lat2)))

    elif orientation == "NORTH AND SOUTH":
        lon11 = lon1
        lon12 = lon1
        lon21 = lon2
        lon22 = lon2
        lat11 = lat1 + (box_radius * KM_SM) / 111.11
        lat12 = lat1 - (box_radius * KM_SM) / 111.11
        lat21 = lat2 + (box_radius * KM_SM) / 111.11
        lat22 = lat2 - (box_radius * KM_SM) / 111.11

    elif orientation == "EITHER SIDE":
        slope = (lat2 - lat1)/(lon2 - lon1)
        angle = (math.pi / 2.0) - math.fabs( math.atan(slope))
        x = box_radius * math.cos(angle)
        y = box_radius * math.sin(angle)
        if (slope < 0):
            y = 0-y
        lat11 = lat1 + y * KM_SM / 111.11
        lat12 = lat1 - y * KM_SM / 111.11
        lat21 = lat2 + y * KM_SM / 111.11
        lat22 = lat2 - y * KM_SM / 111.11
        lon11 = lon1 - x * KM_SM / (111.11 * math.cos(math.radians(lat1)))
        lon12 = lon1 + x * KM_SM / (111.11 * math.cos(math.radians(lat1)))
        lon21 = lon2 - x * KM_SM / (111.11 * math.cos(math.radians(lat2)))
        lon22 = lon2 + x * KM_SM / (111.11 * math.cos(math.radians(lat2)))


    wkt = "%s %s,%s %s,%s %s,%s %s,%s %s" % (lon11, lat11,  
       lon21, lat21, lon22, lat22, lon12, lat12, lon11, lat11)

    def runner(txn):
        # Delete from archive, since maybe it is a correction....
        sql = """DELETE from watches WHERE num = %s and 
           extract(year from issued) = %s""" % (ww_num, sTS.year)
        txn.execute(sql)

        # Insert into our watches table
        giswkt = 'SRID=4326;MULTIPOLYGON(((%s)))' % (wkt,)
        sql = """INSERT into watches (sel, issued, expired, type, report, 
            geom, num) VALUES(%s,%s,%s,%s,%s,%s, %s)""" 
        args = ('SEL%s' % (saw,), sTS.strftime("%Y-%m-%d %H:%M+00"), 
                eTS.strftime("%Y-%m-%d %H:%M+00"), types[ww_type], 
                raw, giswkt, ww_num)
        txn.execute(sql, args)
        sql = """UPDATE watches_current SET issued = %s, expired = %s, type = %s,
            report = %s, geom = %s, num = %s WHERE sel = %s"""
        args = (sTS.strftime("%Y-%m-%d %H:%M+00"), 
                eTS.strftime("%Y-%m-%d %H:%M+00"), types[ww_type], 
                raw, giswkt, ww_num, 'SEL%s' % (saw,))
        txn.execute(sql, args)

    defer = DBPOOL.runInteraction(runner)
    defer.addErrback(common.email_error, 'blah')
    # Figure out WFOs affected...
    jabberTxt = "SPC issues %s watch till %sZ" % \
                 (ww_type, eTS.strftime("%H:%M") )
    jabberTxtHTML = "Storm Prediction Center issues \
<a href=\"http://www.spc.noaa.gov/products/watch/ww%04i.html\">%s watch</a>\
 till %s UTC " % (int(ww_num), ww_type, eTS.strftime("%H:%M") )
    if (jabberReplacesTxt != ""):
        jabberTxt += ", new watch replaces "+ jabberReplacesTxt[:-1]
        jabberTxtHTML += ", new watch replaces "+ jabberReplacesTxt[:-1]

    jabberTxt += " http://www.spc.noaa.gov/products/watch/ww%04i.html" % (int(ww_num), )
    jabberTxtHTML += " (<a href=\"%s?year=%s&amp;num=%s\">Watch Quickview</a>) " % (IEM_URL, sTS.year, ww_num)

    def runner2(txn):
        # Figure out who should get notification of the watch...
        sql = """SELECT distinct wfo from ugcs WHERE 
         ST_Contains('SRID=4326;MULTIPOLYGON(((%s)))', geom)
         and end_ts is null""" % (wkt,)
    
        txn.execute(sql)
        rs = txn.fetchall()
        xtra = {'channels': ['SPC']}
        for i in range(len(rs)):
            wfo = rs[i]['wfo']
            xtra['channels'].append( wfo )
        jabber.sendMessage(jabberTxt, jabberTxtHTML, xtra)

        # Special message for SPC
        lines = raw.split("\n")
        twt = lines[5].replace("\r\r", "")
        url = "http://www.spc.noaa.gov/products/watch/ww%04i.html" % (int(ww_num),)
        xtra['channels'].append("SPC")
        common.tweet(xtra['channels'], twt, url)

    df = DBPOOL.runInteraction(runner2)
    df.addErrback(common.email_error, raw)
    


jabber = common.make_jabber_client("new_watch")

ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run() #@UndefinedVariable
