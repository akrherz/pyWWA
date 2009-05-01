# Copyright (c) 2005 Iowa State University
# http://mesonet.agron.iastate.edu/ -- mailto:akrherz@iastate.edu
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
""" SPC Watch Ingestor """

__revision__ = '$Id: new_watch.py 3160 2008-04-09 23:31:06Z akrherz $'


from twisted.python import log
log.startLogging(open('logs/new_watch.log', 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"


import sys
import re
import math
import mx.DateTime
import StringIO
import traceback
import common
from email.MIMEText import MIMEText

# Non standard Stuff
from pyIEM import stationTable, utils

KM_SM = 1.609347

# Database Connections
import pg, secret
POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser, passwd=secret.dbpass)
POSTGIS.query("SET TIME ZONE 'GMT'")

errors = StringIO.StringIO()

from twisted.words.protocols.jabber import client, jid
from twisted.internet import reactor
from twisted.mail import smtp

IEM_URL = secret.WATCH_URL
errors = StringIO.StringIO()


def cancel_watch(report, ww_num):
    """ Cancel a watch please """
    tokens = re.findall("KWNS ([0-3][0-9])([0-9][0-9])([0-9][0-9])", report)
    day1 = int(tokens[0][0])
    hour1 = int(tokens[0][1])
    minute1 = int(tokens[0][2])
    gmt = mx.DateTime.gmt()
    ts = mx.DateTime.DateTime(gmt.year, gmt.month, day1, hour1, minute1)
    for tbl in ('watches', 'watches_current'):
        sql = "UPDATE %s SET expired = '%s' WHERE num = %s and \
              extract(year from expired) = %s " \
              % (tbl, ts.strftime("%Y-%m-%d %H:%M"), 
               ww_num, ts.year)
        log.msg(sql)
        POSTGIS.query(sql)

def process(raw):
    try:
        #raw = raw.replace("\015\015\012", "\n")
        real_process(raw)
    except:
        io = StringIO.StringIO()
        traceback.print_exc(file=io)
        msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),raw))
        msg['subject'] = 'new_watch.py Traceback'
        msg['From'] = secret.parser_user
        msg['To'] = secret.error_email
        smtp.sendmail("localhost", msg["From"], msg["To"], msg)


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
    ww_type = tokens[0][2]

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

    gmt = mx.DateTime.gmt()
    sTS = mx.DateTime.DateTime(gmt.year, gmt.month, day1, hour1, minute1)
    eTS = mx.DateTime.DateTime(gmt.year, gmt.month, day2, hour2, minute2)

    # If we are near the end of the month and the day1 is 1, add 1 month
    if (gmt.day > 27  and day1 == 1):
        sTS += mx.DateTime.RelativeDateTime(months=+1)
    if (gmt.day > 27  and day2 == 1):
        eTS += mx.DateTime.RelativeDateTime(months=+1)

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
    (lon1, lat1) = utils.loc2lonlat(st, loc1, loc1_vector, loc1_displacement)
    (lon2, lat2) = utils.loc2lonlat(st, loc2, loc2_vector, loc2_displacement)

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


    wkt = "%s %s,%s %s,%s %s,%s %s,%s %s" % (lon11, lat11,  \
       lon21, lat21, lon22, lat22, lon12, lat12, lon11, lat11)

    # Delete from currents
    sql = "DELETE from watches_current WHERE sel = 'SEL%s'" % (saw,)
    POSTGIS.query(sql)

    # Delete from archive, since maybe it is a correction....
    sql = "DELETE from watches WHERE num = %s and \
           extract(year from issued) = %s" % (ww_num, sTS.year)
    POSTGIS.query(sql)

    # Insert into our watches table
    for tbl in ('watches', 'watches_current'):
        sql = "INSERT into %s(sel, issued, expired, type, report, geom, num) \
               VALUES('SEL%s','%s','%s','%s','%s',\
               'SRID=4326;MULTIPOLYGON(((%s)))', %s)" % \
               (tbl, saw, sTS.strftime("%Y-%m-%d %H:%M"), \
                eTS.strftime("%Y-%m-%d %H:%M"), types[ww_type], \
                raw.replace("'","\\'"), wkt, ww_num)
        POSTGIS.query(sql)

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

    # Figure out who should get notification of the watch...
    sql = "SELECT distinct wfo from nws_ugc WHERE \
         contains('SRID=4326;MULTIPOLYGON(((%s)))', geom)" % (wkt,)
    rs = POSTGIS.query(sql).dictresult()
    for i in range(len(rs)):
        wfo = rs[i]['wfo']
        mess = "%s: %s" % (wfo, jabberTxt)
        jabber.sendMessage(mess, jabberTxtHTML)

    # Special message for SPC
    lines = raw.split("\n")
    mess = "SPC: SPC issues %s http://www.spc.noaa.gov/products/watch/ww%04i.html" % (lines[4], int(ww_num) )
    jabber.sendMessage(mess)

def killer():
    reactor.stop()


raw = sys.stdin.read()

myJid = jid.JID('%s@%s/watch_%s' % \
      (secret.iembot_ingest_user, secret.chatserver, \
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = common.JabberClient(myJid)

factory.addBootstrap('//event/stream/authd', jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
reactor.connectTCP(secret.connect_chatserver, 5222, factory)
reactor.callLater(0, process, raw)
reactor.callLater(30, killer)
reactor.run()
