# Parse Watch Data!  Oye!

from twisted.python import log
import os
log.startLogging(open('/mesonet/data/logs/%s/new_watch.log' % (os.getenv("USER"),), 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"


import sys, logging
import re
import mapscript
import math
import smtplib
import mx.DateTime
import sys
import StringIO
import traceback
from email.MIMEText import MIMEText

# Non standard Stuff
from pyIEM import stationTable, utils

KM_SM = 1.609347

# Database Connections
import pg, secret
mydb = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser)
mydb.query("SET TIME ZONE 'GMT'")

errors = StringIO.StringIO()

from twisted.words.protocols.jabber import client, jid
from twisted.words.xish import domish
from twisted.internet import reactor


errors = StringIO.StringIO()

qu = 0

class JabberClient:
    xmlstream = None

    def __init__(self, myJid):
        self.myJid = myJid


    def authd(self,xmlstream):
        print "authenticated"
        self.xmlstream = xmlstream
        presence = domish.Element(('jabber:client','presence'))
        xmlstream.send(presence)

        xmlstream.addObserver('/message',  self.debug)
        xmlstream.addObserver('/presence', self.debug)
        xmlstream.addObserver('/iq',       self.debug)

    def debug(self, elem):
        print elem.toXml().encode('utf-8')
        print "="*20

    def sendMessage(self, body, htmlbody):
        global qu
        while (self.xmlstream is None):
            print "xmlstream is None, so lets try again!"
            reactor.callLater(3, self.sendMessage, body, htmlbody)
            return

        message = domish.Element(('jabber:client','message'))
        message['to'] = "iembot@%s" % (secret.chatserver,)
        message['type'] = 'chat'
        message.addElement('body',None,body)

        html = domish.Element(('http://jabber.org/protocol/xhtml-im','html'))
        body = domish.Element(('http://www.w3.org/1999/xhtml','body'))
        body.addRawXml( htmlbody )
        html.addChild(body)
        message.addChild(html)
        print message.toXml()
        self.xmlstream.send(message)
        qu -= 1
        print "queue2", qu



def cancel_watch(report, ww_num):
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
        mydb.query(sql)

def process(raw):
    try:
        #raw = raw.replace("\015\015\012", "\n")
        real_process(raw)
    except:
        io = StringIO.StringIO()
        traceback.print_exc(file=io)
        msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),raw))
        msg['subject'] = 'new_watch.py Traceback'
        msg['From'] = "ldm@mesonet.agron.iastate.edu"
        msg['To'] = "akrherz@iastate.edu"

        s = smtplib.SMTP()
        s.connect()
        s.sendmail(msg["From"], msg["To"], msg.as_string())
        s.close()


def real_process(raw):

    report = raw.replace("\015\015\012", "").replace("\n", "").replace("'", " ")
    # Set up Station Table!
    st = stationTable.stationTable("/home/ldm/pyWWA/support/spcwatch.tbl")

    # Determine the Saw number
    saw = re.findall("SAW([0-9])", report)[0][0]

    # Determine the Watch type
    tokens = re.findall("WW ([0-9]*) (TEST)? ?(SEVERE TSTM|TORNADO|SEVERE THUNDERSTORM)", report)
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
    if (lon2 == lon1): #same as EW
        orientation = "EAST AND WEST"
    if (lat1 == lat2): #same as NS
        orientation = "NORTH AND SOUTH"

    if (orientation == "EAST AND WEST"):
        lat11 = lat1
        lat12 = lat1
        lat21 = lat2
        lat22 = lat2
        lon11 = lon1 - (box_radius * KM_SM) / (111.11 * math.cos(math.radians(lat1)))
        lon12 = lon1 + (box_radius * KM_SM) / (111.11 * math.cos(math.radians(lat1)))
        lon21 = lon2 - (box_radius * KM_SM) / (111.11 * math.cos(math.radians(lat2)))
        lon22 = lon2 + (box_radius * KM_SM) / (111.11 * math.cos(math.radians(lat2)))

    elif (orientation == "NORTH AND SOUTH"):
        lon11 = lon1
        lon12 = lon1
        lon21 = lon2
        lon22 = lon2
        lat11 = lat1 + (box_radius * KM_SM) / 111.11
        lat12 = lat1 - (box_radius * KM_SM) / 111.11
        lat21 = lat2 + (box_radius * KM_SM) / 111.11
        lat22 = lat2 - (box_radius * KM_SM) / 111.11

    elif (orientation == "EITHER SIDE"):
        slope = (lat2 - lat1)/(lon2 - lon1)
        angle =( math.pi / 2.0) - math.fabs( math.atan(slope))
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
    mydb.query(sql)

    # Insert into our watches table
    for tbl in ('watches', 'watches_current'):
        sql = "INSERT into %s(sel, issued, expired, type, report, geom, num) \
   VALUES('SEL%s','%s','%s','%s','%s','SRID=4326;MULTIPOLYGON(((%s)))', %s)" % \
   (tbl, saw, \
   sTS.strftime("%Y-%m-%d %H:%M"), eTS.strftime("%Y-%m-%d %H:%M"), types[ww_type], \
   raw.replace("'","\\'"), wkt, ww_num)
        mydb.query(sql)

    # Figure out WFOs affected...
    jabberTxt = "SPC issues %s watch till %sZ http://www.spc.noaa.gov/products/watch/ww%04i.html" % (ww_type, eTS.strftime("%H:%M"), int(ww_num) )
    jabberTxtHTML = "Storm Prediction Center issues <a href=\"http://www.spc.noaa.gov/products/watch/ww%04i.html\">%s watch</a> till %s UTC " % (int(ww_num), ww_type, eTS.strftime("%H:%M") )
    if (jabberReplacesTxt != ""):
        jabberTxt += ", new watch replaces "+ jabberReplacesTxt[:-1]
        jabberTxtHTML += ", new watch replaces "+ jabberReplacesTxt[:-1]

    jabberTxt += " (http://mesonet.agron.iastate.edu/GIS/apps/rview/watch.phtml?year=%s&amp;num=%s) " % (sTS.year, ww_num)
    jabberTxtHTML += " (<a href=\"http://mesonet.agron.iastate.edu/GIS/apps/rview/watch.phtml?year=%s&amp;num=%s\">Watch Quickview</a>) " % (sTS.year, ww_num)

    sql = "SELECT distinct wfo from nws_ugc WHERE \
         contains('SRID=4326;MULTIPOLYGON(((%s)))', geom)" % (wkt,)
    rs = mydb.query(sql).dictresult()
    global qu
    for i in range(len(rs)):
        qu += 1
        wfo = rs[i]['wfo']
        mess = "%s: %s" % (wfo, jabberTxt)
        jabber.sendMessage(mess, jabberTxtHTML)

def killer():
    global qu
    print "queue", qu
    if (qu == 0):
        reactor.stop()
    reactor.callLater(10, killer)


raw = sys.stdin.read()

myJid = jid.JID('iembot_ingest@%s/new_watch_%s' % (secret.chatserver, mx.DateTime.now().ticks() ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = JabberClient(myJid)

factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
reactor.connectTCP(secret.chatserver,5222,factory)
reactor.callLater(0, process, raw)
reactor.callLater(10, killer)
reactor.run()
