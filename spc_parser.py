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
""" SPC Geo Products Parser! """

__revision__ = '$Id: spc_parser.py 4513 2009-01-06 16:57:49Z akrherz $'

# Twisted Python imports
from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.internet import reactor
from twisted.python import log
from twisted.enterprise import adbapi
from twisted.mail import smtp

# Standard Python modules
import os, re, traceback, StringIO, smtplib
from email.MIMEText import MIMEText

# Python 3rd Party Add-Ons
import mx.DateTime, pg

# pyWWA stuff
from support import ldmbridge, TextProduct, reference
import secret
import common

def exception_hook(kwargs):
    print kwargs
    if not kwargs.has_key("failure"):
        return

#log.addObserver(exception_hook)
log.startLogging(open('logs/spc_parser.log','a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser, 
                     passwd=secret.dbpass)
DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, 
                               host=secret.dbhost, password=secret.dbpass)
EMAILS = 10





# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


    def process_data(self, buf):
        """ Process the product """
        real_parser(buf)

def consumer(category, lons, lats):
    if lons[0] != lons[-1]:
        lons.append( lons[0] )
        lats.append( lats[0] )
    geometry = "MULTIPOLYGON((( "
    for i in range(len(lons)):
        geometry += "%.2f %.2f," % (lons[i], lats[i])
    geometry = geometry[:-1] + " )))"
    sql = "select distinct wfo from nws_ugc \
       WHERE ( st_overlaps(geomFromEWKT('SRID=4326;%s'), geom) or \
       st_contains(geomFromEWKT('SRID=4326;%s'), geom) )and \
       polygon_class = 'C'" % \
       (geometry, geometry)

    rs = POSTGIS.query(sql).dictresult()
    affectedWFOS = []
    for i in range(len(rs)):
        affectedWFOS.append( rs[i]['wfo'] )

    print "Category: %s Lons: %s Lats: %s WFOS: %s" % (category, lons, lats, \
        affectedWFOS)
    return affectedWFOS
   

def real_parser(buf):
    buf = buf.replace("\015\015\012", "\n")
    tp = TextProduct.TextProduct(buf)
    tokens = re.findall("VALID TIME ([0-9]{6})Z - ([0-9]{6})Z", buf)
    day1 = int(tokens[0][0][:2])
    hour1 = int(tokens[0][0][2:4])
    min1 = int(tokens[0][0][4:])
    day2 = int(tokens[0][1][:2])
    hour2 = int(tokens[0][1][2:4])
    min2 = int(tokens[0][1][4:])
    valid = tp.issueTime + mx.DateTime.RelativeDateTime(day=day1,hour=hour1,minute=min1)
    expire = tp.issueTime + mx.DateTime.RelativeDateTime(day=day2,hour=hour2,minute=min2)
    if day1 < tp.issueTime.day and day1 == 1:
        valid += mx.DateTime.RelativeDateTime(months=1)
    if day2 < tp.issueTime.day and day2 == 1:
        expire += mx.DateTime.RelativeDateTime(months=1)

    if tp.afos == "PTSDY1":
        channelprefix = ""
        product_descript = "Day 1 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day1otlk_%s.html" % (valid.year, valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PFWFD1":
        channelprefix = "DAY1FWX"
        product_descript = "Day 1 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (valid.year, valid.strftime("%y%m%d") )

    wfos = {'TSTM': [], 'EXTM': [], 'SLGT': [], 'CRIT': [], 'MDT': [], 'HIGH': [] }

    category = None
    for line in buf.split("\n"):
        if re.match("^(EXTM|SLGT|MDT|HIGH|CRIT|TSTM) ", line) is not None:
            if category:
                wfos[category].append( consumer(category, lons, lats) )
            category = line.split()[0]
            lats = []
            lons = []
        if category is None:
            continue
        tokens = re.findall("([0-9]{8})", line)
        for t in tokens:
            if t == "99999999":
                continue
            lats.append( float(t[:4]) / 100.0 )
            lon = float(t[4:]) / 100.0
            if int(t[4]) < 3:
                lons.append( -1.0 * (lon + 100.0) )
            else:
                lons.append( -1.0 * lon )
    if category:
        wfos[category].append( consumer(category, lons, lats) )

    codes = {'SLGT': "Slight", 'MDT': "Moderate", 'HIGH': 'High', 'CRIT': 'Critical', 'EXTM': 'Extreme'}


    msgs = {}
    htmlmsgs = {}
    twts = {}
    for cat in ['SLGT','MDT','HIGH','CRIT','EXTM']:
        for z in wfos[cat]:
            for wfo in z:
                msgs[wfo] = "%s%s: The Storm Prediction Center issues Day 1 %s risk \
for portions of %s %s" % (channelprefix, wfo, cat, wfo, url)
                htmlmsgs[wfo] = "The Storm Prediction Center issues \
<a href=\"%s\">%s %s Risk</a> for portions of %s's area" % (url, \
  product_descript, codes[cat], wfo)
                twts[wfo] = "SPC issues Day 1 %s risk" % (cat,)

    for wfo in msgs.keys():
        jabber.sendMessage( msgs[wfo], htmlmsgs[wfo] )

    # Fancy pants twitter logic
    rlkp = {}
    for wfo in twts.keys():
        if not rlkp.has_key(twts[wfo]):
            rlkp[ twts[wfo] ] = []
        rlkp[ twts[wfo] ].append( wfo )
    for twt in rlkp.keys():
        common.tweet( rlkp[twt], twt, url )


myJid = jid.JID('%s@%s/spc_parser_%s' % \
      (secret.iembot_ingest_user, secret.chatserver, \
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = common.JabberClient(myJid)

factory.addBootstrap('//event/stream/authd', jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )

reactor.connectTCP(secret.connect_chatserver, 5222, factory)

ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run()
