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
os.environ['EMAILS'] = "10"





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
        try:
            real_parser(buf)
        except Exception, myexp:
            common.email_error(myexp, buf)

def consumer(category, metadata, lons, lats):
    """
    Do the geometric stuff we care about, including storage
    """
    # Here is a hack to account for when there is only two points
    # in the polygon.
    if len(lons) == 2:
        lons.append( lons[-1] + 0.01 )
        lats.append( lats[-1] + 0.01 )
    if lons[0] != lons[-1]:
        lons.append( lons[0] )
        lats.append( lats[0] )
    geometry = "MULTIPOLYGON((( "
    for i in range(len(lons)):
        geometry += "%.2f %.2f," % (lons[i], lats[i])
    geometry = geometry[:-1] + " )))"
    # Insert geometry into database table please
    sql = """INSERT into spc_outlooks(issue, valid, expire,
        category, category_type, day, outlook_type, geom)
    VALUES ('%s+00', '%s+00', '%s+00', '%s', '%s', '%s', '%s', 
    'SRID=4326;%s')""" % (
            metadata['issue'], metadata['valid'], metadata['expire'],
            category, metadata['category_type'], metadata['day'], 
            metadata['outlook_type'], geometry)
    DBPOOL.runOperation( sql ).addErrback(common.email_error, sql)
    
    # Search for WFOs
    sql = "select distinct wfo from nws_ugc \
       WHERE ( st_overlaps(geomFromEWKT('SRID=4326;%s'), geom) or \
       st_contains(geomFromEWKT('SRID=4326;%s'), geom) )and \
       polygon_class = 'C'" % \
       (geometry, geometry)

    rs = POSTGIS.query(sql).dictresult()
    affectedWFOS = []
    for i in range(len(rs)):
        affectedWFOS.append( rs[i]['wfo'] )

    print "Category Type: %s Category: %s #Lons: %s #Lats: %s #WFOS: %s" % (
        metadata['category_type'], category, len(lons), len(lats), 
        len(affectedWFOS))
    return affectedWFOS

def find_issue_expire(buf, tp):
    """
    Determine the period this product is valid for
    """
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
    return valid, expire

def real_parser(buf):
    buf = buf.replace("\015\015\012", "\n")
    tp = TextProduct.TextProduct(buf)
    valid, expire = find_issue_expire(buf, tp)

    metadata = {'valid': valid, 'expire': expire, 'issue': tp.issueTime}

    if tp.afos == "PTSDY1":
        metadata['day'] = '1'
        metadata['outlook_type'] = 'C'
        channelprefix = ""
        product_descript = "Day 1 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day1otlk_%s.html" % (valid.year, valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PTSDY2":
        metadata['day'] = '2'
        metadata['outlook_type'] = 'C'
        channelprefix = ""
        product_descript = "Day 2 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day2otlk_%s.html" % (valid.year, valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PTSDY3":
        metadata['day'] = '3'
        metadata['outlook_type'] = 'C'
        channelprefix = ""
        product_descript = "Day 3 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day3otlk_%s.html" % (valid.year, valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PTSD48":
        metadata['day'] = '4'
        metadata['outlook_type'] = 'C'
        channelprefix = ""
        product_descript = "Day 4-8 Convective"
        url = "http://www.spc.noaa.gov/products/exper/day4-8/archive/%s/day4-8_%s.html" % (valid.year, valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PFWFD1":
        metadata['day'] = '1'
        metadata['outlook_type'] = 'F'
        channelprefix = "DAY1FWX"
        product_descript = "Day 1 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (valid.year, valid.strftime("%y%m%d") )
    if tp.afos == "PFWFD2":
        metadata['day'] = '2'
        metadata['outlook_type'] = 'F'
        channelprefix = "DAY2FWX"
        product_descript = "Day 2 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (valid.year, valid.strftime("%y%m%d") )
    if tp.afos == "PFWF38":
        metadata['day'] = '3'
        metadata['outlook_type'] = 'F'
        channelprefix = "DAY3FWX"
        product_descript = "Day 3-8 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (valid.year, valid.strftime("%y%m%d") )

    # Remove any previous data
    POSTGIS.query("""DELETE from spc_outlooks where valid = '%(valid)s+00' 
    and expire = '%(expire)s+00' 
    and outlook_type = '%(outlook_type)s' and day = '%(day)s'""" % metadata)

    wfos = {'TSTM': [], 'EXTM': [], 'SLGT': [], 'CRIT': [], 'MDT': [], 
            'HIGH': [] }

    # First we split the product by &&
    for segment in buf.split("&&")[:-1]:
        # We need to figure out the probabilistic or category
        tokens = re.findall("\.\.\. (.*) \.\.\.", segment)
        metadata['category_type'] = tokens[0][0]
        # Now we loop over the lines looking for data
        category = None
        for line in segment.split("\n"):
            if re.match("^(D[3-8]|EXTM|SLGT|MDT|HIGH|CRIT|TSTM|0\.[0-9][0-9]) ", line) is not None:
                if category is not None:
                    awfos = consumer(category, metadata, lons, lats)
                    if wfos.has_key( category ):
                        wfos[category].append( awfos )
                category = line.split()[0]
                lats = []
                lons = []
            if category is None:
                continue
            tokens = re.findall("([0-9]{8})", line)
            for t in tokens:
                # new segment
                if t == "99999999":
                    awfos = consumer(category, metadata, lons, lats)
                    if wfos.has_key( category ):
                        wfos[category].append( awfos )
                    lats = []
                    lons = []
                    continue
                lats.append( float(t[:4]) / 100.0 )
                lon = float(t[4:]) / 100.0
                if int(t[4]) < 3:
                    lons.append( -1.0 * (lon + 100.0) )
                else:
                    lons.append( -1.0 * lon )
        if category is not None:
            awfos = consumer(category, metadata, lons, lats)
            if wfos.has_key( category ):
                wfos[category].append( awfos )

    # Only tweet or Jabber for Day1, for now
    if tp.afos not in ('PTSDY1','PFWFD1'):
        return
    
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

    # Generic for SPC
    twt = "The Storm Prediction Center issues %s Outlook" %( product_descript,)
    common.tweet(['SPC',], twt, url)

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
