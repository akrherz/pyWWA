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

from twisted.python import log
from twisted.python import logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('spc_parser.log','logs'))

# Twisted Python imports
from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.internet import reactor
from twisted.enterprise import adbapi

# Python 3rd Party Add-Ons
import mx.DateTime

# pyWWA stuff
from support import ldmbridge, TextProduct, spcpts
import os
import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))
import common

def exception_hook(kwargs):
    print kwargs
    if not kwargs.has_key("failure"):
        return

DBPOOL = adbapi.ConnectionPool("twistedpg", database="postgis", 
                               host=config.get('database','host'), 
                               user=config.get('database','user'),
                               password=config.get('database','password'), 
                               cp_reconnect=True)
WAITFOR = 20


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
            real_parser( buf )
        except Exception, exp:
            common.email_error(exp, buf)
        
def consume(spc, outlook):
    """
    Do the geometric stuff we care about, including storage
    """
    pgconn = DBPOOL.connect()
    txn = pgconn.cursor()

    # Insert geometry into database table please
    sql = """INSERT into spc_outlooks(issue, valid, expire,
        threshold, category, day, outlook_type, geom)
    VALUES ('%s+00', '%s+00', '%s+00', '%s', '%s', '%s', '%s', 
    'SRID=4326;%s')""" % (
            spc.issue,  spc.valid, spc.expire,
            outlook.threshold, outlook.category, spc.day, 
            spc.outlook_type, outlook.polygon.wkt)
    txn.execute( sql )
    
    # Search for WFOs
    sql = """select distinct wfo from nws_ugc 
       WHERE ( st_overlaps(geomFromEWKT('SRID=4326;%s'), geom) or 
       st_contains(geomFromEWKT('SRID=4326;%s'), geom) )and 
       polygon_class = 'C'""" % (outlook.polygon.wkt, outlook.polygon.wkt)

    txn.execute( sql )
    affectedWFOS = []
    for row in txn.fetchall():
        affectedWFOS.append( row['wfo'] )

    print "Category: %s Threshold: %s  #WFOS: %s" % (
        outlook.category, outlook.threshold,  len(affectedWFOS))
    
    txn.close()
    pgconn.commit()

    return affectedWFOS



def real_parser(buf):
    buf = buf.replace("\015\015\012", "\n")
    tp = TextProduct.TextProduct(buf)
    spc = spcpts.SPCPTS( tp )
    #spc.draw_outlooks()

    # Remove any previous data
    DBPOOL.runOperation("""DELETE from spc_outlooks where valid = '%s+00' 
    and expire = '%s+00' 
    and outlook_type = '%s' and day = '%s'""" % (
                    spc.valid, spc.expire, spc.outlook_type, spc.day))

    wfos = {'TSTM': [], 'EXTM': [], 'SLGT': [], 'CRIT': [], 'MDT': [], 
            'HIGH': [] }

    for outlook in spc.outlooks:
        arWFO = consume(spc, outlook)
        if wfos.has_key( outlook.threshold ):
            wfos[ outlook.threshold ].append( arWFO )
    
    if tp.afos == "PTSDY1":
        channelprefix = ""
        product_descript = "Day 1 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day1otlk_%s.html" % (
                                                    spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PTSDY2":
        channelprefix = ""
        product_descript = "Day 2 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day2otlk_%s.html" % (
                                            spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PTSDY3":
        channelprefix = ""
        product_descript = "Day 3 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day3otlk_%s.html" % (
                                                    spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PTSD48":
        channelprefix = ""
        product_descript = "Day 4-8 Convective"
        url = "http://www.spc.noaa.gov/products/exper/day4-8/archive/%s/day4-8_%s.html" % (
                                                    spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PFWFD1":
        channelprefix = "DAY1FWX"
        product_descript = "Day 1 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d") )
    if tp.afos == "PFWFD2":
        channelprefix = "DAY2FWX"
        product_descript = "Day 2 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d") )
    if tp.afos == "PFWF38":
        channelprefix = "DAY3FWX"
        product_descript = "Day 3-8 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d") )



    # Only tweet or Jabber for Day1, for now
    if tp.afos not in ('PTSDY1','PFWFD1'):
        return
    
    codes = {'SLGT': "Slight", 'MDT': "Moderate", 'HIGH': 'High', 
             'CRIT': 'Critical', 'EXTM': 'Extreme'}


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
                twts[wfo] = "SPC issues Day 1 %s risk for %s" % (cat, wfo)

    for wfo in msgs.keys():
        jabber.sendMessage( msgs[wfo], htmlmsgs[wfo] )

    for wfo in twts.keys():
        common.tweet( [wfo,], twts[wfo], url )

    # Generic for SPC
    twt = "The Storm Prediction Center issues %s Outlook" %( product_descript,)
    common.tweet(['SPC',], twt, url)

myJid = jid.JID('%s@%s/spc_parser_%s' % (config.get('xmpp', 'username'), 
    config.get('xmpp', 'domain'), mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, config.get('xmpp','password'))

jabber = common.JabberClient(myJid)

factory.addBootstrap('//event/stream/authd', jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )

reactor.connectTCP(config.get('xmpp', 'connecthost'), 5222, factory)

ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run()
