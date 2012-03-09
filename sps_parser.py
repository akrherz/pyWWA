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
""" SPS product ingestor 
$Id: $:
"""

from twisted.python import log
from twisted.python import logfile
import os
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('sps_parser.log','logs/'))


import StringIO, traceback, mx.DateTime
from email.MIMEText import MIMEText

import secret
import common, pg
from support import TextProduct, ldmbridge, reference

from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.internet import reactor
from twisted.mail import smtp
from twisted.enterprise import adbapi

POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser, passwd=secret.dbpass)
DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, 
                               host=secret.dbhost, password=secret.dbpass,
                               cp_reconnect=True)


errors = StringIO.StringIO()
EMAILS = 10

ugc_dict = {}
sql = "SELECT name, ugc from nws_ugc WHERE name IS NOT Null"
rs = POSTGIS.query(sql).dictresult()
for i in range(len(rs)):
    name = (rs[i]["name"]).replace("\x92"," ")
    ugc_dict[ rs[i]['ugc'] ] = name
POSTGIS.close()

def countyText(u):
    countyState = {}
    c = ""
    for k in range(len(u)):
        cnty = u[k]
        stateAB = cnty[:2]
        if (not countyState.has_key(stateAB)):
            countyState[stateAB] = []
        if (not ugc_dict.has_key(cnty)):
            name = "((%s))" % (cnty,)
        else:
            name = ugc_dict[cnty]
        countyState[stateAB].append(name)

    for st in countyState.keys():
        countyState[stateAB].sort()
        c +=" %s [%s] and" %(", ".join(countyState[st]), st)
    return c[:-4]




# LDM Ingestor
class myProductIngestor(ldmbridge.LDMProductReceiver):

    def process_data(self, buf):
        try:
            real_process(buf)
        except Exception, myexp:
            common.email_error(myexp, buf)

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


def real_process(raw):
    sqlraw = raw.replace("\015\015\012", "\n")
    prod = TextProduct.TextProduct(raw)

    product_id = prod.get_product_id()
    if len(prod.segments) == 0:
        # Send the office an alert that they probably don't have a trailing
        # $$
        print "Missing $$ detected", product_id
        mess = "%s: iembot failed to process product: %s\nError: %s" % \
               (prod.get_iembot_source(), product_id, "Missing $$")
        htmlmess = "<span style='color: #FF0000; font-weight: bold;'>\
iembot processing error:</span><br />Product: %s<br />Error: %s" % \
            (product_id, "Missing $$")
        jabber.sendMessage(mess, htmlmess)
        return


    if (prod.segments[0].giswkt):
        sql = """INSERT into text_products(product, product_id, geom) values (%s,%s, %s)"""
        myargs = (sqlraw, product_id, prod.segments[0].giswkt )
        
    else:
        sql = "INSERT into text_products(product, product_id) values (%s,%s)" 
        myargs = (sqlraw, product_id)
    deffer = DBPOOL.runOperation(sql, myargs)
    deffer.addErrback( common.email_error, sqlraw )
    deffer.addErrback( log.err )

    for seg in prod.segments:
        headline = "[NO HEADLINE FOUND IN SPS]"
        if len(seg.headlines) > 0:
            headline = (seg.headlines[0]).replace("\n", " ")
        elif raw.find("SPECIAL WEATHER STATEMENT") > 0:
            headline = "Special Weather Statement"
        counties = countyText(seg.ugc)
        if (counties.strip() == ""):
            counties = "entire area"
        expire = ""
        if (seg.ugcExpire is not None):
            expire = "till "+ (seg.ugcExpire - mx.DateTime.RelativeDateTime(hours= reference.offsets[prod.z] )).strftime("%-I:%M %p ")+ prod.z


        mess = "%s: %s issues %s for %s %s %s?pid=%s" % (prod.source[1:], \
           prod.source[1:], headline, counties, expire, secret.PROD_URL, \
           product_id)
        htmlmess = "%s issues <a href='%s?pid=%s'>%s</a> for %s %s" % ( prod.source[1:], secret.PROD_URL, product_id, headline, counties, expire)
        log.msg(mess)

        jabber.sendMessage(mess, htmlmess)

        twt = "%s for %s %s" % (headline, counties, expire)
        url = "%s?pid=%s" % (secret.PROD_URL, product_id)
        common.tweet([prod.source[1:],], twt, url)

myJid = jid.JID('%s@%s/sps2bot_%s' % (secret.iembot_ingest_user, 
                                      secret.chatserver, 
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = common.JabberClient(myJid)

factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )

reactor.connectTCP(secret.connect_chatserver,5222,factory)

ldm = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.run()
