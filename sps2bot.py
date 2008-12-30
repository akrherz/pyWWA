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
""" SPS product ingestor """

from twisted.python import log
import os
log.startLogging(open('logs/sps2bot.log','a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

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
DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, host=secret.dbhost, password=secret.dbpass)


errors = StringIO.StringIO()
EMAILS = 10

ugc_dict = {}
sql = "SELECT name, ugc from nws_ugc WHERE name IS NOT Null"
rs = POSTGIS.query(sql).dictresult()
for i in range(len(rs)):
    name = (rs[i]["name"]).replace("\x92"," ")
    ugc_dict[ rs[i]['ugc'] ] = name

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


def email_error(message, product_text):
    """
    Generic something to send email error messages 
    """
    global EMAILS
    #io = StringIO.StringIO()
    #traceback.print_exc(file=io)
    #log.msg( message )
    EMAILS -= 1
    if (EMAILS < 0):
        return

    msg = MIMEText("Exception:\n%s\n\nRaw Product:\n%s" \
                 % (message, product_text))
    msg['subject'] = 'sps2bot.py Traceback'
    msg['From'] = secret.parser_user
    msg['To'] = 'akrherz@iastate.edu'
    smtp.sendmail("mailhub.iastate.edu", msg["From"], msg["To"], msg)


# LDM Ingestor
class myProductIngestor(ldmbridge.LDMProductReceiver):

    def process_data(self, buf):
        try:
            real_process(buf)
        except Exception, myexp:
            email_error(myexp, buf)

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


def real_process(raw):
    sqlraw = raw.replace("'", "\\'")
    sqlraw = sqlraw.replace("\015\015\012", "\n")
    prod = TextProduct.TextProduct(raw)

    product_id = prod.get_product_id()
    if (prod.segments[0].giswkt):
        sql = "INSERT into text_products(product, product_id, geom) \
      values ('%s','%s', '%s')" % (sqlraw, product_id,prod.segments[0].giswkt )
        DBPOOL.runOperation(sql).addErrback( email_error, sql)
    else:
        sql = "INSERT into text_products(product, product_id) \
      values ('%s','%s')" % (sqlraw, product_id)
        DBPOOL.runOperation(sql).addErrback( email_error, sql)

    for seg in prod.segments:
        headline = "[NO HEADLINE FOUND IN SPS]"
        if (len(seg.headlines) > 0):
            headline = (seg.headlines[0]).replace("\n", " ")
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

myJid = jid.JID('%s@%s/sps2bot_%s' % \
      (secret.iembot_ingest_user, secret.chatserver, \
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
