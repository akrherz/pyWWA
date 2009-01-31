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
""" Parser for NON AFOS Products :( """

__revision__ = '$Id: non_afos_parser.py 4513 2009-01-06 16:57:49Z akrherz $'

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

log.startLogging(open('logs/non_afos.log','a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser, 
                     passwd=secret.dbpass)
DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, 
                               host=secret.dbhost, password=secret.dbpass)
EMAILS = 10

def email_error(message, product_text):
    """
    Generic something to send email error messages 
    """
    global EMAILS
    log.msg( message )
    EMAILS -= 1
    if (EMAILS < 0):
        return

    msg = MIMEText("Exception:\n%s\n\nRaw Product:\n%s" \
                 % (message, product_text))
    msg['subject'] = 'non_afos.py Traceback'
    msg['From'] = secret.parser_user
    msg['To'] = secret.error_email
    smtp.sendmail("localhost", msg["From"], msg["To"], msg)

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
            email_error(myexp, buf)

prodDefinitions = {
    'CWA': 'Center Weather Advisory (CWA)',
    'MIS': 'Information Statement (MIS)',
}

fake_afos = {
  'FAUS20': 'MIS',
  'FAUS21': 'CWA',
  'FAUS22': 'CWA',
  'FAUS23': 'CWA',
  'FAUS24': 'CWA',
  'FAUS25': 'CWA',
  'FAUS26': 'CWA',
}

def real_parser(buf):
    nws = TextProduct.TextProduct(buf)
    lkp = nws.wmo[:5]
    if not fake_afos.has_key(lkp):
        print "MISSING KEY", lkp
        return
    nws.afos = "%s%s" % (fake_afos[lkp], nws.source[1:])

    # Always insert the product into the text archive database
    sqlraw = buf.replace("\015\015\012", "\n").replace("\000", "").strip()
    product_id = nws.get_product_id()
    sql = "INSERT into text_products(product, product_id) \
      values ('%s','%s')" % (sqlraw, product_id)
    DBPOOL.runOperation(sql).addErrback( email_error, sql)
    myurl = "%s?pid=%s" % (secret.PROD_URL, product_id)
    pil = nws.afos[:3]
    wfo = nws.source[1:]
    prodtxt = "(%s)" % (pil,)
    if (prodDefinitions.has_key(pil)):
        prodtxt = prodDefinitions[pil]
    
    mess = "%s: %s issues %s %s" % (wfo, wfo, prodtxt, myurl)
    htmlmess = "%s issues <a href=\"%s\">%s</a> " % (wfo, myurl,prodtxt)
    jabber.sendMessage(mess, htmlmess)

myJid = jid.JID('%s@%s/non_afos_%s' % \
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
