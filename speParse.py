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
""" SPENES product ingestor """

from twisted.python import log
import os
log.startLogging(open('logs/speParse.log', 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

import sys, re, pdb, mx.DateTime
import traceback, StringIO
import smtplib
import secret
import common
from email.MIMEText import MIMEText

from twisted.words.protocols.jabber import client, jid
from twisted.words.xish import domish
from twisted.internet import reactor

from support import TextProduct
import pg
POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser, passwd=secret.dbpass)

raw = sys.stdin.read()



def process(raw):
    try:
        real_process(raw)
    except:
        io = StringIO.StringIO()
        traceback.print_exc(file=io)
        msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),raw))
        msg['subject'] = 'speParse.py Traceback'
        msg['From'] = secret.parser_user
        msg['To'] = secret.error_email

        s = smtplib.SMTP()
        s.connect()
        s.sendmail(msg["From"], msg["To"], msg.as_string())
        s.close()


def real_process(raw):
    sqlraw = raw.replace("'", "\\'").replace("\015\015\012", "\n")
    prod = TextProduct.TextProduct(raw)

    product_id = prod.get_product_id()
    sql = "INSERT into text_products(product, product_id) \
      values ('%s','%s')" % (sqlraw, product_id)
    POSTGIS.query(sql)

    tokens = re.findall("ATTN (WFOS|RFCS)(.*)", raw)
    for tpair in tokens:
        wfos = re.findall("([A-Z]+)\.\.\.", tpair[1])
        for wfo in wfos:
            body = "%s: NESDIS issues Satellite Precipitation Estimates %s?pid=%s" % \
         (wfo, secret.PROD_URL, product_id)
            htmlbody = "NESDIS issues <a href='%s?pid=%s'>Satellite Precipitation Estimates</a>" %(secret.PROD_URL, product_id,)
            jabber.sendMessage(body, htmlbody)


def killer():
    reactor.stop()

myJid = jid.JID('%s@%s/spe_%s' % \
      (secret.iembot_ingest_user, secret.chatserver, \
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = common.JabberClient(myJid)

factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)

reactor.connectTCP(secret.connect_chatserver,5222,factory)
reactor.callLater(0, process, raw)
reactor.callLater(30, killer)
reactor.run()



