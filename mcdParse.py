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
""" MCD product ingestor """

from twisted.python import log
import os
log.startLogging(open('/mesonet/data/logs/%s/mcdParse.log' \
    % (os.getenv("USER"),), 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

import sys, re, pdb, mx.DateTime
import traceback, StringIO
import smtplib
from email.MIMEText import MIMEText
import secret
from support import TextProduct

import pg
POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser)

from twisted.words.protocols.jabber import client, jid
from twisted.words.xish import domish
from twisted.internet import reactor

raw = sys.stdin.read()

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
        self.xmlstream.send(message)
        qu -= 1



def process(raw):
    try:
        real_process(raw)
    except:
        io = StringIO.StringIO()
        traceback.print_exc(file=io)
        msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),raw))
        msg['subject'] = 'mcdParse.py Traceback'
        msg['From'] = "ldm@mesonet.agron.iastate.edu"
        msg['To'] = "akrherz@iastate.edu"

        s = smtplib.SMTP()
        s.connect()
        s.sendmail(msg["From"], msg["To"], msg.as_string())
        s.close()


def real_process(raw):
    global qu
    tokens = re.findall("MESOSCALE DISCUSSION ([0-9]*)", raw)
    num = tokens[0]

    sqlraw = raw.replace("'", "\\'")
    sqlraw = sqlraw.replace("\015\015\012", "\n")
    prod = TextProduct.TextProduct(raw)

    product_id = prod.get_product_id()
    sql = "INSERT into text_products(product, product_id) \
      values ('%s','%s')" % (sqlraw, product_id)
    POSTGIS.query(sql)

    tokens = re.findall("ATTN\.\.\.WFO\.\.\.([\.,A-Z]*)", raw)
    tokens = re.findall("([A-Z][A-Z][A-Z])", tokens[0])
    for wfo in tokens:
        qu += 1
        body = "%s: Storm Prediction Center issues Mesoscale Discussion http://www.spc.noaa.gov/products/md/md%s.html" % \
         (wfo, num)
        htmlbody = "Storm Prediction Center issues <a href='http://www.spc.noaa.gov/products/md/md%s.html'>Mesoscale Discussion #%s</a> (<a href='http://mesonet.agron.iastate.edu/p.php?pid=%s'>View text</a>)" %(num,num,product_id)
        print body
        jabber.sendMessage(body, htmlbody)


def killer():
    global qu
    print "queue", qu
    if (qu == 0):
        reactor.stop()
    reactor.callLater(10, killer)

myJid = jid.JID('iembot_ingest@%s/ingestor_%s' % (secret.chatserver, mx.DateTime.now().ticks() ) )
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



