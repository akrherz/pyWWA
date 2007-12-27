#!/usr/bin/env python
# Parse SPENES to iembot

import sys, re, pdb, mx.DateTime
import traceback, StringIO
import smtplib
import secret
from email.MIMEText import MIMEText

from twisted.words.protocols.jabber import client, jid
from twisted.words.xish import domish
from twisted.internet import reactor

from pyIEM import iemdb
i = iemdb.iemdb()
postgis = i['postgis']


raw = sys.stdin.read()

qu = 0
_CHATSERVER = "localhost"

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
        message['to'] = "iembot@%s" % (_CHATSERVER,)
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
    sqlraw = raw.replace("'", "\\'")
    sql = "INSERT into text_products(product) values ('%s')" % (sqlraw,)
    postgis.query(sql)
    sql = "select last_value from text_products_id_seq"
    rs = postgis.query(sql).dictresult()
    id = rs[0]['last_value']


    tokens = re.findall("ATTN (WFOS|RFCS)(.*)", raw)
    for tpair in tokens:
        if (tpair[0] == "WFOS"):
            wfos = re.findall("([A-Z][A-Z][A-Z])", tpair[1])
            for wfo in wfos:
                qu += 1
                body = "%s: NESDIS issues Satellite Precipitation Estimates http://mesonet.agron.iastate.edu/p.php?id=%s" % \
         (wfo, id)
                htmlbody = "NESDIS issues <a href='http://mesonet.agron.iastate.edu/p.php?id=%s'>Satellite Precipitation Estimates</a>" %(id,)
                jabber.sendMessage(body, htmlbody)


def killer():
    global qu
    print "queue", qu
    if (qu == 0):
        reactor.stop()
    reactor.callLater(10, killer)

myJid = jid.JID('iembot_ingest@%s/spe_%s' % (_CHATSERVER, mx.DateTime.now().ticks() ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = JabberClient(myJid)

factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)

reactor.connectTCP(_CHATSERVER,5222,factory)
reactor.callLater(0, process, raw)
reactor.callLater(10, killer)
reactor.run()



