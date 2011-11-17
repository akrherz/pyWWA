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
""" MCD product ingestor 
$Id: $:
"""

from twisted.python import log
from twisted.python import logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('mcd_parser.log', 'logs'))

import os
import sys, re, pdb, mx.DateTime
import traceback, StringIO
import smtplib
import secret
from support import TextProduct, ldmbridge
import common

from twisted.words.protocols.jabber import client, jid
from twisted.words.xish import domish, xmlstream
from twisted.internet import reactor
from twisted.enterprise import adbapi

import ConfigParser
config = ConfigParser.ConfigParser()
config.read('/home/ldm/pyWWA/cfg.ini')

DBPOOL = adbapi.ConnectionPool("twistedpg", database="postgis", 
                               host=config.get('database','host'), 
                               user=config.get('database','user'),
                               password=config.get('database','password'), 
                               cp_reconnect=True)
# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


    def process_data(self, raw):
        try:
            real_process(raw)
        except Exception, myexp:
            common.email_error(myexp, raw)

def real_process(raw):
    tokens = re.findall("MESOSCALE DISCUSSION ([0-9]*)", raw)
    num = tokens[0]

    sqlraw = raw.replace("\015\015\012", "\n")
    prod = TextProduct.TextProduct(raw)

    product_id = prod.get_product_id()
    sql = """INSERT into text_products(product, product_id) 
      values (%s,%s)""" 
    sqlargs = (sqlraw, product_id)
    DBPOOL.runOperation(sql, sqlargs)

    # Figure out which WFOs SPC wants to alert...
    tokens = re.findall("ATTN\.\.\.WFO\.\.\.([\.,A-Z]*)", raw)
    tokens = re.findall("([A-Z][A-Z][A-Z])", tokens[0])
    for wfo in tokens:
        body = "%s: Storm Prediction Center issues Mesoscale Discussion http://www.spc.noaa.gov/products/md/md%s.html" % \
         (wfo, num)
        htmlbody = "Storm Prediction Center issues <a href='http://www.spc.noaa.gov/products/md/md%s.html'>Mesoscale Discussion #%s</a> (<a href='%s?pid=%s'>View text</a>)" %(num,num, secret.PROD_URL, product_id)
        jabber.sendMessage(body, htmlbody)


    # Figure out which areas SPC is interested in, default to WFOs
    affected = ", ".join(tokens)
    sections = sqlraw.split("\n\n")
    for sect in sections:
        if sect.find("AREAS AFFECTED...") == 0:
            affected = sect[17:]

    # Twitter this
    twt = "Mesoscale Discussion %s for %s" % (num, affected)
    url = "http://www.spc.noaa.gov/products/md/md%s.html" % (num, )
    tokens.append("SPC")
    common.tweet(tokens, twt, url) 

myJid = jid.JID('%s@%s/mcd_parser_%s' %  (config.get('xmpp', 'username'), config.get('xmpp', 'domain'), 
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, config.get('xmpp', 'password'))

jabber = common.JabberClient(myJid)

factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )

reactor.connectTCP(config.get('xmpp', 'connecthost'), 5222, factory)

ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )

reactor.run()
