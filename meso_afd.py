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
""" Hacky meso AFD processor """

from twisted.python import log
import os
log.startLogging(open('/mesonet/data/logs/%s/meso_afd.log' % (os.getenv("USER"),), 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

import sys, logging
import re
import mapscript
import math
import smtplib
import mx.DateTime
import sys
import StringIO
import traceback
from email.MIMEText import MIMEText

# Non standard Stuff
from support import TextProduct, ldmbridge
import common


# Database Connections
import pg, secret
POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser)

errors = StringIO.StringIO()

from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.words.xish import domish
from twisted.internet import reactor


errors = StringIO.StringIO()

class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ I lost my connection, should I do anything else? """
        log.msg("LDM Closed PIPE")


    def process_data(self, raw):
        try:
            real_process(raw)
        except:
            io = StringIO.StringIO()
            traceback.print_exc(file=io)
            msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),raw))
            msg['subject'] = 'meso_afd.py Traceback'
            msg['From'] = "ldm@mesonet.agron.iastate.edu"
            msg['To'] = "akrherz@iastate.edu"

            s = smtplib.SMTP()
            s.connect()
            s.sendmail(msg["From"], msg["To"], msg.as_string())
            s.close()



def real_process(raw):
    tp = TextProduct.TextProduct(raw)
    pil = tp.afos[:3]
    wfo = tp.afos[3:]
    sqlraw = raw.replace("'", "\\'")
    prod = TextProduct.TextProduct(raw)
    product_id = prod.get_product_id()
    print product_id

    tokens = re.findall("\.UPDATE\.\.\.MESOSCALE UPDATE", raw)
    if (len(tokens) == 0):
        return

    sql = "INSERT into text_products(product, product_id) \
      values ('%s','%s')" % (sqlraw, product_id)
    POSTGIS.query(sql)

    mess = "%s: %s issues Mesoscale %s http://mesonet.agron.iastate.edu/p.php?pid=%s" % \
        (wfo, wfo, pil, product_id)
    messHTML = "%s issues <a href=\"http://mesonet.agron.iastate.edu/p.php?pid=%s \">Mesoscale Forecast Discussion</a>" % \
        (wfo, product_id)
    jabber.sendMessage(mess, messHTML)


myJid = jid.JID('iembot_ingest@%s/meso_afd_%s' % \
      (secret.chatserver, mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
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

