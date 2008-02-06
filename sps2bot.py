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
log.startLogging(open('/mesonet/data/logs/%s/sps2bot.log' \
    % (os.getenv("USER"),), 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

import StringIO, traceback, mx.DateTime
import smtplib
from email.MIMEText import MIMEText

import secret
import common
from support import TextProduct, ldmbridge, reference
import pg
POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser)

from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.internet import reactor


errors = StringIO.StringIO()


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




# LDM Ingestor
class myProductIngestor(ldmbridge.LDMProductReceiver):

    def process_data(self, buf):
        try:
            real_process(buf)
        except:
            io = StringIO.StringIO()
            traceback.print_exc(file=io)
            log.msg( io.getvalue() )
            msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),buf.replace("\015\015\012", "\n") ))
            msg['subject'] = 'sps2bot.py Traceback'
            msg['From'] = "ldm@mesonet.agron.iastate.edu"
            msg['To'] = "akrherz@iastate.edu"

            s = smtplib.SMTP()
            s.connect()
            s.sendmail(msg["From"], msg["To"], msg.as_string())
            s.close()

    def connectionLost(self,reason):
        log.msg(reason)
        log.msg("LDM Closed PIPE")


def real_process(raw):
    sqlraw = raw.replace("'", "\\'")
    sqlraw = sqlraw.replace("\015\015\012", "\n")
    prod = TextProduct.TextProduct(raw)

    product_id = prod.get_product_id()
    sql = "INSERT into text_products(product, product_id) \
      values ('%s','%s')" % (sqlraw, product_id)
    POSTGIS.query(sql)

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


        mess = "%s: %s issues %s for %s %s http://mesonet.agron.iastate.edu/p.php?pid=%s" % (prod.source[1:], \
           prod.source[1:], headline, counties, expire, product_id)
        htmlmess = "%s issues <a href='http://mesonet.agron.iastate.edu/p.php?pid=%s'>%s</a> for %s %s" % ( prod.source[1:], product_id, headline, counties, expire)
        log.msg(mess)

        jabber.sendMessage(mess, htmlmess)


myJid = jid.JID('iembot_ingest@%s/sps2bot_%s' \
   % (secret.chatserver, mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
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
