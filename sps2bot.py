# Send SPS messages to iembot

import sys, StringIO, logging, re, traceback, mx.DateTime
import smtplib
from email.MIMEText import MIMEText

import secret, os
from common import *
from pyIEM.nws import TextProduct
from pyIEM import iemdb, ldmbridge
i = iemdb.iemdb(secret.dbhost)
postgis = i['postgis']

from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.words.xish import domish
from twisted.internet import reactor


errors = StringIO.StringIO()
logging.basicConfig(filename='/mesonet/data/logs/%s/sps2bot.log' % (os.getenv("USER"), ), filemode='a')
logger=logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


ugc_dict = {}
sql = "SELECT name, ugc from nws_ugc WHERE name IS NOT Null"
rs = postgis.query(sql).dictresult()
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

offsets = {
 'EDT': 4,
 'CDT': 5, 'EST': 5,
 'MDT': 6, 'CST': 6,
 'PDT': 7, 'MST': 7,
 'ADT': 8, 'PST': 8,
 'HDT': 9, 'AST': 9,
           'HST':10,
}



# LDM Ingestor
class myProductIngestor(ldmbridge.LDMProductReceiver):

    def processData(self, buf):
        try:
            real_process(buf)
        except:
            io = StringIO.StringIO()
            traceback.print_exc(file=io)
            logger.error( io.getvalue() )
            msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),buf.replace("\015\015\012", "\n") ))
            msg['subject'] = 'sps2bot.py Traceback'
            msg['From'] = "ldm@mesonet.agron.iastate.edu"
            msg['To'] = "akrherz@iastate.edu"

            s = smtplib.SMTP()
            s.connect()
            s.sendmail(msg["From"], msg["To"], msg.as_string())
            s.close()

    def connectionLost(self,reason):
        logger.info("LDM Closed PIPE")


def real_process(raw):
    sqlraw = raw.replace("'", "\\'")
    sqlraw = sqlraw.replace("\015\015\012", "\n")
    prod = TextProduct.TextProduct(raw)

    for seg in prod.segments:
        headline = "[NO HEADLINE FOUND IN SPS]"
        if (len(seg.headlines) > 0):
            headline = (seg.headlines[0]).replace("\n", " ")
        counties = countyText(seg.ugc)
        if (counties.strip() == ""):
            counties = "entire area"
        expire = ""
        if (seg.ugcExpire is not None):
            expire = "till "+ (seg.ugcExpire - mx.DateTime.RelativeDateTime(hours= offsets[prod.z] )).strftime("%-I:%M %p ")+ prod.z


        sql = "INSERT into text_products(product) values ('%s')" % (sqlraw,)
        postgis.query(sql)
        sql = "select last_value from text_products_id_seq"
        rs = postgis.query(sql).dictresult()
        id = rs[0]['last_value']

        mess = "%s: %s issues %s for %s %s http://mesonet.agron.iastate.edu/p.php?id=%s" % (prod.source[1:], \
           prod.source[1:], headline, counties, expire, id)
        htmlmess = "%s issues <a href='http://mesonet.agron.iastate.edu/p.php?id=%s'>%s</a> for %s %s" % ( prod.source[1:], id, headline, counties, expire)
        logger.info(mess)

        jabber.sendMessage(mess, htmlmess)


myJid = jid.JID('iembot_ingest@%s/sps2bot_%s' % (secret.chatserver, mx.DateTime.now().ticks() ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = JabberClient(myJid)

factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )

reactor.connectTCP(secret.connect_chatserver,5222,factory)

ldm = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.run()
