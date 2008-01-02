# Generic Text product to iembot

from twisted.python import log
import os
log.startLogging(open('/mesonet/data/logs/%s/gp.log' % (os.getenv("USER"),), 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

import sys, re
import traceback
import StringIO
import mx.DateTime
import secret
from common import *
from pyIEM.nws import TextProduct
from email.MIMEText import MIMEText
import smtplib

from pyIEM import iemdb, ldmbridge
#i = iemdb.iemdb(secret.dbhost)
#postgis = i['postgis']
import psycopg2

errors = StringIO.StringIO()

offsets = {
 'EDT': 4,
 'CDT': 5, 'EST': 5,
 'MDT': 6, 'CST': 6,
 'PDT': 7, 'MST': 7,
 'ADT': 8, 'PST': 8,
 'HDT': 9, 'AST': 9,
           'HST':10,
}

gulfwfo = ['KEY', 'SJU', 'TBW', 'TAE', 'JAX', 'MOB', 'HGX', 'CRP','BMX','EWX', 'FWD', 'SHV', 'JAN', 'LIX', 'LCH', 'FFC', 'MFL', 'MLB','CHS','CAE','RAH','ILM','AKQ']
spcwfo = ['RNK',]

routes = {'TCPAT[0-9]': gulfwfo,
          'TCMAT[0-9]': gulfwfo,
          'TCDAT[0-9]': gulfwfo,
          'SWODY[1-2]': spcwfo,}

from twisted.words.protocols.jabber import client, jid
from twisted.words.xish import domish
from twisted.internet import reactor


errors = StringIO.StringIO()

class myProductIngestor(ldmbridge.LDMProductReceiver):

    def processData(self, buf):
        try:
            real_process(buf)
        except:
            io = StringIO.StringIO()
            traceback.print_exc(file=io)
            log.msg( io.getvalue() )
            msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),buf.replace("\015\015\012", "\n") ))
            msg['subject'] = 'generic_product.py Traceback'
            msg['From'] = "ldm@mesonet.agron.iastate.edu"
            msg['To'] = "akrherz@iastate.edu"

            s = smtplib.SMTP()
            s.connect()
            s.sendmail(msg["From"], msg["To"], msg.as_string())
            s.close()

    def connectionLost(self,reason):
        log.msg("LDM Closed PIPE")


prodDefinitions = {
    'HLS': 'Hurricane Local Statement (HLS)',
    'NOW': 'Short-term Forecast (NOW)',
    'HWO': 'Hazardous Weather Outlook (HWO)',
    'AFD': 'Area Forecast Discussion (AFD)',
    'AWU': 'Area Weather Update (AWU)',
    'PNS': 'Public Information Statement (PNS)',
    'FFW': 'Flash Flood Warning (FFW)',
    'FLS': 'Flood Advisory (FLS)',
    'FFS': 'Flash Flood Statement (FFS)',
    'FLW': 'Flood Warning (FLW)',
    'ESF': 'Hydrologic Outlook (ESF)',
    'PSH': 'Post Tropical Event Report (PSH)',
    'RER': 'Record Event Report (RER)',
    'FTM': 'Free Text Message (FTM)',
    'ADM': 'Administrative Message (ADM)',
    'CAE': 'Child Abduction Emergency (CAE)',
    'ADR': 'Administrative Message (ADR)',
    'TOE': 'Telephone Outage Emergency (TOE)',
    'LAE': 'Local Area Emergency (LAE)',
    'ADR': 'Administrative Message (ADR)',
    'AVA': 'Avalanche Watch (AVA)',
    'AVW': 'Avalanche Warning (AVW)',
    'CDW': 'Civil Danger Warning (CDW)',
    'CEM': 'Civil Emergency Message (CEM)',
    'EQW': 'Earthquake Warning (EQW)',
    'EVI': 'Evacuation Immediate (EVI)',
    'FRW': 'Fire Warning (FRW)',
    'HMW': 'Hazardous Materials Warning (HMW)',
    'LEW': 'Law Enforcement Warning (LEW)',
    'NMN': 'Network Message Notification (NMN)',
    'NUW': 'Nuclear Power Plant Warning (NUW)',
    'RHW': 'Radiological Hazard Warning (RHW)',
    'SPW': 'Shelter In Place Warning (SPW)',
    'VOW': 'Volcano Warning (VOW)',
}

ugc_dict = {}
sql = "SELECT name, ugc from nws_ugc WHERE name IS NOT Null"
postgis_dsn = "dbname=%s host=%s" % (secret.dbname, secret.dbhost)
conn = psycopg2.connect( postgis_dsn )
curs = conn.cursor()
curs.execute( sql )

for row in curs.fetchall():
    name = (row[0]).replace("\x92"," ")
    ugc_dict[ row[1] ] = name

conn.commit()
del conn

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



def real_process(raw):
    # Pick product key and routing from first argument
    prod = TextProduct.TextProduct(raw)
    pil = prod.afos[:3]
    wfo = prod.source[1:]
    raw = raw.replace("'", "\\'")
    sqlraw = raw.replace("\015\015\012", "\n").replace("\000", "").strip()
    if (pil == "FTM"):
      sqlraw = re.sub("[^\n\ra-zA-Z0-9:\.,\s\$\*]", "", sqlraw)

    postgis_dsn = "dbname=%s host=%s" % (secret.dbname, secret.dbhost)
    conn = psycopg2.connect( postgis_dsn )
    curs = conn.cursor()
    sql = "INSERT into text_products(product) values ('%s')" % (sqlraw,)
    curs.execute( sql )
    oid = curs.lastrowid
    sql = "select id from text_products WHERE oid = %s" % (oid,)
    curs.execute( sql )
    id = curs.fetchone()[0]
    conn.commit()
    del conn

    if ( ["CAE", "AFD","FTM","AWU","HWO","NOW","HLS","PSH","NOW","PNS","RER","ADM"].__contains__(pil) ):
        mess = "%s: %s issues %s http://mesonet.agron.iastate.edu/p.php?id=%s" % \
          (wfo, wfo, pil, id)
        prodtxt = "(%s)" % (pil,)
        if (prodDefinitions.has_key(pil)):
            prodtxt = prodDefinitions[pil]
        htmlmess = "%s issues <a href=\"http://mesonet.agron.iastate.edu/p.php?id=%s\">%s</a> " % (wfo, id, prodtxt)
        if (not ["HWO","NOW"].__contains__(pil) and len(prod.segments) > 0 and len(prod.segments[0].headlines) > 0 and len(prod.segments[0].headlines[0]) < 200 ):
          htmlmess += "... %s ..." % (prod.segments[0].headlines[0],)

        jabber.sendMessage(mess, htmlmess)
        return


    # Now, lets look at segments ?
    for seg in prod.segments:
        if (len(seg.ugc) == 0):
            continue
        if (len(seg.hvtec) > 0 and ['FLW','FFA','FLS'].__contains__(pil)): # Handled by other app
            log.msg("HVTEC FOUND!, skipping")
            continue
        counties = countyText(seg.ugc)
        if (counties.strip() == ""):
            counties = "entire area"
        expire = ""
        if (seg.ugcExpire is not None):
            expire = "till "+ (seg.ugcExpire - mx.DateTime.RelativeDateTime(hours= offsets[prod.z] )).strftime("%-I:%M %p ")+ prod.z

        mess = "%s: %s issues %s for %s %s http://mesonet.agron.iastate.edu/p.php?id=%s" % \
          (wfo, wfo, pil, counties, expire, id)
        prodtxt = "(%s)" % (pil,)
        if (prodDefinitions.has_key(pil)):
            prodtxt = prodDefinitions[pil]
        htmlmess = "%s issues <a href=\"http://mesonet.agron.iastate.edu/p.php?id=%s\">%s</a> for %s %s" % (wfo, id, prodtxt, counties, expire)

        jabber.sendMessage(mess, htmlmess)

# PUBLIC ADVISORY NUMBER 10 FOR REMNANTS OF BARRY
# TROPICAL DEPRESSION BARRY ADVISORY NUMBER   5
# TROPICAL STORM BARRY INTERMEDIATE ADVISORY NUMBER   2A

    if (pil == "TCM" or pil == "TCP" or pil == "TCD"):
        tokens = re.findall("(.*) (DISCUSSION|INTERMEDIATE ADVISORY|FORECAST/ADVISORY|ADVISORY|MEMEME) NUMBER\s+([0-9]+)", raw.replace("PUBLIC ADVISORY", "ZZZ MEMEME") )
        mess = "%s: %s issues %s http://mesonet.agron.iastate.edu/p.php?id=%s" % \
          (wfo, wfo, pil, id)
        prodtxt = "(%s)" % (pil,)
        if (prodDefinitions.has_key(pil)):
            prodtxt = prodDefinitions[pil]
        htmlmess = "%s issues <a href=\"http://mesonet.agron.iastate.edu/p.php?id=%s\">%s</a> " % (wfo, id, prodtxt)

        jabber.sendMessage(mess, htmlmess)


    for key in routes.keys():
        if (re.match(key, prod.afos)):
            for wfo2 in routes[key]:
                mess = "%s: %s http://mesonet.agron.iastate.edu/p.php?id=%s" % \
                 (wfo2, prod.afos, id)
                htmlmess = "<a href=\"http://mesonet.agron.iastate.edu/p.php?id=%s\">%s</a>" % (id, prodtxt)
                if (len(tokens) > 0):
                    tt = tokens[0][0]
                    what = tokens[0][1]
                    tnum = tokens[0][2]
                    if (tokens[0][1] == "MEMEME"):
                        tokens2 = re.findall("(PUBLIC ADVISORY) NUMBER\s+([0-9]+) FOR (.*)", raw)
                        what = tokens2[0][0]
                        tt = tokens2[0][2]
                    mess = "%s: National Hurricane Center issues %s #%s for %s http://mesonet.agron.iastate.edu/p.php?id=%s" % (wfo2, what, tnum, tt, id)
                    htmlmess = "National Hurricane Center issues <a href=\"http://mesonet.agron.iastate.edu/p.php?id=%s\">%s #%s</a> for %s" % ( id, what, tnum, tt)
                #print htmlmess, mess
                jabber.sendMessage(mess, htmlmess)



myJid = jid.JID('iembot_ingest@%s/gp_%s' % (secret.chatserver, mx.DateTime.now().ticks() ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = JabberClient(myJid)

factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)

reactor.connectTCP(secret.chatserver,5222,factory)

ldm = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.run()
