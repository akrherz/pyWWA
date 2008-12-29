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
""" generic product ingestor """

__revision__ = '$Id: generic_product.py 3802 2008-07-29 19:55:56Z akrherz $'

# Twisted Python imports
from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.internet import reactor
from twisted.python import log
from twisted.enterprise import adbapi
from twisted.mail import smtp

# Standard Python modules
import os, re, traceback, StringIO
from email.MIMEText import MIMEText

# Python 3rd Party Add-Ons
import mx.DateTime, pg, psycopg2

# pyWWA stuff
from support import ldmbridge, TextProduct, reference
import secret
import common

log.startLogging(open('logs/gp.log', 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"


DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, host=secret.dbhost, password=secret.dbpass)

# These are the offices which get all hurricane stuff
gulfwfo = ['KEY', 'SJU', 'TBW', 'TAE', 'JAX', 'MOB', 'HGX', 'CRP', 'BMX',
    'EWX', 'FWD', 'SHV', 'JAN', 'LIX', 'LCH', 'FFC', 'MFL', 'MLB', 'CHS',
    'CAE', 'RAH', 'ILM', 'AKQ']
# These offices get all SPC stuff...
spcwfo = ['RNK',]

routes = {'TCPAT[0-9]': gulfwfo,
          'TCMAT[0-9]': gulfwfo,
          'TCDAT[0-9]': gulfwfo,
          'TCEAT[0-9]': gulfwfo,
          'TCUAT[0-9]': gulfwfo,
          'DSAAT': gulfwfo,
          'SWODY[1-2]': spcwfo,}

SIMPLE_PRODUCTS = ["TCE", "DSA", "AQA", "DGT", "FWF", "RTP", "HPA", "CWF", 
            "SRF", "SFT", "PFM", "ZFP", "CAE", "AFD", "FTM", "AWU", "HWO",
            "NOW", "PSH", "NOW", "PNS", "RER", "ADM", "TCU", "RVF"]

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
    msg['subject'] = 'generic_product.py Traceback'
    msg['From'] = secret.parser_user
    msg['To'] = 'akrherz@iastate.edu'
    smtp.sendmail("mailhub.iastate.edu", msg["From"], msg["To"], msg)


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


prodDefinitions = {
    'TCU': 'Tropical Cyclone Update (TCU)',
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
    'ZFP': 'Zone Forecast Package (ZFP)',
    'PFM': 'Point Forecast Matrices (PFM)',
    'SFT': 'State Forecast Tabular Product (SFT)',
    'SRF': 'Surf Zone Forecast (SRF)',
    'CWF': 'Coastal Waters Forecast (CWF)',
    'RVS': 'Hydrologic Statement (RVS)',
    'HPA': 'High Pollution Advisory (HPA)',
    'RTP': 'Regional Temperature and Precipitation (RTP)',
    'FWF': 'Fire Weather Planning Forecast (FWF)',
    'DGT': 'Drought Information (DGT)',
    'MWS': 'Marine Weather Statement (MWS)',
    'AQA': 'Air Quality Alert (AQA)',
    'DSA': 'Tropical Disturbance Statement (DSA)',
    'TCE': 'Tropical Cyclone Position Estimate (TCE)',
    'RVF': 'River Forecast (RVF)',
}

ugc_dict = {}
sql = "SELECT name, ugc from nws_ugc WHERE name IS NOT Null"
postgis_dsn = "dbname=%s host=%s password=%s" % (secret.dbname, secret.dbhost, secret.dbpass)
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

def centertext(txt):
    if (txt == "NHC"): return "National Hurricane Center"
    if (txt == "WNH"): return "Hydrometeorological Prediction Center"
    return "%s" % (txt,)


def real_process(raw):
    """ The real processor of the raw data, fun! """

    prod = TextProduct.TextProduct(raw)
    pil = prod.afos[:3]
    wfo = prod.get_iembot_source()

    raw = raw.replace("'", "\\'")
    sqlraw = raw.replace("\015\015\012", "\n").replace("\000", "").strip()

    # FTM sometimes have 'garbage' characters included, get em out
    if (pil == "FTM"):
        sqlraw = re.sub("[^\n\ra-zA-Z0-9:\.,\s\$\*]", "", sqlraw)

    # TODO: If there are no segments, send an alert to room and daryl!

    # Always insert the product into the text archive database
    product_id = prod.get_product_id()
    sql = "INSERT into text_products(product, product_id) \
      values ('%s','%s')" % (sqlraw, product_id)
    if (len(prod.segments) > 0 and prod.segments[0].giswkt):
        sql = "INSERT into text_products(product, product_id, geom) \
      values ('%s','%s','%s')" % (sqlraw, product_id, prod.segments[0].giswkt)
    DBPOOL.runOperation(sql).addErrback( email_error, sql)
    myurl = "%s?pid=%s" % (secret.PROD_URL, product_id)

    # Just send with optional headline to rooms...
    if ( SIMPLE_PRODUCTS.__contains__(pil) ):
        prodtxt = "(%s)" % (pil,)
        if (prodDefinitions.has_key(pil)):
            prodtxt = prodDefinitions[pil]

        mess = "%s: %s issues %s %s" % \
          (wfo, wfo, prodtxt, myurl)
        htmlmess = "%s issues <a href=\"%s\">%s</a> " % (centertext(wfo), myurl, prodtxt)
        if (not ["HWO","NOW","ZFP"].__contains__(pil) and 
         len(prod.segments) > 0 and 
         len(prod.segments[0].headlines) > 0 and 
         len(prod.segments[0].headlines[0]) < 200 ):
            htmlmess += "... %s ..." % (prod.segments[0].headlines[0],)

        jabber.sendMessage(mess, htmlmess)

        # Also send message to any 'subscribing WFO chatrooms'
        for key in routes.keys():
            if (re.match(key, prod.afos)):
                for wfo2 in routes[key]:
                    mess = "%s: %s issues %s %s" % \
                       (wfo2, wfo, prodtxt, myurl)
                    jabber.sendMessage(mess, htmlmess)
        # We are done for this product
        return


    # Now, lets look at segments ?
    for seg in prod.segments:
        # The segment needs to have ugc codes
        if (len(seg.ugc) == 0):
            continue
        # If the product has VTEC, it is handled by the vtec ingestor
        if (len(seg.vtec) > 0 and ['MWS','HLS'].__contains__(pil)):
            log.msg("VTEC FOUND!, skipping")
            continue

        # If the product has HVTEC, it is handled by other ingestor too
        if (len(seg.hvtec) > 0 and ['FLW','FFA','FLS'].__contains__(pil)):
            log.msg("HVTEC FOUND!, skipping")
            continue

        counties = countyText(seg.ugc)
        if (counties.strip() == ""):
            counties = "entire area"
        expire = ""
        if (seg.ugcExpire is not None):
            expire = "till "+ (seg.ugcExpire - mx.DateTime.RelativeDateTime(hours= reference.offsets[prod.z] )).strftime("%-I:%M %p ")+ prod.z

        prodtxt = "(%s)" % (pil,)
        if (prodDefinitions.has_key(pil)):
            prodtxt = prodDefinitions[pil]
        mess = "%s: %s issues %s for %s %s %s" % \
          (wfo, wfo, prodtxt, counties, expire, myurl)
        htmlmess = "%s issues <a href=\"%s\">%s</a> for %s %s" % (wfo, myurl, prodtxt, counties, expire)

        jabber.sendMessage(mess, htmlmess)

# PUBLIC ADVISORY NUMBER 10 FOR REMNANTS OF BARRY
# TROPICAL DEPRESSION BARRY ADVISORY NUMBER   5
# TROPICAL STORM BARRY INTERMEDIATE ADVISORY NUMBER   2A

    if (pil == "TCM" or pil == "TCP" or pil == "TCD"):
        mess = "%s: %s issues %s %s" % (wfo, wfo, pil, myurl)
        prodtxt = "(%s)" % (pil,)
        if (prodDefinitions.has_key(pil)):
            prodtxt = prodDefinitions[pil]
        htmlmess = "%s issues <a href=\"%s\">%s</a> " % (wfo, myurl, prodtxt)
        jabber.sendMessage(mess, htmlmess)



    for key in routes.keys():
        if (re.match(key, prod.afos)):
            for wfo2 in routes[key]:
                mess = "%s: %s %s" % \
                 (wfo2, prod.afos, myurl)
                htmlmess = "<a href=\"%s\">%s</a>" % (myurl, prodtxt)
                tokens = re.findall("(.*) (DISCUSSION|INTERMEDIATE ADVISORY|FORECAST/ADVISORY|ADVISORY|MEMEME) NUMBER\s+([0-9]+)", raw.replace("PUBLIC ADVISORY", "ZZZ MEMEME") )
                if (len(tokens) > 0):
                    tt = tokens[0][0]
                    what = tokens[0][1]
                    tnum = tokens[0][2]
                    if (tokens[0][1] == "MEMEME"):
                        tokens2 = re.findall("(PUBLIC ADVISORY) NUMBER\s+([0-9]+) FOR (.*)", raw)
                        what = tokens2[0][0]
                        tt = tokens2[0][2]
                    mess = "%s: %s issues %s #%s for %s %s" % (wfo2, centertext(wfo), what, tnum, tt, myurl)
                    htmlmess = "%s issues <a href=\"%s\">%s #%s</a> for %s" % ( centertext(wfo), myurl, what, tnum, tt)
                #print htmlmess, mess
                jabber.sendMessage(mess, htmlmess)



myJid = jid.JID('%s@%s/gp_%s' % \
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
