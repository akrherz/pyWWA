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

__revision__ = '$Id: :$'

# Twisted Python imports
from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.internet import reactor
from twisted.python import log
from twisted.python import logfile
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

log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('generic_product.log', 'logs/') )

POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser, 
                     passwd=secret.dbpass)
DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, 
                               host=secret.dbhost, password=secret.dbpass,
                               cp_reconnect=True)

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
            "NOW", "PSH", "NOW", "PNS", "RER", "ADM", "TCU", "RVA", "EQR",
            "OEP", "SIG", "VAA", "RVF", "PWO", "TWO"]

EMAILS = 10

AHPS_TEMPLATE = {
  'CR': 'http://www.crh.noaa.gov/ahps2/hydrograph.php?wfo=%s&amp;gage=%s&amp;view=1,1,1,1,1,1,1,1',
  'SR': 'http://ahps.srh.noaa.gov/ahps2/hydrograph.php?wfo=%s&amp;gage=%s&amp;view=1,1,1,1,1,1,1,1',
  'ER': 'http://newweb.erh.noaa.gov/ahps2/hydrograph.php?wfo=%s&amp;gage=%s&amp;view=1,1,1,1,1,1,1,1',
  'WR': 'http://ahps2.wrh.noaa.gov/ahps2/hydrograph.php?wfo=%s&amp;gage=%s&amp;view=1,1,1,1,1,1,1,1',
  'PR': 'http://aprfc.arh.noaa.gov/ahps2/hydrograph.php?wfo=%s&amp;gage=%s&amp;view=1,1,1,1,1,1,1,1',
}



class myProductIngestor(ldmbridge.LDMProductReceiver):

    def process_data(self, buf):
        try:
            real_process(buf)
        except Exception, myexp:
            common.email_error(myexp, buf)

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


prodDefinitions = {
    'TWO': 'Tropical Weather Outlook (TWO)',
    'PWO': 'Public Severe Weather Outlook (PWO)',
    'TCM': 'Tropical Storm Forecast (TCM)',
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
    'RVA': 'Hydrologic Summary (RVA)',
    'EQR': 'Earthquake Report (EQR)',
    'OEP': 'TAF Collaboration Product (OEP)',
    'SIG': 'Convective Sigment (SIG)',
    'VAA': 'Volcanic Ash Advisory (VAA)',
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
    if (txt == "SPC"): return "Storm Prediction Center"
    if (txt == "WNS"): return "Storm Prediction Center"
    if (txt == "NHC"): return "National Hurricane Center"
    if (txt == "WNH"): return "Hydrometeorological Prediction Center"
    return "%s" % (txt,)

def snowfall_pns(prod):
    """
    Process Snowfall PNS, from ARX at the moment
    """
    if prod.raw.find("...STORM TOTAL SNOWFALL AMOUNTS...") == -1:
        return
    DBPOOL.runOperation("DELETE from snowfall_pns where source = '%s'" % (
                                                                prod.afos,))
    for line in prod.raw.split("\n"):
        tokens = line.split()
        if len(tokens) < 6:
            continue
        if tokens[-3] not in ['AM','PM']:
            continue
        # we have a good ob?
        lon = 0 - float(tokens[-1].replace("W", ""))
        lat = float(tokens[-2].replace("N", ""))
        snowfall = tokens[-5]
        DBPOOL.runOperation("""INSERT into snowfall_pns(valid, source, snow, geom)
        VALUES (now(), '%s', %s, 'SRID=4326;POINT(%s %s)')""" % (prod.afos,
                            snowfall, lon, lat)).addErrback(common.email_error)
        

def real_process(raw):
    """ The real processor of the raw data, fun! """

    prod = TextProduct.TextProduct(raw)
    pil = prod.afos[:3]
    wfo = prod.get_iembot_source()
    # sigh, can't use originating center for the route
    if (pil == "OEP"):
        wfo = prod.afos[3:]

    #raw = raw.replace("'", "\\'")
    sqlraw = raw.replace("\015\015\012", "\n").replace("\000", "").strip()

    # FTM sometimes have 'garbage' characters included, get em out
    #if (pil == "FTM"):
    #    sqlraw = re.sub("[^\n\ra-zA-Z0-9:\.,\s\$\*]", "", sqlraw)

    # TODO: If there are no segments, send an alert to room and daryl!

    # Always insert the product into the text archive database
    product_id = prod.get_product_id()
    sql = """INSERT into text_products(product, product_id) values (%s,%s)"""
    myargs = (sqlraw, product_id)
    if (len(prod.segments) > 0 and prod.segments[0].giswkt):
        sql = """INSERT into text_products(product, product_id, geom) values (%s,%s,%s)""" 
        myargs = (sqlraw, product_id, prod.segments[0].giswkt)
    deffer = DBPOOL.runOperation(sql, myargs)
    deffer.addErrback( common.email_error, sqlraw)
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

        channels = [wfo,]
        # Also send message to any 'subscribing WFO chatrooms'
        for key in routes.keys():
            if (re.match(key, prod.afos)):
                for wfo2 in routes[key]:
                    mess = "%s: %s issues %s %s" % \
                       (wfo2, wfo, prodtxt, myurl)
                    jabber.sendMessage(mess, htmlmess)
                    channels.append( wfo2 )

        twt = prodtxt
        url = myurl
        common.tweet(channels, twt, url)
        if prod.afos == "PNSARX":
            snowfall_pns(prod)
        # We are done for this product
        return


    # Now, lets look at segments ?
    if (pil == "RVF"):
        for seg in prod.segments:
            tokens = re.findall("\.E ([A-Z0-9]{5}) ", seg.raw)
            if (len(tokens) == 0):
                print 'Whoa, did not find NWSLI?', seg
                return
            hsas = re.findall("HSA:([A-Z]{3}) ", seg.raw)
            prodtxt = prodDefinitions[pil]
            mess = "%s: %s issues %s" % \
              (wfo, wfo, prodtxt)
            htmlmess = "%s issues <a href=\"%s\">%s</a> for " \
               % (wfo, myurl, prodtxt)
            usednwsli = {}
            hsa_cnt = -1
            rivers = {}
            for nwsli in tokens:
                if usednwsli.has_key(nwsli):
                    continue
                usednwsli[nwsli] = 1
                hsa_cnt += 1
                if (nwsli_dict.has_key(nwsli)):
                    rname = nwsli_dict[nwsli]['rname']
                    r = nwsli_dict[nwsli]['river']
                else:
                    rname = "((%s))" % (nwsli,)
                    r = "Unknown River"
                if not rivers.has_key(r):
                    rivers[r] = "<br/>%s " % (r,)
                if len(hsas) > hsa_cnt and \
                   reference.wfo_dict.has_key( hsas[hsa_cnt] ):
                    uri = AHPS_TEMPLATE[ reference.wfo_dict[hsas[hsa_cnt]]['region'] ] %\
                        (hsas[hsa_cnt].lower(), nwsli.lower() ) 
                    rivers[r] += "<a href=\"%s\">%s</a> (%s), " % (uri, rname, nwsli)
                else:
                    rivers[r] += "%s (%s), " % (rname, nwsli)
            for r in rivers.keys():
                htmlmess += " %s" % (rivers[r][:-2],)
            jabber.sendMessage(mess[:-1] +" "+ myurl, htmlmess[:-1])
            continue

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
        
        common.tweet([wfo], prodtxt, myurl)


    for key in routes.keys():
        if (re.match(key, prod.afos)):
            channels = []
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
                channels.append( wfo2 )
            twt = "%s issues %s %s for %s" % (centertext(wfo), what, tnum, tt)
            common.tweet(channels, twt, myurl)


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
        twt = "%s for %s %s" % (prodtxt, counties, expire)
        common.tweet([wfo,], twt, myurl)

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
        common.tweet([wfo,], prodtxt, myurl)



    for key in routes.keys():
        if (re.match(key, prod.afos)):
            channels = []
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
                channels.append( wfo2 )
            twt = "%s issues %s %s for %s" % (centertext(wfo), what, tnum, tt)
            common.tweet(channels, twt, myurl)

""" Load up H-VTEC NWSLI reference """
nwsli_dict = {}
sql = "SELECT nwsli, river_name as r, \
 proximity || ' ' || name || ' ['||state||']' as rname \
 from hvtec_nwsli"
rs = POSTGIS.query(sql).dictresult()
for i in range(len(rs)):
    nwsli_dict[ rs[i]['nwsli'] ] = {
      'rname': (rs[i]['rname']).replace("&"," and "), 
      'river': (rs[i]['r']).replace("&"," and ") }



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
