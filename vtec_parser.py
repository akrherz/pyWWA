
from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.words.xish import domish
from twisted.internet import reactor
from twisted.python import log
import os
log.startLogging(open('/mesonet/data/logs/%s/vtec_parser.log' % (os.getenv("USER"),), 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

from pyIEM import  ldmbridge
from pyIEM.nws import TextProduct
import secret
from common import *
import pg
postgis = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser)

import re, mx.DateTime, traceback, StringIO
import smtplib
from email.MIMEText import MIMEText


offsets = {
 'EDT': 4,
 'CDT': 5, 'EST': 5,
 'MDT': 6, 'CST': 6,
 'PDT': 7, 'MST': 7,
 'ADT': 8, 'PST': 8,
 'HDT': 9, 'AST': 9,
           'HST':10,
}

class NoVTECFoundError(Exception):
    pass


# LDM Ingestor
class myProductIngestor(ldmbridge.LDMProductReceiver):

    def processData(self, buf):
        try:
            real_processor(buf)
        except:
            io = StringIO.StringIO()
            traceback.print_exc(file=io)
            log.msg( io.getvalue() )
            msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),buf.replace("\015\015\012", "\n") ))
            msg['subject'] = 'vtec_parse.py Traceback'
            msg['From'] = "ldm@mesonet.agron.iastate.edu"
            msg['To'] = "akrherz@iastate.edu"

            s = smtplib.SMTP()
            s.connect()
            s.sendmail(msg["From"], msg["To"], msg.as_string())
            s.close()

    def connectionLost(self,reason):
        log.msg("LDM Closed PIPE")
        

def real_processor(buf):
    """ The real data processor here """
    sqlraw = re.sub("'", "\\'", buf)
    sqlraw = sqlraw.replace("\015\015\012", "\n")
    text_product = TextProduct.TextProduct( buf )
    log.msg( str(text_product) )
    jabberMessages = []
    jabberMessagesHTML = []
    arSQL = []
    gmtnow = mx.DateTime.gmt()


    for seg in text_product.segments:
        if (len(seg.vtec) == 0 and len(seg.ugc) > 0 and not ["MWS","FLS","FLW"].__contains__(text_product.afos[:3]) ):
            jabberTxt = "%s: iembot encounted a VTEC product (%s) error. Missing or incomplete VTEC encoding in segment (%s)" % (text_product.source[1:], text_product.afos , text_product.sections[0].replace("\n", " ") )
            #jabber.sendMessage(jabberTxt, jabberTxt)
            raise NoVTECFoundError("No VTEC coding found for this segment")

        for v in seg.vtec:
            if (v.status == "X"):
                pass
            elif (v.status != "O"):
                continue
            u = seg.ugc
            h = seg.hvtec

            # Set up Jabber Dict for stuff to fill in
            jabberDict = {'wfo': v.office, 'product': v.productString(),
               'county': '', 'sts': ' ', 'ets': ' ', 
               'year': mx.DateTime.now().year, 'phenomena': v.phenomena,
               'eventid': v.ETN, 'significance': v.significance}
            if (v.beginTS != None and \
                v.beginTS > (gmtnow + mx.DateTime.RelativeDateTime(hours=+1))):
                efmt = "%b %d, %-I:%M %p "
                jabberDict['sts'] = " valid at %s%s " % ( (v.beginTS - mx.DateTime.RelativeDateTime(hours= offsets[text_product.z] )).strftime(efmt), text_product.z )
            else:
                efmt = "%-I:%M %p "
            if (v.endTS > (gmtnow + mx.DateTime.RelativeDateTime(days=+1))):
                efmt = "%b %d, %-I:%M %p "
            if (v.endTS == None):
                jabberDict['ets'] =  "further notice"
            else:
                jabberDict['ets'] =  "%s%s" % ((v.endTS - mx.DateTime.RelativeDateTime(hours= offsets[text_product.z] )).strftime(efmt), text_product.z)

            # We need to get the County Name
            countyState = {}
            affectedWFOS = {}
            for k in range(len(u)):
                cnty = u[k]
                stateAB = cnty[:2]
                if (not countyState.has_key(stateAB)):
                    countyState[stateAB] = []
                if (ugc2wfo.has_key(cnty)):
                    affectedWFOS[ ugc2wfo[cnty] ] = 1
                if (ugc_dict.has_key(cnty)):
                    name = ugc_dict[cnty]
                else:
                    log.msg("ERROR: Unknown ugc %s" % (cnty,))
                    name = "((%s))" % (cnty,)
                countyState[stateAB].append(name)
            # Test for affectedWFOS
            if (len(affectedWFOS) == 0):
                affectedWFOS[ v.office ] = 1

            for st in countyState.keys():
                countyState[stateAB].sort()
                jabberDict['county']+=" %s [%s] and" %(", ".join(countyState[st]), st)
            jabberDict['county'] = jabberDict['county'][:-4]
            # Check for Hydro-VTEC stuff
            if (len(h) > 0 and h[0].nwsli != "00000"):
                nwsli = h[0].nwsli
                rname = "((%s))" % (nwsli,)
                if (nwsli_dict.has_key(nwsli)):
                    rname = "the "+ nwsli_dict[nwsli]
                jabberDict['county'] = rname

            warning_tables = ["warnings", "warnings_%s" % (text_product.issueTime.year)]
        
            """
          NEW - New Warning
          EXB - Extended both in area and time (new area means new entry)
          EXA - Extended in area, which means new entry
           1. Insert any polygons
           2. Insert any counties
           3. Format Jabber message
            """
            if (v.action == "NEW" or v.action == "EXB" or v.action == "EXA"):
                if (v.endTS == None):
                    v.endTS = v.beginTS + mx.DateTime.RelativeDateTime(days=1)
                bts = v.beginTS
                if (v.action == "EXB" or v.action == "EXA"):
                    bts = text_product.issueTime
            # Insert Polygon
                for tbl in warning_tables:
                    if (seg.giswkt == None):
                        continue
                    fcster = re.sub("'", " ", text_product.fcster)
                    sql = "INSERT into %s (issue, expire, report, significance, \
   geom, phenomena, gtype, wfo, eventid, status, updated, fcster) VALUES \
   ('%s+00','%s+00','%s','%s','%s','%s','%s', '%s',%s,'%s', '%s+00', '%s')" \
   % (tbl, bts, v.endTS , sqlraw, v.significance, \
      seg.giswkt, v.phenomena, 'P', v.office, v.ETN, v.action, \
      text_product.issueTime, fcster )
                    arSQL.append( sql )
                # Insert Counties
                for k in range(len(u)):
                    cnty = u[k]
                    for tbl in warning_tables:
                        fcster = re.sub("'", " ", text_product.fcster)
  
                        sql = "INSERT into %s (issue,expire,report, geom, \
   phenomena, gtype, wfo, eventid, status,updated, fcster, ugc, significance) \
   VALUES('%s+00', '%s+00', '%s',\
       (select geom from nws_ugc WHERE ugc = '%s' LIMIT 1), \
   '%s', 'C', '%s',%s,'%s','%s+00', '%s', '%s','%s')" % \
   (tbl, bts, v.endTS, sqlraw, cnty, \
    v.phenomena, v.office, v.ETN, \
    v.action, text_product.issueTime, fcster, cnty, v.significance)
                        arSQL.append( sql )

                for w in affectedWFOS.keys():
                    jabberDict['w'] = w
                    jabberTxt = "%(w)s: %(wfo)s %(product)s%(sts)sfor %(county)s till %(ets)s http://mesonet.agron.iastate.edu/GIS/apps/rview/warnings_cat.phtml?year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;significance=%(significance)s" % jabberDict
                    jabberMessages.append(jabberTxt)
                    jabberTxt = "%(wfo)s <a href='http://mesonet.agron.iastate.edu/GIS/apps/rview/warnings_cat.phtml?year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;significance=%(significance)s'>%(product)s</a>%(sts)sfor %(county)s till %(ets)s" % jabberDict
                    jabberMessagesHTML.append(jabberTxt)

            elif (v.action == "CON"):
            # Lets find our county and update it with action
            # Not worry about polygon at the moment.
                for cnty in u:
                    for tbl in warning_tables:
                        sql = "UPDATE %s SET status = '%s', updated = '%s+00' \
                        WHERE ugc = '%s' and wfo = '%s' and eventid = %s and \
                      phenomena = '%s' and significance = '%s'" % \
                     (tbl, v.action, text_product.issueTime, cnty, v.office, v.ETN,\
                            v.phenomena, v.significance)
                        arSQL.append( sql )

                if (len(seg.vtec) == 1):
                    for tbl in warning_tables:
                        sql = "UPDATE %s SET status = '%s',  \
                     updated = '%s+00' WHERE gtype = 'P' and wfo = '%s' \
                     and eventid = %s and phenomena = '%s' \
                     and significance = '%s'" % (tbl, v.action, text_product.issueTime, v.office, v.ETN, v.phenomena, v.significance)
                        arSQL.append( sql )

                if (len(text_product.afos) > 3):
                    jabberTxt = "%(wfo)s: %(wfo)s %(product)s%(sts)sfor %(county)s till %(ets)s http://mesonet.agron.iastate.edu/GIS/apps/rview/warnings_cat.phtml?year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;significance=%(significance)s" % jabberDict
                    jabberMessages.append(jabberTxt)
                    jabberTxt = "%(wfo)s <a href='http://mesonet.agron.iastate.edu/GIS/apps/rview/warnings_cat.phtml?year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;significance=%(significance)s'>%(product)s</a>%(sts)sfor %(county)s till %(ets)s" % jabberDict
                    jabberMessagesHTML.append(jabberTxt)

            elif (v.action in ["CAN", "EXP", "UPG", "EXT"] ):
                end_ts = v.endTS
                if (v.endTS is None):  # 7 days into the future?
                    end_ts = text_product.issueTime + mx.DateTime.RelativeDateTime(days=7)
                if (v.action == "CAN" or v.action == "UPG"):
                    end_ts = text_product.issueTime
                issueSpecial = "issue"
                if (v.action == "EXT" and v.beginTS != None): # Extend both ways!
                    issueSpecial = "'%s+00'" % (v.beginTS,)
            # Lets cancel county
                for cnty in u:
                    for tbl in warning_tables:
                        sql = "UPDATE %s SET status = '%s', expire = '%s+00',\
                       updated = '%s+00', issue = %s WHERE ugc = '%s' and \
                     wfo = '%s' and eventid = %s and phenomena = '%s' \
                     and significance = '%s'" % \
                     (tbl, v.action, end_ts, text_product.issueTime, issueSpecial, \
                      cnty, v.office, v.ETN, v.phenomena, v.significance)
                        arSQL.append( sql )
            # If this is the only county, we can cancel the polygon too
                if (len(seg.vtec) == 1):
                    for tbl in warning_tables:
                        sql = "UPDATE %s SET status = '%s', expire = '%s+00', \
                     updated = '%s+00' WHERE gtype = 'P' and wfo = '%s' \
                     and eventid = %s and phenomena = '%s' \
                     and significance = '%s'" % (tbl, v.action, end_ts, text_product.issueTime, v.office, v.ETN, v.phenomena, v.significance)
                        arSQL.append( sql )
                jabberDict['action'] = "cancels"
                fmt = "%(w)s: %(wfo)s  %(product)s for %(county)s http://mesonet.agron.iastate.edu/GIS/apps/rview/warnings_cat.phtml?year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;significance=%(significance)s"
                htmlfmt = "%(wfo)s <a href='http://mesonet.agron.iastate.edu/GIS/apps/rview/warnings_cat.phtml?year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;significance=%(significance)s'>%(product)s</a> for %(county)s"
                if (v.action == "EXT" and v.beginTS != None):
                    jabberDict['sts'] = " valid at %s%s " % ( (v.beginTS - mx.DateTime.RelativeDateTime(hours= offsets[text_product.z] )).strftime(efmt), text_product.z )
                    fmt = "%(w)s: %(wfo)s  %(product)s for %(county)s%(sts)still %(ets)s http://mesonet.agron.iastate.edu/GIS/apps/rview/warnings_cat.phtml?year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;significance=%(significance)s"
                    htmlfmt = "%(wfo)s <a href='http://mesonet.agron.iastate.edu/GIS/apps/rview/warnings_cat.phtml?year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;significance=%(significance)s'>%(product)s</a> for %(county)s%(sts)still %(ets)s"
                elif (v.action == "EXT"):
                    fmt += " till %(ets)s"
                    htmlfmt += " till %(ets)s"
                if (v.action != 'UPG'):
                    for w in affectedWFOS.keys():
                        jabberDict['w'] = w
                        jabberTxt = fmt % jabberDict
                        jabberTxtHTML = htmlfmt % jabberDict
                        jabberMessages.append(jabberTxt)
                        jabberMessagesHTML.append(jabberTxtHTML)

            if (v.action != "NEW"):
                for tbl in warning_tables:
                    sql = "UPDATE %s SET svs = \
                  (CASE WHEN (svs IS NULL) THEN '__' ELSE svs END) \
                   || '%s' || '__' WHERE eventid = %s and wfo = '%s' \
                   and phenomena = '%s' and significance = '%s'" % \
                   (tbl, sqlraw, v.ETN, v.office, v.phenomena, v.significance)
                    arSQL.append( sql )

    for m in range(len(jabberMessages)):
        jabber.sendMessage(jabberMessages[m], jabberMessagesHTML[m])

    global postgis
    for sql in arSQL:
        postgis.query( sql )



""" Load me up with NWS dictionaries! """
ugc_dict = {}
ugc2wfo = {}
sql = "SELECT name, ugc, wfo from nws_ugc WHERE name IS NOT Null"
rs = postgis.query(sql).dictresult()
for i in range(len(rs)):
    name = (rs[i]["name"]).replace("\x92"," ")
    ugc_dict[ rs[i]['ugc'] ] = name
    ugc2wfo[ rs[i]['ugc'] ] = rs[i]['wfo'][:3]

""" Load up H-VTEC NWSLI reference """
nwsli_dict = {}
sql = "SELECT nwsli, river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname from hvtec_nwsli"
rs = postgis.query(sql).dictresult()
for i in range(len(rs)):
    nwsli_dict[ rs[i]['nwsli'] ] = rs[i]['rname']

myJid = jid.JID('iembot_ingest@%s/vtecparser_%s' % (secret.chatserver, mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
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

