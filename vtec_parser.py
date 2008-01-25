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
""" VTEC product ingestor """

__revision__ = '$Id$'

# Twisted Python imports
from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.internet import reactor
from twisted.python import log
from twisted.enterprise import adbapi

# Standard Python modules
import os, re, traceback, StringIO, smtplib
from email.MIMEText import MIMEText

# Python 3rd Party Add-Ons
import mx.DateTime, pg

# pyWWA stuff
from support import ldmbridge, TextProduct
import secret
import common

log.startLogging(open('/mesonet/data/logs/%s/vtec_parser.log' \
    % (os.getenv("USER"),), 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser)
DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, host=secret.dbhost)
URLBASE = 'http://mesonet.agron.iastate.edu/GIS/apps/rview/warnings_cat.phtml'

OFFSETS = {
 'EDT': 4,
 'CDT': 5, 'EST': 5,
 'MDT': 6, 'CST': 6,
 'PDT': 7, 'MST': 7,
 'ADT': 8, 'PST': 8,
 'HDT': 9, 'AST': 9,
           'HST':10,
}

class NoVTECFoundError(Exception):
    """ Exception place holder """
    pass

class ProcessingError(Exception):
    """ Exception place holder """
    pass


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def process_data(self, buf):
        """ Process the product """
        try:
            text_product = TextProduct.TextProduct( buf )
            log.msg( str(text_product) )
            for j in range(len(text_product.segments)):
                segment_processor(text_product, j)

        except:
            sio = StringIO.StringIO()
            traceback.print_exc(file=sio)
            log.msg( sio.getvalue() )
            msg = MIMEText("%s\n\n>RAW DATA\n\n%s" % (sio.getvalue(),
                   buf.replace("\015\015\012", "\n") ) )
            msg['subject'] = 'vtec_parse.py Traceback'
            msg['From'] = "ldm@mesonet.agron.iastate.edu"
            msg['To'] = "akrherz@iastate.edu"

            smtp = smtplib.SMTP()
            smtp.connect()
            smtp.sendmail(msg["From"], msg["To"], msg.as_string())
            smtp.close()

    def connectionLost(self, reason):
        """ I lost my connection, should I do anything else? """
        log.msg("LDM Closed PIPE")
        

def segment_processor(text_product, i):
    """ The real data processor here """
    gmtnow = mx.DateTime.gmt()
    local_offset = mx.DateTime.RelativeDateTime(hours= OFFSETS[text_product.z])
    seg = text_product.segments[i]

    # A segment must have UGC
    if (len(seg.ugc) == 0):
        return
    
    # Firstly, certain products may not contain VTEC, silently return
    if (["MWS","FLS","FLW"].__contains__(text_product.afos[:3])):
        if (len(seg.vtec) == 0):
            log.msg("I FOUND NO VTEC, BUT THAT IS OK")
            return

    # If we found no VTEC and it has UGC, we complain about this
    if (len(seg.vtec) == 0):
        msg = "%s: iembot encounted a VTEC product (%s) error. \
Missing or incomplete VTEC encoding in segment (%s)" % \
            (text_product.source[1:], text_product.afos ,
             text_product.sections[0].replace("\n", " ") )
        jabber.sendMessage(msg)
        raise NoVTECFoundError("No VTEC coding found for this segment")


    # A segment could have multiple vtec codes :)
    for vtec in seg.vtec:
        if (vtec.status == "T"):
            return
        ugc = seg.ugc
        hvtec = seg.hvtec

        # Set up Jabber Dict for stuff to fill in
        jmsg_dict = {'wfo': vtec.office, 'product': vtec.productString(),
             'county': '', 'sts': ' ', 'ets': ' ', 
             'year': mx.DateTime.now().year, 'phenomena': vtec.phenomena,
             'eventid': vtec.ETN, 'significance': vtec.significance,
             'urlbase': URLBASE}

        if (vtec.beginTS != None and \
            vtec.beginTS > (gmtnow + mx.DateTime.RelativeDateTime(hours=+1))):
            efmt = "%b %d, %-I:%M %p "
            jmsg_dict['sts'] = " valid at %s%s " % \
                 ((vtec.beginTS - local_offset).strftime(efmt), text_product.z)
        else:
            efmt = "%-I:%M %p "

        if (vtec.endTS > (gmtnow + mx.DateTime.RelativeDateTime(days=+1))):
            efmt = "%b %d, %-I:%M %p "
        if (vtec.endTS == None):
            jmsg_dict['ets'] =  "further notice"
        else:
            jmsg_dict['ets'] =  "%s%s" % \
                ((vtec.endTS - local_offset).strftime(efmt), text_product.z)

        # We need to get the County Name
        countyState = {}
        affectedWFOS = {}
        for k in range(len(ugc)):
            cnty = ugc[k]
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
            affectedWFOS[ vtec.office ] = 1

        for st in countyState.keys():
            countyState[stateAB].sort()
            jmsg_dict['county']+=" %s [%s] and" % \
                  (", ".join(countyState[st]), st)

        jmsg_dict['county'] = jmsg_dict['county'][:-4]
        # Check for Hydro-VTEC stuff
        if (len(hvtec) > 0 and hvtec[0].nwsli != "00000"):
            nwsli = hvtec[0].nwsli
            rname = "((%s))" % (nwsli,)
            if (nwsli_dict.has_key(nwsli)):
                rname = "the "+ nwsli_dict[nwsli]
            jmsg_dict['county'] = rname

        # Figure out which tables we need to update, have to do 2 :)
        warning_tables = ["warnings", "warnings_%s" % 
                                          (text_product.issueTime.year,)]
        if (vtec.beginTS is not None):
            warning_tables = ['warnings', "warnings_%s" % 
                                          (vtec.beginTS.year,)]
        
        #  NEW - New Warning
        #  EXB - Extended both in area and time (new area means new entry)
        #  EXA - Extended in area, which means new entry
        #   1. Insert any polygons
        #   2. Insert any counties
        #   3. Format Jabber message
        if (vtec.action == "NEW" or vtec.action == "EXB" or \
            vtec.action == "EXA"):
            if (vtec.beginTS is None):
               vtec.beginTS = text_product.issueTime
            if (vtec.endTS is None):
                vtec.endTS = vtec.beginTS + mx.DateTime.RelativeDateTime(days=1)
            bts = vtec.beginTS
            if (vtec.action == "EXB" or vtec.action == "EXA"):
                bts = text_product.issueTime
        # Insert Polygon
            for tbl in warning_tables:
                if (seg.giswkt == None):
                    continue
                fcster = re.sub("'", " ", text_product.fcster)
                sql = "INSERT into %s (issue, expire, report, \
significance, geom, phenomena, gtype, wfo, eventid, status, updated, \
fcster) VALUES ('%s+00','%s+00','%s','%s','%s','%s','%s', '%s',%s,'%s', \
'%s+00', '%s')" \
   % (tbl, bts, vtec.endTS , text_product.sqlraw(), vtec.significance, \
      seg.giswkt, vtec.phenomena, 'P', vtec.office, vtec.ETN, vtec.action, \
      text_product.issueTime, fcster )
                DBPOOL.runOperation( sql )

            # Insert Counties
            for k in range(len(ugc)):
                cnty = ugc[k]
                for tbl in warning_tables:
                    fcster = re.sub("'", " ", text_product.fcster)
  
                    sql = "INSERT into %s (issue,expire,report, geom, \
phenomena, gtype, wfo, eventid, status,updated, fcster, ugc, significance) \
VALUES('%s+00', '%s+00', '%s',\
(select geom from nws_ugc WHERE ugc = '%s' LIMIT 1), \
'%s', 'C', '%s',%s,'%s','%s+00', '%s', '%s','%s')" % \
(tbl, bts, vtec.endTS, text_product.sqlraw(), cnty, \
vtec.phenomena, vtec.office, vtec.ETN, \
vtec.action, text_product.issueTime, fcster, cnty, vtec.significance)
                    DBPOOL.runOperation( sql )
            for w in affectedWFOS.keys():
                jmsg_dict['w'] = w
                jabberTxt = "%(w)s: %(wfo)s %(product)s%(sts)sfor \
%(county)s till %(ets)s %(urlbase)s?year=%(year)s&amp;wfo=%(wfo)s&amp;\
phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;\
significance=%(significance)s" % jmsg_dict
                jabberHTML = "%(wfo)s <a href='%(urlbase)s?year=%(year)s\
&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;\
significance=%(significance)s'>%(product)s</a>%(sts)sfor %(county)s \
till %(ets)s" % jmsg_dict
                jabber.sendMessage(jabberTxt, jabberHTML)

        elif (vtec.action == "CON"):
        # Lets find our county and update it with action
        # Not worry about polygon at the moment.
            for cnty in ugc:
                for tbl in warning_tables:
                    sql = "UPDATE %s SET status = '%s', updated = '%s+00' \
                        WHERE ugc = '%s' and wfo = '%s' and eventid = %s and \
                      phenomena = '%s' and significance = '%s'" % \
              (tbl, vtec.action, text_product.issueTime, cnty, \
               vtec.office, vtec.ETN,\
                            vtec.phenomena, vtec.significance)
                    DBPOOL.runOperation( sql )

            if (len(seg.vtec) == 1):
                for tbl in warning_tables:
                    sql = "UPDATE %s SET status = '%s',  \
                     updated = '%s+00' WHERE gtype = 'P' and wfo = '%s' \
                     and eventid = %s and phenomena = '%s' \
                     and significance = '%s'" % (tbl, vtec.action, \
       text_product.issueTime, vtec.office, \
       vtec.ETN, vtec.phenomena, vtec.significance)
                    DBPOOL.runOperation( sql )

            jabberTxt = "%(wfo)s: %(wfo)s %(product)s%(sts)sfor \
%(county)s till %(ets)s %(urlbase)s?year=%(year)s&amp;wfo=%(wfo)s&amp;\
phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;\
significance=%(significance)s" % jmsg_dict
            jabberHTML = "%(wfo)s <a href='%(urlbase)s?year=%(year)s\
&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;\
significance=%(significance)s'>%(product)s</a>%(sts)sfor %(county)s \
till %(ets)s" % jmsg_dict
            jabber.sendMessage(jabberTxt, jabberHTML)
#--

        elif (vtec.action in ["CAN", "EXP", "UPG", "EXT"] ):
            end_ts = vtec.endTS
            if (vtec.endTS is None):  # 7 days into the future?
                end_ts = text_product.issueTime + \
                         mx.DateTime.RelativeDateTime(days=7)
            if (vtec.action == "CAN" or vtec.action == "UPG"):
                end_ts = text_product.issueTime
            issueSpecial = "issue"
            if (vtec.action == "EXT" and vtec.beginTS != None): 
                issueSpecial = "'%s+00'" % (vtec.beginTS,)
        # Lets cancel county
            for cnty in ugc:
                for tbl in warning_tables:
                    sql = "UPDATE %s SET status = '%s', expire = '%s+00',\
                       updated = '%s+00', issue = %s WHERE ugc = '%s' and \
                     wfo = '%s' and eventid = %s and phenomena = '%s' \
                     and significance = '%s'" % \
             (tbl, vtec.action, end_ts, text_product.issueTime, issueSpecial, \
              cnty, vtec.office, vtec.ETN, \
              vtec.phenomena, vtec.significance)
                    DBPOOL.runOperation( sql )

            # If this is the only county, we can cancel the polygon too
            if (len(seg.vtec) == 1):
                for tbl in warning_tables:
                    sql = "UPDATE %s SET status = '%s', expire = '%s+00', \
                     updated = '%s+00' WHERE gtype = 'P' and wfo = '%s' \
                     and eventid = %s and phenomena = '%s' \
                     and significance = '%s'" % (tbl, vtec.action, end_ts, \
      text_product.issueTime, vtec.office, vtec.ETN, \
      vtec.phenomena, vtec.significance)
                    DBPOOL.runOperation( sql )

            jmsg_dict['action'] = "cancels"
            fmt = "%(w)s: %(wfo)s  %(product)s for %(county)s \
%(urlbase)s?year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;\
eventid=%(eventid)s&amp;significance=%(significance)s"
            htmlfmt = "%(wfo)s <a href='%(urlbase)s?year=%(year)s&amp;\
wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;\
significance=%(significance)s'>%(product)s</a> for %(county)s"
            if (vtec.action == "EXT" and vtec.beginTS != None):
                jmsg_dict['sts'] = " valid at %s%s " % ( \
                (vtec.beginTS - local_offset).strftime(efmt), text_product.z )
                fmt = "%(w)s: %(wfo)s  %(product)s for %(county)s\
%(sts)still %(ets)s %(urlbase)s?year=%(year)s&amp;wfo=%(wfo)s&amp;\
phenomena=%(phenomena)s&amp;eventid=%(eventid)s&amp;\
significance=%(significance)s"
                htmlfmt = "%(wfo)s <a href='%(urlbase)s?\
year=%(year)s&amp;wfo=%(wfo)s&amp;phenomena=%(phenomena)s&amp;\
eventid=%(eventid)s&amp;significance=%(significance)s'>%(product)s</a>\
 for %(county)s%(sts)still %(ets)s"
            elif (vtec.action == "EXT"):
                fmt += " till %(ets)s"
                htmlfmt += " till %(ets)s"
            if (vtec.action != 'UPG'):
                for w in affectedWFOS.keys():
                    jmsg_dict['w'] = w
                    jabber.sendMessage(fmt % jmsg_dict, htmlfmt % jmsg_dict)

        if (vtec.action != "NEW"):
            ugc_limiter = ""
            for cnty in ugc:
                ugc_limiter += "'%s'," % (cnty,)

            for tbl in warning_tables:
                sql = "UPDATE %s SET svs = \
                  (CASE WHEN (svs IS NULL) THEN '__' ELSE svs END) \
                   || '%s' || '__' WHERE eventid = %s and wfo = '%s' \
                   and phenomena = '%s' and significance = '%s' \
                   and ugc IN (%s)" % \
                   (tbl, text_product.sqlraw(), vtec.ETN, vtec.office, \
                    vtec.phenomena, vtec.significance, ugc_limiter[:-1] )
                log.msg("Updating SVS For:"+ ugc_limiter[:-1] )
                DBPOOL.runOperation( sql )

    # Update polygon if necessary
    if (vtec.action != "NEW" and seg.giswkt is not None):
        for tbl in warning_tables:
            sql = "UPDATE %s SET svs = \
              (CASE WHEN (svs IS NULL) THEN '__' ELSE svs END) \
               || '%s' || '__' WHERE eventid = %s and wfo = '%s' \
               and phenomena = '%s' and significance = '%s' \
               and gtype = 'P'" %\
               (tbl, text_product.sqlraw(), vtec.ETN, vtec.office, \
                vtec.phenomena, vtec.significance )
            log.msg("Updating SVS For Polygon")
            DBPOOL.runOperation( sql )
   

""" Load me up with NWS dictionaries! """
ugc_dict = {}
ugc2wfo = {}
sql = "SELECT name, ugc, wfo from nws_ugc WHERE name IS NOT Null"
rs = POSTGIS.query(sql).dictresult()
for i in range(len(rs)):
    ugc_dict[ rs[i]['ugc'] ] = (rs[i]["name"]).replace("\x92"," ")
    ugc2wfo[ rs[i]['ugc'] ] = rs[i]['wfo'][:3]

""" Load up H-VTEC NWSLI reference """
nwsli_dict = {}
sql = "SELECT nwsli, \
 river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname \
 from hvtec_nwsli"
rs = POSTGIS.query(sql).dictresult()
for i in range(len(rs)):
    nwsli_dict[ rs[i]['nwsli'] ] = rs[i]['rname']

myJid = jid.JID('iembot_ingest@%s/vtecparser_%s' % \
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

