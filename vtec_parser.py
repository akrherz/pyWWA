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
from twisted.mail import smtp

# Standard Python modules
import os, re, traceback, StringIO, smtplib
from email.MIMEText import MIMEText

# Python 3rd Party Add-Ons
import mx.DateTime, pg

# pyWWA stuff
from support import ldmbridge, TextProduct, reference
import secret
import common
import StringIO
import socket
import sys

log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(open('logs/vtec_parser.log','a'))


POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser, passwd=secret.dbpass)
DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, host=secret.dbhost, password=secret.dbpass)
TIMEFORMAT="%Y-%m-%d %H:%M+00"

class NoVTECFoundError(Exception):
    """ Exception place holder """
    pass

class ProcessingError(Exception):
    """ Exception place holder """
    pass


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(7, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


    def process_data(self, buf):
        """ Process the product """
        try:
            # Make sure we have a trailing $$
            #if buf.find("$$") == -1:
            #    buf += "\n\n$$\n\n"
            text_product = TextProduct.TextProduct( buf )
            skip_con = False
            if (text_product.afos[:3] == "FLS" and 
                len(text_product.segments) > 4):
                skip_con = True

            log.msg( str(text_product) )
            for j in range(len(text_product.segments)):
                segment_processor(text_product, j, skip_con)

            if skip_con:
                wfo = text_product.get_iembot_source()
                jabber_txt = "%s: %s has sent an updated FLS product (continued products were not reported here).  Consult this website for more details. %s?wfo=%s" % (wfo, wfo, secret.RIVER_APP, wfo)
                jabber_html = "%s has sent an updated FLS product (continued products were not reported here).  Consult <a href=\"%s?wfo=%s\">this website</a> for more details." % (wfo, secret.RIVER_APP, wfo)
                jabber.sendMessage(jabber_txt, jabber_html)
                twt = "Updated Flood Statement"
                uri = "%s?wfo=%s" % (secret.RIVER_APP, wfo)
                common.tweet([wfo,], twt, uri)

        except Exception, myexp:
            common.email_error(myexp, buf)

def alert_error(tp, errorText):
    msg = "%s: iembot processing error:\nProduct: %s\nError:%s" % \
            (tp.get_iembot_source(), \
             tp.get_product_id(), errorText )

    htmlmsg = "<span style='color: #FF0000; font-weight: bold;'>\
iembot processing error:</span><br />Product: %s<br />Error: %s" % \
            (tp.get_product_id(), errorText )
    jabber.sendMessage(msg, htmlmsg)

def ugc_to_text(ugclist):
    """
    Need a helper function to convert an array of ugc codes to a textual
    representation
    """
    states = {}
    for ugc in ugclist:
        stabbr = ugc[:2]
        if not states.has_key(stabbr):
            states[stabbr] = []
        if not ugc_dict.has_key(ugc):
            log.msg("ERROR: Unknown ugc %s" % (ugc,))
            name = "((%s))" % (ugc,)
        else:
            name = ugc_dict[ugc]
        states[stabbr].append(name)

    txt = []
    for st in states.keys():
        states[st].sort()
        str = " %s [%s]" % (", ".join(states[st]), st)
        if len(str) > 350:
            str = " %s counties/zones in [%s]" % (len(states[st]), st)
        txt.append(str)

    return " and".join(txt)

def segment_processor(text_product, i, skip_con):
    """ The real data processor here """
    gmtnow = mx.DateTime.gmt()
    local_offset = mx.DateTime.RelativeDateTime(hours= reference.offsets[text_product.z])
    seg = text_product.segments[i]


    # A segment must have UGC
    if (len(seg.ugc) == 0):
        return
    
    # Firstly, certain products may not contain VTEC, silently return
    if (["MWS","FLS","FLW","CFW"].__contains__(text_product.afos[:3])):
        if (len(seg.vtec) == 0):
            log.msg("I FOUND NO VTEC, BUT THAT IS OK")
            return

    # If we found no VTEC and it has UGC, we complain about this
    if (len(seg.vtec) == 0):
        if text_product.get_iembot_source() == 'JSJ':
            return
        if text_product.issueTime.year < 2005:
            text_product.generate_fake_vtec()
        else:
            alert_error(text_product, 
         "Missing or incomplete VTEC encoding in segment number %s" % (i+1,))
            raise NoVTECFoundError("No VTEC coding found for this segment")

    # New policy, we only insert the relevant stuff!
    if (i == 0):
        product_text = text_product.raw
    else:
        product_text = "%s\n\n%s\n\n&&\n\n%s" % (text_product.product_header, \
                    re.sub("'", "\\'", seg.raw), text_product.fcster)

    end_ts = None
    # A segment could have multiple vtec codes :)
    for vtec in seg.vtec:
        if (vtec.status == "T"):
            return
        ugc = seg.ugc
        hvtec = seg.hvtec

        # Set up Jabber Dict for stuff to fill in
        jmsg_dict = {'wfo': vtec.office, 'product': vtec.productString(),
             'county': ugc_to_text(ugc), 'sts': ' ', 'ets': ' ', 
             'svs_special': '',
             'year': text_product.issueTime.year, 'phenomena': vtec.phenomena,
             'eventid': vtec.ETN, 'significance': vtec.significance,
             'url': "%s#%s" % (secret.VTEC_APP, vtec.url(text_product.issueTime.year)) }

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

        if (vtec.phenomena in ['TO',] and vtec.significance == 'W'):
            jmsg_dict['svs_special'] = seg.svs_search()

        # We need to get the County Name
        affectedWFOS = {}
        for k in range(len(ugc)):
            cnty = ugc[k]
            if (ugc2wfo.has_key(cnty)):
                for c in ugc2wfo[cnty]:
                    affectedWFOS[ c ] = 1

        # Test for affectedWFOS
        if (len(affectedWFOS) == 0):
            affectedWFOS[ vtec.office ] = 1

        if text_product.afos[:3] == "RFW" and vtec.office in ['AMA','LUB','MAF','EPZ','ABQ','TWC','PSR','FGZ','VEF']:
            affectedWFOS["RGN3FWX"] = 1

        # Check for Hydro-VTEC stuff
        if (len(hvtec) > 0 and hvtec[0].nwsli != "00000"):
            nwsli = hvtec[0].nwsli
            rname = "((%s))" % (nwsli,)
            if (nwsli_dict.has_key(nwsli)):
                rname = "the "+ nwsli_dict[nwsli]
            jmsg_dict['county'] = rname
            seg.bullet_splitter()
            if (len(seg.bullets) > 3):
                stage_text = ""
                flood_text = ""
                forecast_text = ""
                for qqq in range(len(seg.bullets)):
                    if (seg.bullets[qqq].strip().find("FLOOD STAGE") == 0):
                        flood_text = seg.bullets[qqq]
                    if (seg.bullets[qqq].strip().find("FORECAST") == 0):
                        forecast_text = seg.bullets[qqq]
                    if seg.bullets[qqq].strip().find("AT ") == 0 and stage_text == "":
                        stage_text = seg.bullets[qqq]


                deffer = DBPOOL.runOperation("""INSERT into riverpro(nwsli, stage_text, 
                  flood_text, forecast_text, severity) VALUES 
                  (%s,%s,%s,%s,%s) """, (nwsli, stage_text, flood_text, forecast_text, hvtec[0].severity) )
                deffer.addErrback( common.email_error, 'RIVERPRO ERROR')
          

        warning_table = "warnings_%s" % (text_product.issueTime.year,)
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
            fcster = re.sub("'", " ", text_product.fcster)
        # Insert Polygon
            if (seg.giswkt != None):
                deffer = DBPOOL.runOperation("""INSERT into """+ warning_table +""" (issue, expire, report, 
                 significance, geom, phenomena, gtype, wfo, eventid, status, updated, 
                fcster, hvtec_nwsli) VALUES (%s,%s,%s,%s,%s,%s,%s, 
                %s,%s,%s,%s, %s, %s)""", (bts.strftime(TIMEFORMAT), vtec.endTS.strftime(TIMEFORMAT) , 
                                text_product.raw, vtec.significance, 
      seg.giswkt, vtec.phenomena, 'P', vtec.office, vtec.ETN, vtec.action, 
      text_product.issueTime.strftime(TIMEFORMAT), fcster, seg.get_hvtec_nwsli() ) )
                deffer.addErrback( common.email_error, "INSERT GISWKT")

            # Insert Counties
            for k in range(len(ugc)):
                cnty = ugc[k]
                fcster = re.sub("'", " ", text_product.fcster)
  
                deffer = DBPOOL.runOperation("""INSERT into """+ warning_table +""" 
                (issue,expire,report, geom, phenomena, gtype, wfo, eventid, status,
                updated, fcster, ugc, significance, hvtec_nwsli) VALUES(%s, %s, %s, 
                (select geom from nws_ugc WHERE ugc = %s LIMIT 1), %s, 'C', %s,%s,%s,%s, 
                %s, %s,%s, %s)""",  (bts.strftime(TIMEFORMAT), vtec.endTS.strftime(TIMEFORMAT), 
                                      text_product.raw, cnty, vtec.phenomena, vtec.office, vtec.ETN, 
                                      vtec.action, text_product.issueTime.strftime(TIMEFORMAT), 
                                      fcster, cnty, vtec.significance, seg.get_hvtec_nwsli() ))
                deffer.addErrback( common.email_error, "INSERT")
            channels = []
            for w in affectedWFOS.keys():
                channels.append(w)
                jmsg_dict['w'] = w
                jabberTxt = "%(w)s: %(wfo)s %(product)s%(sts)sfor \
%(county)s till %(ets)s %(svs_special)s %(url)s" % jmsg_dict
                jabberHTML = "%(wfo)s <a href='%(url)s'>%(product)s</a>%(sts)sfor %(county)s \
till %(ets)s %(svs_special)s" % jmsg_dict
                jabber.sendMessage(jabberTxt, jabberHTML)
            twt = "%(product)s%(sts)sfor %(county)s till %(ets)s" % jmsg_dict
            url = jmsg_dict["url"]
            common.tweet(channels, twt, url)

        elif (vtec.action in ["CON", "COR"] ):
        # Lets find our county and update it with action
        # Not worry about polygon at the moment.
            for cnty in ugc:
                _expire = 'expire'
                if vtec.endTS is None:
                    _expire = 'expire + \'10 days\'::interval'
                deffer = DBPOOL.runOperation("""UPDATE """+ warning_table +""" SET status = %s, 
                    updated = %s, expire = """+ _expire +""" WHERE ugc = %s and wfo = %s and eventid = %s and 
                    phenomena = %s and significance = %s""", ( vtec.action, 
                        text_product.issueTime.strftime(TIMEFORMAT), cnty, vtec.office, vtec.ETN,
                            vtec.phenomena, vtec.significance ))
                deffer.addErrback( common.email_error, "UPDATE")

            if (len(seg.vtec) == 1):
                deffer = DBPOOL.runOperation("""UPDATE """+ warning_table +""" SET status = %s,  
                     updated = %s WHERE gtype = 'P' and wfo = %s and eventid = %s and phenomena = %s 
                     and significance = %s""", (vtec.action, text_product.issueTime.strftime(TIMEFORMAT), 
                                                vtec.office, vtec.ETN, vtec.phenomena, vtec.significance))
                deffer.addErrback( common.email_error, "UPDATE!")

            channels = []
            for w in affectedWFOS.keys():
                jmsg_dict['w'] = w
                jabberTxt = "%(w)s: %(wfo)s %(product)s%(sts)sfor \
%(county)s till %(ets)s %(svs_special)s %(url)s" % jmsg_dict
                jabberHTML = "%(wfo)s <a href='%(url)s'>%(product)s</a>%(sts)sfor %(county)s \
till %(ets)s %(svs_special)s" % jmsg_dict
                if not skip_con:
                    jabber.sendMessage(jabberTxt, jabberHTML)
                    channels.append(w)
            twt = "%(product)s%(sts)sfor %(county)s till %(ets)s" % jmsg_dict
            url = jmsg_dict["url"]
            common.tweet(channels, twt, url)
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
                deffer = DBPOOL.runOperation("""UPDATE """+ warning_table +""" SET status = %s, 
                expire = %s, updated = %s, issue = """+ issueSpecial +""" WHERE ugc = %s and wfo = %s and 
                eventid = %s and phenomena = %s and significance = %s""", (vtec.action, 
                        end_ts.strftime(TIMEFORMAT), text_product.issueTime.strftime(TIMEFORMAT), 
                        cnty, vtec.office, vtec.ETN, vtec.phenomena, vtec.significance))
                deffer.addErrback( common.email_error, "UPDATE12")

            # If this is the only county, we can cancel the polygon too
            if (len(text_product.segments) == 1):
                log.msg("Updating Polygon as well")
                deffer = DBPOOL.runOperation("""UPDATE """+warning_table+""" SET status = %s, 
                expire = %s, updated = %s WHERE gtype = 'P' and wfo = %s and eventid = %s 
                and phenomena = %s and significance = %s""" , (vtec.action, end_ts.strftime(TIMEFORMAT), 
                        text_product.issueTime.strftime(TIMEFORMAT), vtec.office, vtec.ETN, 
                        vtec.phenomena, vtec.significance) )
                deffer.addErrback( common.email_error, "UPDATEPOLY")

            jmsg_dict['action'] = "cancels"
            fmt = "%(w)s: %(wfo)s  %(product)s for %(county)s %(svs_special)s "
            htmlfmt = "%(wfo)s <a href='%(url)s'>%(product)s</a> for %(county)s %(svs_special)s"
            if (vtec.action == "EXT" and vtec.beginTS != None):
                jmsg_dict['sts'] = " valid at %s%s " % ( \
                (vtec.beginTS - local_offset).strftime(efmt), text_product.z )
                fmt = "%(w)s: %(wfo)s  %(product)s for %(county)s %(svs_special)s\
%(sts)still %(ets)s"
                htmlfmt = "%(wfo)s <a href='%(url)s'>%(product)s</a>\
 for %(county)s%(sts)still %(ets)s %(svs_special)s"
            elif (vtec.action == "EXT"):
                fmt += " till %(ets)s"
                htmlfmt += " till %(ets)s"
            fmt += " %(url)s"
            if (vtec.action != 'UPG'):
                channels = []
                for w in affectedWFOS.keys():
                    jmsg_dict['w'] = w
                    jabber.sendMessage(fmt % jmsg_dict, htmlfmt % jmsg_dict)
                    channels.append( w )
                twt = "%(product)s%(sts)sfor %(county)s till %(ets)s" % jmsg_dict
                url = jmsg_dict["url"]
                common.tweet(channels, twt, url)

        if (vtec.action != "NEW"):
            ugc_limiter = ""
            for cnty in ugc:
                ugc_limiter += "'%s'," % (cnty,)

            log.msg("Updating SVS For:"+ ugc_limiter[:-1] )
            deffer = DBPOOL.runOperation("""UPDATE """+ warning_table +""" SET svs = 
                  (CASE WHEN (svs IS NULL) THEN '__' ELSE svs END) 
                   || %s || '__' WHERE eventid = %s and wfo = %s 
                   and phenomena = %s and significance = %s 
                   and ugc IN ("""+ ugc_limiter[:-1] +""")""" , (text_product.raw, vtec.ETN, vtec.office, 
                    vtec.phenomena, vtec.significance  ))
            deffer.addErrback( common.email_error, "UPDATE NOTNEW")

    # Update polygon if necessary
    if (vtec.action != "NEW" and seg.giswkt is not None):
        log.msg("Updating SVS For Polygon")
        deffer = DBPOOL.runOperation("""UPDATE """+ warning_table +""" SET svs = 
              (CASE WHEN (svs IS NULL) THEN '__' ELSE svs END) 
               || %s || '__' WHERE eventid = %s and wfo = %s 
               and phenomena = %s and significance = %s 
               and gtype = P""" , (text_product.raw, vtec.ETN, vtec.office, 
                vtec.phenomena, vtec.significance )  )
        deffer.addErrback( common.email_error, "BLAH")

    # New fancy SBW Stuff!
    if seg.giswkt is not None:
        # If we are dropping the product and there is only 1 segment
        # We need not wait for more action, we do two things
        # 1. Update the polygon_end to cancel time for the last polygon
        # 2. Update everybodies expiration time, product changed yo!
        if vtec.action in ["CAN", "UPG"] and len(text_product.segments) == 1:
             deffer = DBPOOL.runOperation("""UPDATE sbw_"""+ str(text_product.issueTime.year) +""" SET 
                polygon_end = (CASE WHEN polygon_end = expire
                               THEN %s ELSE polygon_end END), 
                expire = %s WHERE 
                eventid = %s and wfo = %s 
                and phenomena = %s and significance = %s""", (
                text_product.issueTime.strftime(TIMEFORMAT), text_product.issueTime.strftime(TIMEFORMAT), 
                vtec.ETN, vtec.office, vtec.phenomena, vtec.significance) )
             deffer.addErrback( common.email_error, "UPDATESBW")

        # If we are VTEC CON, then we need to find the last polygon
        # and update its expiration time, since we have new info!
        if vtec.action == "CON":
            deffer = DBPOOL.runOperation("""UPDATE sbw_"""+ str(text_product.issueTime.year) +""" SET 
                polygon_end = %s WHERE polygon_end = expire and eventid = %s and wfo = %s 
                and phenomena = %s and significance = %s""" , ( text_product.issueTime.strftime(TIMEFORMAT), 
                vtec.ETN, vtec.office, vtec.phenomena, vtec.significance))
            deffer.addErrback( common.email_error, "UPDATE SBW")


        my_sts = "'%s+00'" % (vtec.beginTS,)
        if vtec.beginTS is None:
             my_sts = "(SELECT issue from sbw_%s WHERE eventid = %s \
              and wfo = '%s' and phenomena = '%s' and significance = '%s' \
              LIMIT 1)" % (text_product.issueTime.year, vtec.ETN, vtec.office, \
              vtec.phenomena, vtec.significance)
        my_ets = "'%s+00'" % (vtec.endTS,)
        if vtec.endTS is None:
             my_ets = "(SELECT expire from sbw_%s WHERE eventid = %s \
              and wfo = '%s' and phenomena = '%s' and significance = '%s' \
              LIMIT 1)" % (text_product.issueTime.year, vtec.ETN, vtec.office, \
              vtec.phenomena, vtec.significance)

        if vtec.action in ['CAN',]:
            sql = """INSERT into sbw_"""+ str(text_product.issueTime.year) +"""(wfo, eventid, 
                significance, phenomena, issue, expire, init_expire, polygon_begin, 
                polygon_end, geom, status, report, windtag, hailtag) VALUES (%s,
                %s,%s,%s,"""+ my_sts +""",%s,"""+ my_ets +""",%s,%s,%s,%s,%s,%s,%s)"""
            myargs = (vtec.office, vtec.ETN, 
                 vtec.significance, vtec.phenomena, 
                 text_product.issueTime.strftime(TIMEFORMAT), 
                 text_product.issueTime.strftime(TIMEFORMAT), 
                 text_product.issueTime.strftime(TIMEFORMAT), 
                  seg.giswkt, vtec.action, product_text,
                 (seg.windtag or 'Null'), (seg.hailtag or 'Null'))

        elif vtec.action in ['EXP', 'UPG', 'EXT']:
            sql = """INSERT into sbw_"""+ str(text_product.issueTime.year) +"""(wfo, eventid, significance,
                phenomena, issue, expire, init_expire, polygon_begin, polygon_end, geom, 
                status, report, windtag, hailtag) VALUES (%s,
                %s,%s,%s, """+ my_sts +""","""+ my_ets +""","""+ my_ets +""",%s,%s, 
                %s,%s,%s,%s,%s)"""
            vvv = text_product.issueTime.strftime(TIMEFORMAT)
            if vtec.endTS:
                vvv = vtec.endTS.strftime(TIMEFORMAT)
            myargs = ( vtec.office, vtec.ETN, 
                 vtec.significance, vtec.phenomena, vvv, vvv, 
                  seg.giswkt, vtec.action, product_text, (seg.windtag or 'Null'), (seg.hailtag or 'Null'))
        else:
            _expire = vtec.endTS
            if vtec.endTS is None:
                _expire = mx.DateTime.now() + mx.DateTime.RelativeDateTime(days=10)
            sql = """INSERT into sbw_"""+ str(text_product.issueTime.year) +"""(wfo, eventid, 
                significance, phenomena, issue, expire, init_expire, polygon_begin, polygon_end, geom, 
                status, report, windtag, hailtag) VALUES (%s,
                %s,%s,%s, """+ my_sts +""",%s,%s,%s,%s, %s,%s,%s,%s,%s)""" 
            vvv = text_product.issueTime.strftime(TIMEFORMAT)
            if vtec.beginTS:
                vvv = vtec.beginTS.strftime(TIMEFORMAT)
            myargs = (vtec.office, vtec.ETN, 
                 vtec.significance, vtec.phenomena, _expire.strftime(TIMEFORMAT), 
                 _expire.strftime(TIMEFORMAT), vvv, 
                 _expire.strftime(TIMEFORMAT), seg.giswkt, vtec.action, product_text,
                 seg.windtag, seg.hailtag)
        deffer = DBPOOL.runOperation(sql, myargs)
        deffer.addErrback( common.email_error, "LASTONE")


""" Load me up with NWS dictionaries! """
ugc_dict = {}
ugc2wfo = {}
sql = "SELECT name, ugc, wfo from nws_ugc WHERE name IS NOT Null"
rs = POSTGIS.query(sql).dictresult()
for i in range(len(rs)):
    ugc_dict[ rs[i]['ugc'] ] = (rs[i]["name"]).replace("\x92"," ").replace("\xc2"," ")
    ugc2wfo[ rs[i]['ugc'] ] = re.findall(r'([A-Z][A-Z][A-Z])',rs[i]['wfo'])

""" Load up H-VTEC NWSLI reference """
nwsli_dict = {}
sql = "SELECT nwsli, \
 river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname \
 from hvtec_nwsli"
rs = POSTGIS.query(sql).dictresult()
for i in range(len(rs)):
    nwsli_dict[ rs[i]['nwsli'] ] = (rs[i]['rname']).replace("&"," and ")

myJid = jid.JID('%s@%s/vtecparser_%s' % \
      (secret.iembot_ingest_user, secret.chatserver, \
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
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

