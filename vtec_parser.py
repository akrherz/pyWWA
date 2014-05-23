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
""" VTEC product ingestor 

The warnings table has the following timestamp based columns, this gets ugly 
with watches.  Lets try to explain

    issue   <- VTEC timestamp of when this event was valid for
    expire  <- When does this VTEC product expire
    updated <- Product Timestamp of when a product gets updated
    init_expire <- When did this product initially expire
    product_issue <- When was this product issued by the NWS

"""

from twisted.python import log
from twisted.python import logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('vtec_parser.log','logs'))

# Twisted Python imports
from twisted.internet import reactor
from twisted.enterprise import adbapi

# Standard Python modules
import re
import os
import datetime

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

# pyLDM https://github.com/akrherz/pyLDM
from pyldm import ldmbridge
# pyIEM https://github.com/akrherz/pyIEM
from pyiem import iemtz, reference
from pyiem.nws import product

import common

POSTGIS = adbapi.ConnectionPool("twistedpg", database="postgis", cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password')) 


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
        log.msg('connectionLost')
        log.err( reason )
        reactor.callLater(7, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


    def process_data(self, buf):
        """ Process the product """
        try:
            # Make sure we have a trailing $$
            if buf.find("$$") == -1:
                common.email_error("No $$ Found!", buf)
                buf += "\n\n$$\n\n"
            text_product = product.TextProduct( buf )
            xtra = {
                    'product_id': text_product.get_product_id(),
                    'channels': []
                    }
            skip_con = False
            if (text_product.afos[:3] == "FLS" and 
                len(text_product.segments) > 4):
                skip_con = True

            log.msg( str(text_product) )
            for j in range(len(text_product.segments)):
                df = POSTGIS.runInteraction(segment_processor, text_product, j, 
                                      skip_con)
                df.addErrback(common.email_error, text_product.unixtext)
                df.addErrback( log.err )
                
            if skip_con:
                wfo = text_product.source[1:]
                xtra['channels'].append(wfo)
                jabber_txt = "%s has sent an updated FLS product (continued products were not reported here).  Consult this website for more details. %s?wfo=%s" % (wfo, 
                                                                                                                                                                        config.get('urls', 'riverapp'), wfo)
                jabber_html = "%s has sent an updated FLS product (continued products were not reported here).  Consult <a href=\"%s?wfo=%s\">this website</a> for more details." % (wfo, config.get('urls', 'riverapp'), wfo)
                jabber.sendMessage(jabber_txt, jabber_html, xtra)
                twt = "Updated Flood Statement"
                uri = "%s?wfo=%s" % (config.get('urls', 'riverapp'), wfo)
                common.tweet([wfo,], twt, uri)

        except Exception, myexp:
            common.email_error(myexp, buf)

def ugc_to_text(ugclist):
    """
    Need a helper function to convert an array of ugc codes to a textual
    representation
    """
    states = {}
    for ugc in ugclist:
        stabbr = ugc.state
        if not states.has_key(stabbr):
            states[stabbr] = []
        if not ugc_dict.has_key(str(ugc)):
            log.msg("ERROR: Unknown ugc %s" % (ugc,))
            name = "((%s))" % (ugc,)
        else:
            name = ugc_dict[str(ugc)]
        states[stabbr].append(name)

    txt = []
    for st in states.keys():
        states[st].sort()
        s = " %s [%s]" % (", ".join(states[st]), st)
        if len(s) > 350:
            s = " %s counties/zones in [%s]" % (len(states[st]), st)
        txt.append(s)

    return " and".join(txt)

def segment_processor(txn, text_product, i, skip_con):
    """ The real data processor here """
    gmtnow = datetime.datetime.utcnow()
    gmtnow = gmtnow.replace(tzinfo=iemtz.UTC())
    local_offset = datetime.timedelta(hours= reference.offsets.get(
                                                    text_product.z, 0))
    seg = text_product.segments[i]


    # A segment must have UGC
    if len(seg.ugcs) == 0:
        return
    
    # Firstly, certain products may not contain VTEC, silently return
    if (["MWS","FLS","FLW","CFW"].__contains__(text_product.afos[:3])):
        if (len(seg.vtec) == 0):
            log.msg("I FOUND NO VTEC, BUT THAT IS OK")
            return

    # If we found no VTEC and it has UGC, we complain about this
    if len(seg.vtec) == 0:
        if text_product.source[1:] in ['JSJ','STU']:
            return
        if text_product.valid.year < 2005:
            text_product.generate_fake_vtec()
        else:
            raise NoVTECFoundError("No VTEC coding found for this segment")

    product_text = text_product.text

    end_ts = None
    # A segment could have multiple vtec codes :)
    for vtec in seg.vtec:
        if vtec.status == "T":
            return

        xtra = {
            'status' : vtec.status,
            'vtec' : vtec.getID(text_product.valid.year),
            'ptype': vtec.phenomena,
            'product_id': text_product.get_product_id(),
            'channels': []
            }
        if seg.giswkt is not None:
            xtra['category'] = 'SBW'
            xtra['geometry'] = seg.giswkt.replace("SRID=4326;", "")
        if vtec.endts is not None:
            xtra['expire'] = vtec.endts.strftime("%Y%m%dT%H:%M:00")

        ugc = seg.ugcs
        hvtec = seg.hvtec

        # Set up Jabber Dict for stuff to fill in
        jmsg_dict = {'wfo': vtec.office, 'product': vtec.product_string(),
             'county': ugc_to_text(ugc), 'sts': ' ', 'ets': ' ', 
             'svs_special': '',
             'year': text_product.valid.year, 'phenomena': vtec.phenomena,
             'eventid': vtec.ETN, 'significance': vtec.significance,
             'url': "%s#%s" % (config.get('urls', 'vtec'), 
                               vtec.url(text_product.valid.year)) }

        if (vtec.begints != None and 
            vtec.begints > (gmtnow + datetime.timedelta(hours=+1))):
            efmt = "%b %d, %-I:%M %p "
            jmsg_dict['sts'] = " valid at %s%s " % (
                                (vtec.begints - local_offset).strftime(efmt), 
                                text_product.z)
        else:
            efmt = "%-I:%M %p "

        if vtec.endts is None:
            jmsg_dict['ets'] =  "further notice"
        else:
            if (vtec.endts > (gmtnow + datetime.timedelta(days=+1))):
                efmt = "%b %d, %-I:%M %p "
            jmsg_dict['ets'] =  "%s%s" % \
                ((vtec.endts - local_offset).strftime(efmt), text_product.z)

        if (vtec.phenomena in ['TO',] and vtec.significance == 'W'):
            jmsg_dict['svs_special'] = seg.svs_search()

        # We need to get the County Name
        affectedWFOS = {}
        for k in range(len(ugc)):
            cnty = str(ugc[k])
            if (ugc2wfo.has_key(cnty)):
                for c in ugc2wfo[cnty]:
                    affectedWFOS[ c ] = 1
        if 'PSR' in affectedWFOS.keys():
            affectedWFOS = {vtec.office: 1}
        # Test for affectedWFOS
        if (len(affectedWFOS) == 0):
            affectedWFOS[ vtec.office ] = 1

        if text_product.afos[:3] == "RFW" and vtec.office in ['AMA','LUB','MAF','EPZ','ABQ','TWC','PSR','FGZ','VEF']:
            affectedWFOS["RGN3FWX"] = 1

        # Check for Hydro-VTEC stuff
        if len(hvtec) > 0 and hvtec[0].nwsli.id != "00000":
            nwsli = hvtec[0].nwsli.id
            rname = "((%s))" % (nwsli,)
            if (nwsli_dict.has_key(nwsli)):
                rname = "the "+ nwsli_dict[nwsli]
            jmsg_dict['county'] = rname
            if (len(seg.bullets) > 3):
                stage_text = ""
                flood_text = ""
                forecast_text = ""
                for qqq in range(len(seg.bullets)):
                    if (seg.bullets[qqq].strip().upper().find("FLOOD STAGE") == 0):
                        flood_text = seg.bullets[qqq]
                    if (seg.bullets[qqq].strip().upper().find("FORECAST") == 0):
                        forecast_text = seg.bullets[qqq]
                    if seg.bullets[qqq].strip().upper().find("AT ") == 0 and stage_text == "":
                        stage_text = seg.bullets[qqq]


                txn.execute("""INSERT into riverpro(nwsli, stage_text, 
                  flood_text, forecast_text, severity) VALUES 
                  (%s,%s,%s,%s,%s) """, (nwsli, stage_text, flood_text, 
                                         forecast_text, hvtec[0].severity) )
                

        warning_table = "warnings_%s" % (text_product.valid.year,)
        #  NEW - New Warning
        #  EXB - Extended both in area and time (new area means new entry)
        #  EXA - Extended in area, which means new entry
        #   1. Insert any polygons
        #   2. Insert any counties
        #   3. Format Jabber message
        if vtec.action in ["NEW","EXB","EXA"]:
            if vtec.begints is None:
                vtec.begints = text_product.valid
            if vtec.endts is None:
                vtec.endts = vtec.begints + datetime.timedelta(days=1)
            bts = vtec.begints
            if (vtec.action == "EXB" or vtec.action == "EXA"):
                bts = text_product.valid
            
            # Insert Polygon
            fcster = text_product.get_signature()
            if fcster is not None:
                fcster = fcster[:24]
            if seg.sbw is not None:
                txn.execute("""INSERT into """+ warning_table +""" (issue, expire, report, 
                 significance, geom, phenomena, gtype, wfo, eventid, status, updated, 
                fcster, hvtec_nwsli, init_expire, product_issue) 
                VALUES (%s,%s,%s,%s,%s,%s,%s, 
                %s,%s,%s,%s, %s, %s, %s, %s)""", (bts, vtec.endts , 
                                text_product.text, vtec.significance, 
      seg.giswkt, vtec.phenomena, 'P', vtec.office, vtec.ETN, vtec.action, 
      text_product.valid, fcster, seg.get_hvtec_nwsli(),
      vtec.endts, text_product.valid ) )
                
            # Insert Counties
            for k in range(len(ugc)):
                cnty = str(ugc[k])
                txn.execute("""INSERT into """+ warning_table +""" 
        (issue,expire,report, geom, phenomena, gtype, wfo, eventid, status,
        updated, fcster, ugc, significance, hvtec_nwsli, gid, init_expire,
        product_issue) VALUES(%s, %s, %s, 
        (select geom from ugcs WHERE ugc = %s and end_ts is null LIMIT 1), %s, 
        'C', %s,%s,%s,%s, 
        %s, %s,%s, %s, get_gid(%s, %s), %s, %s)""",  (bts, vtec.endts, 
                text_product.text, cnty, vtec.phenomena, vtec.office, vtec.ETN, 
                vtec.action, text_product.valid, 
                fcster, cnty, vtec.significance, seg.get_hvtec_nwsli(),
                cnty, text_product.valid, vtec.endts, text_product.valid ))
            for w in affectedWFOS.keys():
                xtra['channels'].append(w)
            jabberTxt = "%(wfo)s %(product)s%(sts)sfor \
%(county)s till %(ets)s %(svs_special)s %(url)s" % jmsg_dict
            jabberHTML = "%(wfo)s <a href='%(url)s'>%(product)s</a>%(sts)sfor %(county)s \
till %(ets)s %(svs_special)s" % jmsg_dict
            jabber.sendMessage(jabberTxt, jabberHTML, xtra)
            twt = "%(product)s%(sts)sfor %(county)s till %(ets)s" % jmsg_dict
            url = jmsg_dict["url"]
            common.tweet(xtra['channels'], twt, url)

        elif (vtec.action in ["CON", "COR"] ):
        # Lets find our county and update it with action
        # Not worry about polygon at the moment.
            for cnty in ugc:
                _expire = 'expire'
                if vtec.endts is None:
                    _expire = 'expire + \'10 days\'::interval'
                txn.execute("""UPDATE """+ warning_table +""" SET status = %s, 
                    updated = %s, expire = """+ _expire +""",
                    init_expire = """+ _expire +""" WHERE ugc = %s and 
                    wfo = %s and eventid = %s and 
                    phenomena = %s and significance = %s""", ( vtec.action, 
                        text_product.valid, str(cnty), vtec.office, vtec.ETN,
                            vtec.phenomena, vtec.significance ))
             
            if (len(seg.vtec) == 1):
                txn.execute("""UPDATE """+ warning_table +""" SET status = %s,  
                     updated = %s WHERE gtype = 'P' and wfo = %s and eventid = %s and phenomena = %s 
                     and significance = %s""", (vtec.action, text_product.valid, 
                                                vtec.office, vtec.ETN, vtec.phenomena, vtec.significance))
               
            for w in affectedWFOS.keys():
                xtra['channels'].append(w)
            jabberTxt = "%(wfo)s %(product)s%(sts)sfor \
%(county)s till %(ets)s %(svs_special)s %(url)s" % jmsg_dict
            jabberHTML = "%(wfo)s <a href='%(url)s'>%(product)s</a>%(sts)sfor %(county)s \
till %(ets)s %(svs_special)s" % jmsg_dict
            if not skip_con:
                jabber.sendMessage(jabberTxt, jabberHTML, xtra)
            twt = "%(product)s%(sts)sfor %(county)s till %(ets)s" % jmsg_dict
            url = jmsg_dict["url"]
            common.tweet(xtra['channels'], twt, url)
#--

        elif (vtec.action in ["CAN", "EXP", "UPG", "EXT"] ):
            end_ts = vtec.endts
            if (vtec.endts is None):  # 7 days into the future?
                end_ts = text_product.valid + \
                         datetime.timedelta(days=7)
            if (vtec.action == "CAN" or vtec.action == "UPG"):
                end_ts = text_product.valid
            issueSpecial = "issue"
            if vtec.action in ["EXT","UPG"] and vtec.begints is not None: 
                issueSpecial = "'%s'" % (vtec.begints,)
        # Lets cancel county
            for cnty in ugc:
                txn.execute("""UPDATE """+ warning_table +""" SET status = %s, 
                expire = %s, updated = %s, issue = """+ issueSpecial +""" WHERE ugc = %s and wfo = %s and 
                eventid = %s and phenomena = %s and significance = %s""", (vtec.action, 
                        end_ts, text_product.valid, 
                        str(cnty), vtec.office, vtec.ETN, vtec.phenomena, vtec.significance))
             
            # If this is the only county, we can cancel the polygon too
            if len(text_product.segments) > 1 and len(text_product.segments[1].vtec) == 0:
                log.msg("Updating Polygon as well")
                txn.execute("""UPDATE """+warning_table+""" SET status = %s, 
                expire = %s, updated = %s WHERE gtype = 'P' and wfo = %s and eventid = %s 
                and phenomena = %s and significance = %s""" , (vtec.action, end_ts, 
                        text_product.valid, vtec.office, vtec.ETN, 
                        vtec.phenomena, vtec.significance) )
            
            jmsg_dict['action'] = "cancels"
            fmt = "%(wfo)s  %(product)s for %(county)s %(svs_special)s "
            htmlfmt = "%(wfo)s <a href='%(url)s'>%(product)s</a> for %(county)s %(svs_special)s"
            if (vtec.action == "EXT" and vtec.begints != None):
                jmsg_dict['sts'] = " valid at %s%s " % ( \
                (vtec.begints - local_offset).strftime(efmt), text_product.z )
                fmt = "%(wfo)s  %(product)s for %(county)s %(svs_special)s\
%(sts)still %(ets)s"
                htmlfmt = "%(wfo)s <a href='%(url)s'>%(product)s</a>\
 for %(county)s%(sts)still %(ets)s %(svs_special)s"
            elif (vtec.action == "EXT"):
                fmt += " till %(ets)s"
                htmlfmt += " till %(ets)s"
            fmt += " %(url)s"
            if (vtec.action != 'UPG'):
                for w in affectedWFOS.keys():
                    xtra['channels'].append( w )
                jabber.sendMessage(fmt % jmsg_dict, htmlfmt % jmsg_dict, 
                                       xtra)
                twt = "%(product)s%(sts)sfor %(county)s till %(ets)s" % jmsg_dict
                url = jmsg_dict["url"]
                common.tweet(xtra['channels'], twt, url)

        if (vtec.action != "NEW"):
            ugc_limiter = ""
            for cnty in ugc:
                ugc_limiter += "'%s'," % (cnty,)

            log.msg("Updating SVS For:"+ ugc_limiter[:-1] )
            txn.execute("""UPDATE """+ warning_table +""" SET svs = 
                  (CASE WHEN (svs IS NULL) THEN '__' ELSE svs END) 
                   || %s || '__' WHERE eventid = %s and wfo = %s 
                   and phenomena = %s and significance = %s 
                   and ugc IN ("""+ ugc_limiter[:-1] +""")""" , (text_product.text, vtec.ETN, vtec.office, 
                    vtec.phenomena, vtec.significance  ))
            
    # Update polygon if necessary
    if (vtec.action != "NEW" and seg.sbw is not None):
        log.msg("Updating SVS For Polygon")
        txn.execute("""UPDATE """+ warning_table +""" SET svs = 
              (CASE WHEN (svs IS NULL) THEN '__' ELSE svs END) 
               || %s || '__' WHERE eventid = %s and wfo = %s 
               and phenomena = %s and significance = %s 
               and gtype = 'P'""" , (text_product.text, vtec.ETN, vtec.office, 
                vtec.phenomena, vtec.significance )  )
     
    # New fancy SBW Stuff!
    if seg.sbw is not None:
        # If we are dropping the product and there is only 1 segment
        # We need not wait for more action, we do two things
        # 1. Update the polygon_end to cancel time for the last polygon
        # 2. Update everybodies expiration time, product changed yo!
        if vtec.action in ["CAN", "UPG"] and len(text_product.segments) == 1:
            txn.execute("""UPDATE sbw_"""+ str(text_product.valid.year) +""" SET 
                polygon_end = (CASE WHEN polygon_end = expire
                               THEN %s ELSE polygon_end END), 
                expire = %s WHERE 
                eventid = %s and wfo = %s 
                and phenomena = %s and significance = %s""", (
                text_product.valid, text_product.valid, 
                vtec.ETN, vtec.office, vtec.phenomena, vtec.significance) )
        
        # If we are VTEC CON, then we need to find the last polygon
        # and update its expiration time, since we have new info!
        if vtec.action == "CON":
            txn.execute("""UPDATE sbw_"""+ str(text_product.valid.year) +""" SET 
                polygon_end = %s WHERE polygon_end = expire and eventid = %s and wfo = %s 
                and phenomena = %s and significance = %s""" , ( text_product.valid, 
                vtec.ETN, vtec.office, vtec.phenomena, vtec.significance))
          

        my_sts = "'%s'" % (vtec.begints,)
        if vtec.begints is None:
            my_sts = """(SELECT issue from sbw_%s WHERE eventid = %s 
              and wfo = '%s' and phenomena = '%s' and significance = '%s' 
              LIMIT 1)""" % (text_product.valid.year, vtec.ETN, vtec.office, 
              vtec.phenomena, vtec.significance)
        my_ets = "'%s'" % (vtec.endts,)
        if vtec.endts is None:
            my_ets = """(SELECT expire from sbw_%s WHERE eventid = %s 
              and wfo = '%s' and phenomena = '%s' and significance = '%s' 
              LIMIT 1)""" % (text_product.valid.year, vtec.ETN, vtec.office, 
              vtec.phenomena, vtec.significance)

        tml_valid = None
        tml_column = 'tml_geom'
        if seg.tml_giswkt and seg.tml_giswkt.find("LINE") > 0:
            tml_column = 'tml_geom_line'
        if seg.tml_valid:
            tml_valid = seg.tml_valid
        if vtec.action in ['CAN',]:
            sql = """INSERT into sbw_"""+ str(text_product.valid.year) +"""(wfo, eventid, 
                significance, phenomena, issue, expire, init_expire, polygon_begin, 
                polygon_end, geom, status, report, windtag, hailtag, tornadotag,
                tornadodamagetag, tml_valid, tml_direction, tml_sknt,
                """+ tml_column +""") VALUES (%s,
                %s,%s,%s,"""+ my_sts +""",%s,"""+ my_ets +""",%s,%s,%s,%s,%s,
                %s,%s,%s,%s, %s, %s, %s, %s)"""
            myargs = (vtec.office, vtec.ETN, 
                 vtec.significance, vtec.phenomena, 
                 text_product.valid, 
                 text_product.valid, 
                 text_product.valid, 
                  seg.giswkt, vtec.action, product_text,
                 seg.windtag, seg.hailtag, seg.tornadotag, seg.tornadodamagetag,
                 tml_valid, seg.tml_dir, seg.tml_sknt, seg.tml_giswkt)

        elif vtec.action in ['EXP', 'UPG', 'EXT']:
            sql = """INSERT into sbw_"""+ str(text_product.valid.year) +"""(wfo, eventid, significance,
                phenomena, issue, expire, init_expire, polygon_begin, polygon_end, geom, 
                status, report, windtag, hailtag, tornadotag, tornadodamagetag, tml_valid, tml_direction, tml_sknt,
                """+ tml_column +""") VALUES (%s,
                %s,%s,%s, """+ my_sts +""","""+ my_ets +""","""+ my_ets +""",%s,%s, 
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            vvv = text_product.valid
            if vtec.endts:
                vvv = vtec.endts
            myargs = ( vtec.office, vtec.ETN, 
                 vtec.significance, vtec.phenomena, vvv, vvv, 
                  seg.giswkt, vtec.action, product_text, seg.windtag, seg.hailtag,
                  seg.tornadotag, seg.tornadodamagetag,
                  tml_valid, seg.tml_dir, seg.tml_sknt, seg.tml_giswkt)
        else:
            _expire = vtec.endts
            if vtec.endts is None:
                _expire = datetime.datetime.now() + datetime.timedelta(days=10)
                _expire = _expire.replace(tzinfo=iemtz.UTC())
            sql = """INSERT into sbw_"""+ str(text_product.valid.year) +"""(wfo, eventid, 
                significance, phenomena, issue, expire, init_expire, polygon_begin, polygon_end, geom, 
                status, report, windtag, hailtag, tornadotag, tornadodamagetag, tml_valid, tml_direction, tml_sknt,
                """+ tml_column +""") VALUES (%s,
                %s,%s,%s, """+ my_sts +""",%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s)""" 
            vvv = text_product.valid
            if vtec.begints:
                vvv = vtec.begints
            myargs = (vtec.office, vtec.ETN, 
                 vtec.significance, vtec.phenomena, _expire, 
                 _expire, vvv, 
                 _expire, seg.giswkt, vtec.action, product_text,
                 seg.windtag, seg.hailtag, seg.tornadotag, seg.tornadodamagetag,
                 tml_valid, seg.tml_dir, seg.tml_sknt, seg.tml_giswkt)
        txn.execute(sql, myargs)
        

""" Load me up with NWS dictionaries! """
ugc_dict = {}
ugc2wfo = {}
def load_ugc(txn):
    """ load ugc"""
    sql = """SELECT name, ugc, wfo from ugcs WHERE 
        name IS NOT Null and end_ts is null"""
    txn.execute(sql)
    for row in txn:
        ugc_dict[ row['ugc'] ] = (row["name"]).replace("\x92"," ").replace("\xc2"," ")
        ugc2wfo[ row['ugc'] ] = re.findall(r'([A-Z][A-Z][A-Z])',row['wfo'])

""" Load up H-VTEC NWSLI reference """
nwsli_dict = {}
def load_nwsli(txn):
    """ load_nwsli"""
    sql = """SELECT nwsli, 
     river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname 
     from hvtec_nwsli"""
    txn.execute( sql )
    for row in txn:
        nwsli_dict[ row['nwsli'] ] = (row['rname']).replace("&"," and ")

    return None

def ready(res):
    ldmbridge.LDMProductFactory( MyProductIngestor() )


df = POSTGIS.runInteraction(load_nwsli)
df = POSTGIS.runInteraction(load_ugc)
df.addCallback( ready )
jabber = common.make_jabber_client('vtec_parser')

reactor.run()

