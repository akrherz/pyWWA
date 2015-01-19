""" SPC Geo Products Parser! """

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/spc_parser', facility=LOG_LOCAL2)
from twisted.python import log
from twisted.internet import reactor

# pyWWA stuff
from pyldm import ldmbridge
from pyiem.nws import product, spcpts
import os
import datetime
import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))
import common

def exception_hook(kwargs):
    print kwargs
    if not kwargs.has_key("failure"):
        return

DBPOOL = common.get_database('postgis')
WAITFOR = 20


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.err( reason )
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


    def process_data(self, buf):
        """ Process the product """
        df = DBPOOL.runInteraction(real_parser, buf)
        df.addErrback(common.email_error, buf)
        
def consume(txn, spc, outlook):
    """
    Do the geometric stuff we care about, including storage
    """

    # Insert geometry into database table please
    sql = """INSERT into spc_outlooks(issue, valid, expire,
        threshold, category, day, outlook_type, geom)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""" 
    args = (spc.issue,  spc.valid, spc.expire,
            outlook.threshold, outlook.category, spc.day, 
            spc.outlook_type, "SRID=4326;%s" % (outlook.geometry.wkt,))
    txn.execute( sql, args )

    # Don't subject us to bad geos
    if outlook.category == 'TSTM':
        return []
    
    # Search for WFOs
    sql = """select distinct wfo from ugcs WHERE 
       st_contains(ST_geomFromEWKT('SRID=4326;%s'), centroid) and 
       substr(ugc,3,1) = 'C' and wfo is not null
       and end_ts is null""" % (outlook.geometry.wkt,)

    txn.execute( sql )
    affectedWFOS = []
    for row in txn.fetchall():
        affectedWFOS.append( row['wfo'] )
    affectedWFOS.sort()
    log.msg("Category: %s Threshold: %s  #WFOS: %s %s" % (
        outlook.category, outlook.threshold,  len(affectedWFOS),
        ",".join(affectedWFOS)))
    
    return affectedWFOS



def real_parser(txn, buf):
    buf = buf.replace("\015\015\012", "\n")
    tp = product.TextProduct(buf)
    xtra ={
           'product_id': tp.get_product_id(),
           }
    spc = spcpts.SPCPTS( tp )
    #spc.draw_outlooks()

    # Remove any previous data
    txn.execute("""DELETE from spc_outlooks where valid = %s 
    and expire = %s 
    and outlook_type = %s and day = %s""", (
                    spc.valid, spc.expire, spc.outlook_type, spc.day))

    wfos = {'TSTM': [], 'EXTM': [], 'SLGT': [], 'CRIT': [], 'MDT': [], 
            'HIGH': [] }

    for outlook in spc.outlooks:
        arWFO = consume(txn, spc, outlook)
        if wfos.has_key( outlook.threshold ):
            wfos[ outlook.threshold ].append( arWFO )
    
    if tp.afos == "PTSDY1":
        channelprefix = ""
        product_descript = "Day 1 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day1otlk_%s.html" % (
                                                    spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PTSDY2":
        channelprefix = ""
        product_descript = "Day 2 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day2otlk_%s.html" % (
                                            spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PTSDY3":
        channelprefix = ""
        product_descript = "Day 3 Convective"
        url = "http://www.spc.noaa.gov/products/outlook/archive/%s/day3otlk_%s.html" % (
                                                    spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PTSD48":
        channelprefix = ""
        product_descript = "Day 4-8 Convective"
        url = "http://www.spc.noaa.gov/products/exper/day4-8/archive/%s/day4-8_%s.html" % (
                                                    spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d_%H%M") )
    if tp.afos == "PFWFD1":
        channelprefix = "DAY1FWX"
        product_descript = "Day 1 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d") )
    if tp.afos == "PFWFD2":
        channelprefix = "DAY2FWX"
        product_descript = "Day 2 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d") )
    if tp.afos == "PFWF38":
        channelprefix = "DAY3FWX"
        product_descript = "Day 3-8 Fire"
        url = "http://www.spc.noaa.gov/products/fire_wx/%s/%s.html" % (spc.valid.year, 
                                                    spc.valid.strftime("%Y%m%d") )



    # Only tweet or Jabber for Day1, for now
    if tp.afos not in ('PTSDY1','PFWFD1'):
        return
    
    codes = {'SLGT': "Slight", 'MDT': "Moderate", 'HIGH': 'High', 
             'CRIT': 'Critical', 'EXTM': 'Extreme'}


    msgs = {}
    htmlmsgs = {}
    twts = {}
    for cat in ['SLGT','MDT','HIGH','CRIT','EXTM']:
        for z in wfos[cat]:
            for wfo in z:
                msgs[wfo] = "The Storm Prediction Center issues Day 1 %s risk \
for portions of %s %s" % (cat, wfo, url)
                htmlmsgs[wfo] = "The Storm Prediction Center issues \
<a href=\"%s\">%s %s Risk</a> for portions of %s's area" % (url, \
  product_descript, codes[cat], wfo)
                twts[wfo] = "SPC issues Day 1 %s risk for %s" % (cat, wfo)

    for wfo in msgs.keys():
        xtra['channels'] = wfo
        jabber.sendMessage( msgs[wfo], htmlmsgs[wfo], xtra )

    # Generic for SPC
    xtra['twitter'] = "The Storm Prediction Center issues %s Outlook" % (
                                                            product_descript,)
    jabber.sendMessage(xtra['twitter'], xtra['twitter'], xtra)

jabber = common.make_jabber_client('spc_parser')

ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run()