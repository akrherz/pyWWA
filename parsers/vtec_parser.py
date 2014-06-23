''' 
VTEC product ingestor 

The warnings table has the following timestamp based columns, this gets ugly 
with watches.  Lets try to explain

    issue   <- VTEC timestamp of when this event was valid for
    expire  <- When does this VTEC product expire
    updated <- Product Timestamp of when a product gets updated
    init_expire <- When did this product initially expire
    product_issue <- When was this product issued by the NWS

'''

# Twisted Python imports
from twisted.python import log
from twisted.python import logfile
from twisted.internet import reactor

# Standard Python modules
import re
import datetime
import sys

# third party
import pytz

# pyLDM https://github.com/akrherz/pyLDM
from pyldm import ldmbridge
# pyIEM https://github.com/akrherz/pyIEM
from pyiem.nws.products.vtec import parser as vtecparser
from pyiem.nws import ugc
from pyiem.nws import nwsli

import common

PGCONN = common.get_database("postgis")
ugc_dict = {}
nwsli_dict = {}

JABBER_ON = True
if len(sys.argv) == 2 and sys.argv[1] == 'nojabber':
    log.msg("Disabling Jabber as per command line")
    JABBER_ON = False

def shutdown():
    ''' Stop this app '''
    log.msg("Shutting down...")
    reactor.callWhenRunning(reactor.stop)
    

# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        ''' callback when the stdin reader connection is closed '''
        log.msg('connectionLost() called...')
        log.err( reason )
        reactor.callLater(7, shutdown)

    def process_data(self, buf):
        """ Process the product """
        try:
            really_process_data(buf)
        except Exception, myexp: #pylint: disable=W0703
            common.email_error(myexp, buf) 

def really_process_data(buf):
    ''' Actually do some processing '''
    gmtnow = datetime.datetime.utcnow()
    gmtnow = gmtnow.replace(tzinfo=pytz.timezone("UTC"))
    
    # Make sure we have a trailing $$, if not report error and slap one on
    if buf.find("$$") == -1:
        common.email_error("No $$ Found!", buf)
        buf += "\n\n$$\n\n"
        
    # Create our TextProduct instance
    text_product = vtecparser( buf, utcnow=gmtnow, ugc_provider=ugc_dict,
                               nwsli_provider=nwsli_dict)

    df = PGCONN.runInteraction(text_product.sql)
    df.addCallback(step2, text_product)
    df.addErrback(common.email_error, text_product.unixtext)
    df.addErrback( log.err )   

def step2(dummy, text_product):
    ''' After the SQL is done, lets do other things '''
    if len(text_product.warnings) > 0:
        common.email_error( "\n\n".join(text_product.warnings), 
                            text_product.text)
    
    #Do the Jabber work necessary after the database stuff has completed
    for (plain, html, xtra) in text_product.get_jabbers( 
                    common.settings.get('pywwa_vtec_url', 'pywwa_vtec_url'),
                    common.settings.get('pywwa_river_url', 'pywwa_river_url') ):
        if xtra.get('channels', '') == '':
            common.email_error("xtra[channels] is empty!", text_product.text)
        if JABBER_ON:
            jabber.sendMessage(plain, html, xtra)
    
def load_ugc(txn):
    """ load ugc"""
    sql = """SELECT name, ugc, wfo from ugcs WHERE 
        name IS NOT Null and end_ts is null"""
    txn.execute(sql)
    for row in txn:
        ugc_dict[ row['ugc'] ] = ugc.UGC(row['ugc'][:2], row['ugc'][2],
                        row['ugc'][3:],
                name=(row["name"]).replace("\x92"," ").replace("\xc2"," "),
                wfos=re.findall(r'([A-Z][A-Z][A-Z])',row['wfo']))

    log.msg("ugc_dict loaded %s entries" % (len(ugc_dict),))

    sql = """SELECT nwsli, 
     river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname 
     from hvtec_nwsli"""
    txn.execute( sql )
    for row in txn:
        nwsli_dict[ row['nwsli'] ] = nwsli.NWSLI(row['nwsli'], 
                                name=row['rname'].replace("&"," and "))

    log.msg("nwsli_dict loaded %s entries" % (len(nwsli_dict),))
    
    return None

def ready(dummy):
    ''' cb when our database work is done '''
    ldmbridge.LDMProductFactory( MyProductIngestor() )

def dbload():
    ''' Load up database stuff '''
    df = PGCONN.runInteraction(load_ugc)
    df.addCallback( ready )

if __name__ == '__main__':
    log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
    log.startLogging( logfile.DailyLogFile('vtec_parser.log','logs'))

    # Fire up!
    dbload()
    jabber = common.make_jabber_client('vtec_parser')
    
    reactor.run()
