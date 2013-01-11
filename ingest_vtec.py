"""
  Ingest VTEC information
"""
import os
import datetime

from pyiem.nws import product
from pyldm import ldmbridge
import common
import iemtz

from twisted.internet import reactor
from twisted.python import log, logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('vtec.log','logs'))
from twisted.enterprise import adbapi

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

DBPOOL = adbapi.ConnectionPool("twistedpg", database="postgis", 
                                cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password')) 

class UGC(object):
    
    def __init__(self, dbid, ugc, name):
        self.dbid = dbid
        self.ugc = ugc
        self.name = name

# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        reactor.callLater(7, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)

    def process_data(self, buf):
        """ Process the product """
        try:
            text_product = product.TextProduct( buf )
        except Exception, myexp:
            common.email_error(myexp, buf)
            return
        
        df = DBPOOL.runInteraction(process, text_product)            
        df.addErrback( log.err )

def get_vtec_event(txn, text_product, vtec):
    """ Figure out if we already have a VTEC event for this VTEC """
    
    txn.execute("""
    SELECT id, first_product from vtec_events WHERE year = %s and class = %s 
    and phenomena = %s and eventid = %s and significance = %s and center = %s
    """, (text_product.issueTime.year, vtec.status, vtec.phenomena, 
          vtec.ETN, vtec.significance, vtec.office4))
    
    if txn.rowcount == 1 and vtec.action in ['NEW', 'UPG']:
        # Be careful here
        row = txn.fetchone()
        prevts = datetime.datetime.strptime(row['first_product'][:12],
                                            '%Y%m%d%H%M')
        prevts = prevts.replace(tzinfo=iemtz.UTC())
        timediff = (text_product.issueTime - prevts)
        if timediff.days > 30 or timediff.days < -30:
            log.msg("FATAL EventID Conflict!")
            return None
        return row['id']
    
    if txn.rowcount == 0 and vtec.action not in ['NEW', 'UPG']:
        # Lets go look at last year
        txn.execute("""
        SELECT id, first_product from vtec_events WHERE year = %s and class = %s 
        and phenomena = %s and eventid = %s and significance = %s and center = %s
        """, (text_product.issueTime.year -1, vtec.status, vtec.phenomena, 
          vtec.ETN, vtec.significance, vtec.office4))
        if txn.rowcount == 1:
            row = txn.fetchone()
            prevts = datetime.datetime.strptime(row['first_product'][:12],
                                            '%Y%m%d%H%M')
            prevts = prevts.replace(tzinfo=iemtz.UTC())
            timediff = (text_product.issueTime - prevts)
            if timediff.days < 30:
                return row['id']
        return None
    
    if txn.rowcount == 0 and vtec.action in ['NEW', 'UPG']:
        # Add an entry
        txn.execute("""INSERT into vtec_events(year, class, phenomena, eventid,
            significance, center, first_product) VALUES (%s,%s,%s,%s,%s,%s,%s) 
            RETURNING id""", 
            (text_product.issueTime.year, vtec.status, vtec.phenomena, 
             vtec.ETN, vtec.significance, vtec.office4,
             text_product.get_product_id() ))
        row = txn.fetchone()
        log.msg("Adding VTEC_EVENTS %s %s" % (vtec, row['id']))
        return row['id']
    
    if txn.rowcount == 1:
        row = txn.fetchone()
        return row['id']
    
    return None

def process(txn, text_product):
    """ With a database transaction, lets process this text_product! """
    
    for segment in text_product.segments:
        for vtec in segment.vtec:
            vtec_event = get_vtec_event(txn, text_product, vtec)
            if vtec_event is None:
                log.msg("DANGER! Missing vtec_event for %s" % (vtec,))
                continue
            log.msg("vtec: %s dbid: %s" % (vtec, vtec_event ))
            for ugc in segment.ugc:
                if UGCS.get(ugc) is None:
                    log.msg("Unknown UGC: %s" % (ugc,))
                    continue
                u = UGCS[ugc]
                txn.execute(""" INSERT INTO vtec_ugc_log (vtec_event, ugc, 
                    begints, endts, status, product) VALUES (%s, %s,%s,%s,%s,%s)
                    """,
                    (vtec_event, u.dbid, vtec.beginTS, vtec.endTS, 
                     vtec.action, text_product.get_product_id() ))
                if vtec.action in ['NEW', 'UPG', 'EXA', 'EXB']:
                    txn.execute(""" INSERT INTO vtec_ugc_status (vtec_event, ugc, 
                        begints, endts, status, product) VALUES (%s, %s,%s,%s,%s,%s)
                        """,
                        (vtec_event, u.dbid, vtec.beginTS, vtec.endTS, 
                         vtec.action, text_product.get_product_id() ))
                else:
                    txn.execute(""" UPDATE vtec_ugc_status SET begints = %s, 
                    endts = %s, status = %s, product = %s WHERE
                    vtec_event = %s and ugc = %s RETURNING vtec_event
                    """,
                    (vtec.beginTS, vtec.endTS, 
                     vtec.action, text_product.get_product_id(), vtec_event,
                     u.dbid ))
                    if txn.rowcount == 0:
                        log.msg("No rows found vtec: %s ugc: %s" % (vtec, ugc))
UGCS = {}        
def load_database(txn):
    txn.execute("""SELECT id, ugc, name from ugc """)
    for row in txn:
        UGCS[ row['ugc'] ] = UGC(row['id'], row['ugc'], row['name'])
        
def ready( foo ):
    ldmbridge.LDMProductFactory( MyProductIngestor() )

df = DBPOOL.runInteraction(load_database)
df.addCallback( ready )
df.addErrback( log.err )

reactor.run()