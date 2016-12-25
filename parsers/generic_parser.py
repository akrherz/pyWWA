""" Generic NWS Product Parser """

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/generic_parser', facility=LOG_LOCAL2)
from twisted.python import log
from twisted.internet import reactor

# Standard Python modules
import re
import datetime
import sys

# third party
import pytz
from shapely.geometry import MultiPolygon

# pyLDM https://github.com/akrherz/pyLDM
from pyldm import ldmbridge
# pyIEM https://github.com/akrherz/pyIEM
from pyiem.nws.products import parser as productparser
from pyiem.nws import ugc
from pyiem.nws import nwsli

import common
DB_ON = bool(common.settings.get('pywwa_save_text_products', False))

ugc_dict = {}
nwsli_dict = {}


def shutdown():
    ''' Stop this app '''
    log.msg("Shutting down...")
    reactor.callWhenRunning(reactor.stop)


def error_wrapper(exp, buf):
    """Don't whine about known invalid products"""
    if buf.find("HWOBYZ") > -1:
        log.msg("Skipping Error for HWOBYZ")
        return
    common.email_error(exp, buf)


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        ''' callback when the stdin reader connection is closed '''
        log.msg('connectionLost() called...')
        log.err(reason)
        reactor.callLater(7, shutdown)

    def process_data(self, buf):
        """ Process the product """
        defer = PGCONN.runInteraction(really_process_data, buf)
        defer.addErrback(error_wrapper, buf)
        defer.addErrback(log.err)


def really_process_data(txn, buf):
    ''' Actually do some processing '''
    utcnow = datetime.datetime.utcnow()
    utcnow = utcnow.replace(tzinfo=pytz.timezone("UTC"))

    # Create our TextProduct instance
    prod = productparser(buf, utcnow=utcnow, ugc_provider=ugc_dict,
                         nwsli_provider=nwsli_dict)

    # Do the Jabber work necessary after the database stuff has completed
    for (plain, html, xtra) in prod.get_jabbers(
            common.settings.get('pywwa_product_url', 'pywwa_product_url')):
        if xtra.get('channels', '') == '':
            common.email_error("xtra[channels] is empty!", buf)
        if not MANUAL:
            jabber.sendMessage(plain, html, xtra)

    if DB_ON:
        # Insert into database
        product_id = prod.get_product_id()
        sqlraw = buf.replace("\015\015\012", "\n").replace("\000", "").strip()
        sql = """
        INSERT into text_products(product, product_id) values (%s,%s)
        """
        myargs = (sqlraw, product_id)
        if (len(prod.segments) > 0 and prod.segments[0].sbw):
            giswkt = ('SRID=4326;%s'
                      ) % (MultiPolygon([prod.segments[0].sbw]).wkt, )
            sql = """
                INSERT into text_products(product, product_id, geom)
                values (%s,%s,%s)
            """
            myargs = (sqlraw, product_id, giswkt)
        txn.execute(sql, myargs)


def load_ugc(txn):
    """ load ugc"""
    sql = """SELECT name, ugc, wfo from ugcs WHERE
        name IS NOT Null and end_ts is null"""
    txn.execute(sql)
    for row in txn:
        nm = (row["name"]).replace("\x92", " ").replace("\xc2", " ")
        wfos = re.findall(r'([A-Z][A-Z][A-Z])', row['wfo'])
        ugc_dict[row['ugc']] = ugc.UGC(row['ugc'][:2], row['ugc'][2],
                                       row['ugc'][3:],
                                       name=nm,
                                       wfos=wfos)

    log.msg("ugc_dict loaded %s entries" % (len(ugc_dict),))

    sql = """SELECT nwsli,
     river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname
     from hvtec_nwsli"""
    txn.execute(sql)
    for row in txn:
        nm = row['rname'].replace("&", " and ")
        nwsli_dict[row['nwsli']] = nwsli.NWSLI(row['nwsli'], name=nm)

    log.msg("nwsli_dict loaded %s entries" % (len(nwsli_dict),))

    return None


def ready(dummy):
    ''' cb when our database work is done '''
    ldmbridge.LDMProductFactory(MyProductIngestor())


def dbload():
    ''' Load up database stuff '''
    df = PGCONN.runInteraction(load_ugc)
    df.addCallback(ready)

if __name__ == '__main__':

    MANUAL = False
    if len(sys.argv) == 2 and sys.argv[1] == 'manual':
        log.msg("Manual runtime (no jabber, 1 database connection) requested")
        MANUAL = True

    # Fire up!
    PGCONN = common.get_database("postgis", cp_max=(5 if not MANUAL else 1))
    dbload()
    jabber = common.make_jabber_client('generic_parser')

    reactor.run()
