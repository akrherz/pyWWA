"""
VTEC product ingestor

The warnings table has the following timestamp based columns, this gets ugly
with watches.  Lets try to explain

    issue   <- VTEC timestamp of when this event was valid for
    expire  <- When does this VTEC product expire
    updated <- Product Timestamp of when a product gets updated
    init_expire <- When did this product initially expire
    product_issue <- When was this product issued by the NWS
"""
import re
import datetime
import sys

import pytz
from twisted.python import log
from twisted.internet import reactor
from twisted.web.client import HTTPClientFactory
from twisted.mail.smtp import SMTPSenderFactory
from pyldm import ldmbridge
from pyiem.nws.products.vtec import parser as vtecparser
from pyiem.nws import ugc
from pyiem.nws import nwsli
import common


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
        log.err(reason)
        reactor.callLater(7, shutdown)

    def process_data(self, data):
        """ Process the product """
        try:
            really_process_data(data)
        except Exception as myexp:  # pylint: disable=W0703
            common.email_error(myexp, data)


def really_process_data(buf):
    ''' Actually do some processing '''
    gmtnow = datetime.datetime.utcnow()
    gmtnow = gmtnow.replace(tzinfo=pytz.utc)

    # Make sure we have a trailing $$, if not report error and slap one on
    if buf.find("$$") == -1:
        common.email_error("No $$ Found!", buf)
        buf += "\n\n$$\n\n"

    # Create our TextProduct instance
    text_product = vtecparser(buf, utcnow=gmtnow, ugc_provider=ugc_dict,
                              nwsli_provider=nwsli_dict)
    # Don't parse these as they contain duplicated information
    if text_product.source == 'KNHC' and text_product.afos[:3] == 'TCV':
        return
    # Skip spanish products
    if text_product.source == 'TJSJ' and text_product.afos[3:] == 'SPN':
        return

    df = PGCONN.runInteraction(text_product.sql)
    df.addCallback(step2, text_product)
    df.addErrback(common.email_error, text_product.unixtext)
    df.addErrback(log.err)


def step2(_dummy, text_product):
    """After the SQL is done, lets do other things"""
    if text_product.warnings:
        common.email_error("\n\n".join(text_product.warnings),
                           text_product.text)

    # Do the Jabber work necessary after the database stuff has completed
    for (plain, html, xtra) in text_product.get_jabbers(
            common.SETTINGS.get('pywwa_vtec_url', 'pywwa_vtec_url'),
            common.SETTINGS.get('pywwa_river_url', 'pywwa_river_url')):
        if xtra.get('channels', '') == '':
            common.email_error("xtra[channels] is empty!", text_product.text)
        if not MANUAL:
            jabber.send_message(plain, html, xtra)


def load_ugc(txn):
    """ load ugc"""
    # Careful here not to load things from the future
    txn.execute("""
        SELECT name, ugc, wfo from ugcs WHERE
        name IS NOT Null and begin_ts < now() and
        (end_ts is null or end_ts > now())
    """)
    for row in txn.fetchall():
        nm = (row["name"]).replace("\x92", " ").replace("\xc2", " ")
        wfos = re.findall(r'([A-Z][A-Z][A-Z])', row['wfo'])
        ugc_dict[row['ugc']] = ugc.UGC(row['ugc'][:2], row['ugc'][2],
                                       row['ugc'][3:],
                                       name=nm,
                                       wfos=wfos)

    log.msg("ugc_dict loaded %s entries" % (len(ugc_dict),))

    sql = """
     SELECT nwsli,
     river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname
     from hvtec_nwsli
    """
    txn.execute(sql)
    for row in txn.fetchall():
        nm = row['rname'].replace("&", " and ")
        nwsli_dict[row['nwsli']] = nwsli.NWSLI(row['nwsli'],
                                               name=nm)

    log.msg("nwsli_dict loaded %s entries" % (len(nwsli_dict),))

    return None


def ready(_dummy):
    ''' cb when our database work is done '''
    ldmbridge.LDMProductFactory(MyProductIngestor(dedup=True))


def bootstrap():
    """Things to do at startup"""
    df = PGCONN.runInteraction(load_ugc)
    df.addCallback(ready)
    df.addErrback(common.email_error, "load_ugc failure!")


if __name__ == '__main__':
    HTTPClientFactory.noisy = False
    SMTPSenderFactory.noisy = False
    ugc_dict = {}
    nwsli_dict = {}

    MANUAL = False
    if len(sys.argv) == 2 and sys.argv[1] == 'manual':
        log.msg("Manual runtime (no jabber, 1 database connection) requested")
        MANUAL = True

    # Fire up!
    PGCONN = common.get_database(common.CONFIG['databaserw']['postgis'],
                                 cp_max=(5 if not MANUAL else 1))
    bootstrap()
    jabber = common.make_jabber_client('vtec_parser')

    reactor.run()
