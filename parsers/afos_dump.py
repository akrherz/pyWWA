""" Twisted Way to dump data to the database """
from syslog import LOG_LOCAL2
import sys
import datetime

import pytz
from twisted.python import syslog
from twisted.python import log
from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.nws import product
from pyiem.util import utc
import common  # @UnresolvedImport

syslog.startLogging(prefix='pyWWA/afos_dump', facility=LOG_LOCAL2)

DBPOOL = common.get_database('afos')


def shutdown():
    """ Down we go! """
    log.msg("Stopping...")
    reactor.callWhenRunning(reactor.stop)  # @UndefinedVariable


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ called when the connection is lost """
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, shutdown)  # @UndefinedVariable

    def process_data(self, data):
        """ Process the product """
        defer = DBPOOL.runInteraction(real_parser, data)
        defer.addErrback(common.email_error, data)
        defer.addErrback(log.err)


class ParseError(Exception):
    """ general exception """
    pass


def real_parser(txn, buf):
    """ Actually do something with the buffer, please """
    if buf.strip() == "":
        return
    utcnow = datetime.datetime.utcnow()
    utcnow = utcnow.replace(tzinfo=pytz.utc)

    nws = product.TextProduct(buf, utcnow=UTCNOW, parse_segments=False)

    # When we are in realtime processing, do not consider old data, typically
    # when a WFO fails to update the date in their MND
    if not MANUAL and ((utcnow - nws.valid).days > 180 or
                       (utcnow - nws.valid).days < -180):
        raise ParseError("Very Latent Product! %s" % (nws.valid,))

    if nws.valid.month > 6:
        table = "products_%s_0712" % (nws.valid.year,)
    else:
        table = "products_%s_0106" % (nws.valid.year,)

    if nws.afos is None:
        if MANUAL or nws.source[0] not in ['K', 'P']:
            return
        raise ParseError("TextProduct.afos is null")

    # Run the database transaction
    if MANUAL:
        txn.execute("""
            SELECT * from """+table+""" WHERE
            pil = %s and entered = %s and source = %s and wmo = %s
        """, (nws.afos.strip(), nws.valid, nws.source, nws.wmo))
        if txn.rowcount == 1:
            log.msg("Duplicate: %s" % (nws.get_product_id(),))
            return
    txn.execute("""
        INSERT into """+table+"""(pil, data, entered,
        source, wmo) VALUES(%s, %s, %s, %s, %s)
    """, (nws.afos.strip(), nws.text, nws.valid, nws.source, nws.wmo))


if __name__ == '__main__':
    # Go
    MANUAL = False
    UTCNOW = None
    if len(sys.argv) == 6 and sys.argv[1] == 'manual':
        MANUAL = True
        UTCNOW = utc(int(sys.argv[2]), int(sys.argv[3]),
                     int(sys.argv[4]), int(sys.argv[5]))
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()  # @UndefinedVariable
