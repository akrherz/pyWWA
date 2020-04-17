""" Twisted Way to dump data to the database """
import sys
import datetime

import pytz
from twisted.python import log
from twisted.internet import reactor
from txyam.client import YamClient
from pyldm import ldmbridge
from pyiem.nws import product
from pyiem.util import utc
import common  # @UnresolvedImport

DBPOOL = common.get_database("afos")
MEMCACHE_EXCLUDE = [
    "RR1",
    "RR2",
    "RR3",
    "RR4",
    "RR5",
    "RR6",
    "RR7",
    "RR8",
    "RR9",
    "ROB",
    "HML",
]
MEMCACHE_CLIENT = YamClient(reactor, ["tcp:iem-memcached3:11211"])
MEMCACHE_CLIENT.connect()


def shutdown():
    """ Down we go! """
    log.msg("Stopping...")
    reactor.callWhenRunning(reactor.stop)  # @UndefinedVariable


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ called when the connection is lost """
        log.msg("connectionLost")
        log.err(reason)
        reactor.callLater(5, shutdown)  # @UndefinedVariable

    def process_data(self, data):
        """ Process the product """
        defer = DBPOOL.runInteraction(real_parser, data)
        defer.addCallback(write_memcache)
        defer.addErrback(common.email_error, data)
        defer.addErrback(log.err)


class ParseError(Exception):
    """ general exception """

    pass


def write_memcache(nws):
    """write our TextProduct to memcached"""
    if nws is None:
        return
    # 10 minutes should be enough time
    # log.msg("writing %s to memcache" % (nws.get_product_id(), ))
    df = MEMCACHE_CLIENT.set(
        nws.get_product_id().encode("utf-8"),
        nws.unixtext.replace("\001\n", "").encode("utf-8"),
        expireTime=600,
    )
    df.addErrback(log.err)


def real_parser(txn, buf):
    """ Actually do something with the buffer, please """
    if buf.strip() == "":
        return
    utcnow = datetime.datetime.utcnow()
    utcnow = utcnow.replace(tzinfo=pytz.utc)

    nws = product.TextProduct(buf, utcnow=UTCNOW, parse_segments=False)

    # When we are in realtime processing, do not consider old data, typically
    # when a WFO fails to update the date in their MND
    if not MANUAL and (
        (utcnow - nws.valid).days > 180 or (utcnow - nws.valid).days < -180
    ):
        raise ParseError("Very Latent Product! %s" % (nws.valid,))

    if nws.afos is None:
        if MANUAL or nws.source[0] not in ["K", "P"]:
            return
        raise ParseError("TextProduct.afos is null")

    # Run the database transaction
    if MANUAL:
        txn.execute(
            "SELECT * from products WHERE "
            "pil = %s and entered = %s and source = %s and wmo = %s ",
            (nws.afos.strip(), nws.valid, nws.source, nws.wmo),
        )
        if txn.rowcount == 1:
            log.msg("Duplicate: %s" % (nws.get_product_id(),))
            return
    txn.execute(
        "INSERT into products (pil, data, entered, "
        "source, wmo) VALUES(%s, %s, %s, %s, %s)",
        (nws.afos.strip(), nws.text, nws.valid, nws.source, nws.wmo),
    )
    if MANUAL or nws.afos[:3] in MEMCACHE_EXCLUDE:
        return

    return nws


if __name__ == "__main__":
    # Go
    MANUAL = False
    UTCNOW = None
    if len(sys.argv) == 6 and sys.argv[1] == "manual":
        MANUAL = True
        UTCNOW = utc(
            int(sys.argv[2]),
            int(sys.argv[3]),
            int(sys.argv[4]),
            int(sys.argv[5]),
        )
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()  # @UndefinedVariable
