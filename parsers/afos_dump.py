""" Twisted Way to dump data to the database """

from twisted.internet import reactor
from txyam.client import YamClient
from pyldm import ldmbridge
from pyiem.util import LOG
from pyiem.nws import product
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


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ called when the connection is lost """
        common.shutdown()

    def process_data(self, data):
        """ Process the product """
        defer = DBPOOL.runInteraction(real_parser, data)
        defer.addCallback(write_memcache)
        defer.addErrback(common.email_error, data)
        defer.addErrback(LOG.error)


def write_memcache(nws):
    """write our TextProduct to memcached"""
    if nws is None:
        return
    # 10 minutes should be enough time
    LOG.debug("writing %s to memcache", nws.get_product_id())
    df = MEMCACHE_CLIENT.set(
        nws.get_product_id().encode("utf-8"),
        nws.unixtext.replace("\001\n", "").encode("utf-8"),
        expireTime=600,
    )
    df.addErrback(LOG.error)


def real_parser(txn, buf):
    """ Actually do something with the buffer, please """
    if buf.strip() == "":
        return None
    utcnow = common.utcnow()

    nws = product.TextProduct(buf, utcnow=utcnow, parse_segments=False)

    # When we are in realtime processing, do not consider old data, typically
    # when a WFO fails to update the date in their MND
    if (utcnow - nws.valid).days > 180 or (utcnow - nws.valid).days < -180:
        raise Exception(f"Very Latent Product! {nws.valid}")

    if nws.afos is None:
        if nws.source[0] not in ["K", "P"]:
            return None
        raise Exception("TextProduct.afos is null")

    txn.execute(
        "INSERT into products (pil, data, entered, "
        "source, wmo) VALUES(%s, %s, %s, %s, %s)",
        (nws.afos.strip(), nws.text, nws.valid, nws.source, nws.wmo),
    )
    if nws.afos[:3] in MEMCACHE_EXCLUDE:
        return None

    return nws


if __name__ == "__main__":
    # Go
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()  # @UndefinedVariable
