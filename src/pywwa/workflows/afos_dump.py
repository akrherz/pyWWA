"""AFOS Database Workflow."""
# 3rd Party
import click
from pyiem.nws import product
from pyiem.util import LOG
from twisted.internet import reactor
from txyam.client import YamClient

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

DBPOOL = get_database("afos", cp_max=5)
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
MEMCACHE_CLIENT = YamClient(reactor, ["tcp:iem-memcached:11211"])
MEMCACHE_CLIENT.connect()


def process_data(data):
    """Process the product"""
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
    """Actually do something with the buffer, please"""
    if buf.strip() == "":
        return None
    utcnow = common.utcnow()

    nws = product.TextProduct(buf, utcnow=utcnow, parse_segments=False)

    # When we are in realtime processing, do not consider old data, typically
    # when a WFO fails to update the date in their MND
    if not common.replace_enabled():
        delta = (nws.valid - utcnow).total_seconds()
        if delta < (-180 * 86400):  # 180 days
            raise Exception(f"Very Latent Product! {nws.valid}")
        if delta > (6 * 3600):  # Six Hours
            raise Exception(f"Product from the future! {nws.valid}")
    if nws.warnings:
        common.email_error("\n".join(nws.warnings), buf)
    if nws.afos is None:
        if nws.source[0] not in ["K", "P"]:
            return None
        raise Exception("TextProduct.afos is null")

    if common.replace_enabled():
        args = [nws.afos.strip(), nws.source, nws.valid]
        bbb = ""
        if nws.bbb:
            bbb = " and bbb = %s "
            args.append(nws.bbb)
        txn.execute(
            "DELETE from products where pil = %s and source = %s and "
            f"entered = %s {bbb}",
            args,
        )
        LOG.info("Removed %s rows for %s", txn.rowcount, nws.get_product_id())

    txn.execute(
        "INSERT into products (pil, data, entered, "
        "source, wmo, bbb) VALUES(%s, %s, %s, %s, %s, %s)",
        (nws.afos.strip(), nws.text, nws.valid, nws.source, nws.wmo, nws.bbb),
    )
    if nws.afos[:3] in MEMCACHE_EXCLUDE:
        return None

    return nws


@click.command()
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Fire up our workflow."""
    bridge(process_data)
    reactor.run()  # @UndefinedVariable
