"""SPENES product ingestor"""
# stdlib
import re

# 3rd Party
from twisted.internet import reactor
from pyiem.nws import product

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge
from pywwa.database import get_database

PYWWA_PRODUCT_URL = common.SETTINGS.get(
    "pywwa_product_url", "pywwa_product_url"
)
JABBER = make_jabber_client()


def real_process(txn, raw):
    """Do work please"""
    sqlraw = raw.replace("\015\015\012", "\n")
    prod = product.TextProduct(raw)

    product_id = prod.get_product_id()
    if common.dbwrite_enabled():
        sql = """
            INSERT into text_products(product, product_id) values (%s,%s)
        """
        myargs = (sqlraw, product_id)
        txn.execute(sql, myargs)

    tokens = re.findall("ATTN (WFOS|RFCS)(.*)", raw)
    channels = []
    for tpair in tokens:
        for center in re.findall(r"([A-Z]+)\.\.\.", tpair[1]):
            channels.append("SPENES.%s" % (center,))
    xtra = {"product_id": product_id}
    xtra["channels"] = ",".join(channels)
    xtra["twitter"] = (
        "NESDIS issues Satellite Precipitation " "Estimates %s?pid=%s"
    ) % (PYWWA_PRODUCT_URL, product_id)

    body = ("NESDIS issues Satellite Precipitation Estimates %s?pid=%s") % (
        PYWWA_PRODUCT_URL,
        product_id,
    )
    htmlbody = (
        "<p>NESDIS issues "
        "<a href='%s?pid=%s'>Satellite Precipitation Estimates</a>"
        "</p>"
    ) % (PYWWA_PRODUCT_URL, product_id)
    JABBER.send_message(body, htmlbody, xtra)


def killer():
    """Stop Right Now!"""
    reactor.stop()


def main():
    """Go Main Go."""
    bridge(real_process, dbpool=get_database("postgis"))
    reactor.run()


if __name__ == "__main__":
    main()
