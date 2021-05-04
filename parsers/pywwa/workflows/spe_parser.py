"""SPENES product ingestor"""
# stdlib
import re

# 3rd Party
from twisted.internet import reactor
from pyiem.nws import product

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import get_database


def real_process(txn, raw):
    """Do work please"""
    sqlraw = raw.replace("\015\015\012", "\n")
    prod = product.TextProduct(raw, utcnow=common.utcnow())

    product_id = prod.get_product_id()
    if common.dbwrite_enabled():
        txn.execute(
            "INSERT into text_products(product, product_id) values (%s,%s)",
            (sqlraw, product_id),
        )

    tokens = re.findall("ATTN (WFOS|RFCS)(.*)", raw)
    channels = []
    for tpair in tokens:
        for center in re.findall(r"([A-Z]+)\.\.\.", tpair[1]):
            channels.append("SPENES.%s" % (center,))
    baseurl = common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    xtra = {"product_id": product_id}
    xtra["channels"] = ",".join(channels)
    xtra["twitter"] = (
        "NESDIS issues Satellite Precipitation " "Estimates %s?pid=%s"
    ) % (baseurl, product_id)

    body = ("NESDIS issues Satellite Precipitation Estimates %s?pid=%s") % (
        baseurl,
        product_id,
    )
    htmlbody = (
        "<p>NESDIS issues "
        "<a href='%s?pid=%s'>Satellite Precipitation Estimates</a>"
        "</p>"
    ) % (baseurl, product_id)
    common.send_message(body, htmlbody, xtra)


def main():
    """Go Main Go."""
    common.main()
    bridge(real_process, dbpool=get_database("postgis"))
    reactor.run()


if __name__ == "__main__":
    main()
