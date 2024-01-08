"""SPENES product ingestor"""
# stdlib
import re

import click
from pyiem.nws import product

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


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
            channels.append(f"SPENES.{center}")
    baseurl = common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    xtra = {"product_id": product_id}
    xtra["channels"] = ",".join(channels)
    body = (
        "NESDIS issues Satellite Precipitation Estimates "
        f"{baseurl}?pid={product_id}"
    )
    xtra["twitter"] = body
    htmlbody = (
        f"<p>NESDIS issues <a href='{baseurl}?pid={product_id}'>"
        "Satellite Precipitation Estimates</a></p>"
    )
    common.send_message(body, htmlbody, xtra)


@click.command()
@common.init
def main(*args, **kwargs):
    """Go Main Go."""
    bridge(real_process, dbpool=get_database("postgis"))
