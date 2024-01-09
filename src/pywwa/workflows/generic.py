""" Generic NWS Product Parser """
# stdlib
from datetime import timedelta
from functools import partial

import click
from pyiem.nws.product import TextProduct
from pyiem.nws.products import parser as productparser
from pyiem.nws.ugc import UGCProvider
from shapely.geometry import MultiPolygon

# Local
from pywwa import common
from pywwa.database import get_database, get_dbconn, load_nwsli
from pywwa.ldm import bridge

NWSLI_DICT = {}


def process_data(ugc_dict, txn, buf) -> TextProduct:
    """Actually do some processing"""

    # Create our TextProduct instance
    prod = productparser(
        buf,
        utcnow=common.utcnow(),
        ugc_provider=ugc_dict,
        nwsli_provider=NWSLI_DICT,
    )

    # Do the Jabber work necessary after the database stuff has completed
    for plain, html, xtra in prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    ):
        if xtra.get("channels", "") == "":
            common.email_error("xtra[channels] is empty!", buf)
        common.send_message(plain, html, xtra)

    if not common.dbwrite_enabled():
        return None
    # Insert into database only if there is a polygon!
    if not prod.segments or prod.segments[0].sbw is None:
        return None

    expire = prod.segments[0].ugcexpire
    if expire is None:
        prod.warnings.append("ugcexpire is none, defaulting to 90 minutes.")
        expire = prod.valid + timedelta(minutes=90)
    product_id = prod.get_product_id()
    giswkt = f"SRID=4326;{MultiPolygon([prod.segments[0].sbw]).wkt}"
    sql = (
        "INSERT into text_products(product_id, geom, issue, expire, pil) "
        "values (%s,%s,%s,%s,%s)"
    )
    myargs = (
        product_id,
        giswkt,
        prod.valid,
        expire,
        prod.afos,
    )
    txn.execute(sql, myargs)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)
    return prod


@click.command(help=__doc__)
@common.init
def main(*args, **kwargs):
    """Go Main Go."""
    load_nwsli(NWSLI_DICT)
    ugc_dict = UGCProvider(pgconn=get_dbconn("postgis"))
    func = partial(process_data, ugc_dict)
    bridge(func, dbpool=get_database("postgis"))
