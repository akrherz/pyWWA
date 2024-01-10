""" NLDN """
# stdlib
from io import BytesIO

import click
from pyiem.nws.products.nldn import parser

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def process_data(txn, buf):
    """The real processor of the raw data, fun!"""
    if buf == b"":
        return None
    prod = parser(BytesIO(b"NLDN" + buf))
    if common.dbwrite_enabled():
        prod.sql(txn)
    return prod


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go Main"""
    bridge(
        process_data,
        isbinary=True,
        product_end=b"NLDN",
        dbpool=get_database("nldn"),
    )
