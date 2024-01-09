""" NLDN """
# stdlib
from functools import partial
from io import BytesIO

import click
from pyiem.nws.products.nldn import parser

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def process_data(dbpool, data):
    """Actual ingestor"""
    if data == b"":
        return
    try:
        real_process(dbpool, data)
    except Exception as myexp:
        common.email_error(myexp, data)


def real_process(dbpool, buf):
    """The real processor of the raw data, fun!"""
    np = parser(BytesIO(b"NLDN" + buf))
    if common.dbwrite_enabled():
        dbpool.runInteraction(np.sql)


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go Main"""
    func = partial(process_data, get_database("nldn"))
    bridge(func, isbinary=True, product_end=b"NLDN")
