"""TAF Ingestor"""

import click
from pyiem.nws.products.taf import parser

from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def real_process(txn, raw):
    """Process the product, please"""
    prod = parser(raw, utcnow=common.utcnow())
    if common.dbwrite_enabled():
        prod.sql(txn)
    baseurl = common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    jmsgs = prod.get_jabbers(baseurl)
    for mess, htmlmess, xtra in jmsgs:
        common.send_message(mess, htmlmess, xtra)
    if prod.warnings:
        common.email_error("\n\n".join(prod.warnings), prod.text)


@click.command(help=__doc__)
@common.init
def main(*args, **kwargs):
    """Go Main Go"""
    bridge(real_process, dbpool=get_database("asos"))
