""" NESDIS SCP Ingestor """

# 3rd Party
import click
from pyiem.nws.products.scp import parser

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def real_process(txn, raw):
    """Process the product, please"""
    prod = parser(raw, utcnow=common.utcnow())
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), raw)


@click.command()
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go Main Go"""
    bridge(real_process, dbpool=get_database("asos"))
