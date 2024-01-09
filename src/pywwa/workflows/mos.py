""" MOS Data Ingestor, why not? """
# 3rd Party
import click
from pyiem.nws.products.mos import parser

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def process_data(txn, text):
    """The real processor of the raw data, fun!"""
    prod = parser(text, utcnow=common.utcnow())
    if not common.dbwrite_enabled():
        return
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), text)
    prod.sql(txn)


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go Main Go."""
    bridge(process_data, dbpool=get_database("mos", cp_max=5))
