"""Parse CF6 text products."""

# 3rd Party
import click
from pyiem.nws.products.cf6 import parser

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

DBPOOL = get_database("iem")


def processor(txn, text):
    """Protect the realprocessor"""
    prod = parser(text, utcnow=common.utcnow())
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), text)
    return prod


@click.command()
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go Main Go."""
    bridge(processor, dbpool=DBPOOL)
