"""Parse FD text products."""

# 3rd Party
import click
from pyiem.nws.products.fd import parser

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def processor(txn, text):
    """Protect the realprocessor"""
    prod = parser(text, utcnow=common.utcnow())
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), text)
    return prod


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go Main Go."""
    bridge(processor, dbpool=get_database("asos", cp_max=2))
