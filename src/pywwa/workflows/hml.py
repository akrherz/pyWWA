""" HML parser! """

# 3rd Party
import click
from pyiem.nws.products.hml import parser as hmlparser
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def real_parser(txn, buf):
    """I'm gonna do the heavy lifting here"""
    prod = hmlparser(buf)
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)


@click.command()
@common.disable_xmpp
@common.init
def main(*args, **kwargs):
    """Go Main Go."""
    bridge(real_parser, dbpool=get_database("hml"))
    reactor.run()  # @UndefinedVariable
