""" XTEUS Product Parser! """
# 3rd Party
import click
from pyiem.nws.products.xteus import parser

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def process_data(txn, data):
    """Process the product"""
    prod = parser(data, utcnow=common.utcnow())
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), data)
    prod.sql(txn)
    return prod


@click.command(help=__doc__)
@common.init
def main(*args, **kwargs):
    """Fire things up."""
    bridge(process_data, dbpool=get_database("iem"))
