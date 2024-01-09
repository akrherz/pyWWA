""" MOS Data Ingestor, why not? """
from functools import partial

# 3rd Party
import click
from pyiem.nws.products.mos import parser

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

MEMORY = {"ingested": 0}


def process_data(data):
    """
    Actual ingestor
    """
    try:
        real_process(data)
    except Exception as myexp:
        common.email_error(myexp, data)


def got_data(res):
    """Callback from the database save"""
    MEMORY["ingested"] += res


def real_process(dbpool, text):
    """The real processor of the raw data, fun!"""
    prod = parser(text, utcnow=common.utcnow())
    if not common.dbwrite_enabled():
        return
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), text)
    df = dbpool.runInteraction(prod.sql)
    df.addCallback(got_data)
    df.addErrback(common.email_error, text)


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go Main Go."""
    func = partial(process_data, get_database("mos", cp_max=5))
    bridge(func)
