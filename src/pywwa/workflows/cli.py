"""Parse CLI text products

The CLI report has lots of good data that is hard to find in other products,
so we take what data we find in this product and overwrite the database
storage of what we got from the automated observations
"""
from functools import partial

# 3rd Party
import click
from pyiem.network import Table as NetworkTable
from pyiem.nws.products import parser
from pyiem.util import LOG
from twisted.internet import reactor, task

# Local
from pywwa import common
from pywwa.database import get_database, get_dbconnc
from pywwa.ldm import bridge


def processor(nt, txn, text):
    """Protect the realprocessor"""
    prod = parser(text, nwsli_provider=nt.sts, utcnow=common.utcnow())
    # Run through database save now
    prod.sql(txn)
    jres = prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    )
    for j in jres:
        common.send_message(j[0], j[1], j[2])
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), text)
    return prod


def ready(nt):
    """We are ready."""
    func = partial(processor, nt)
    bridge(func, dbpool=get_database("iem"))


def load_stations():
    """load the stations and then fire the ingest."""
    # Can't use cursor= as it isn't an interator :(
    conn, cursor = get_dbconnc("mesosite")
    nt = NetworkTable("NWSCLI", only_online=False, cursor=cursor)
    LOG.info("Found %s NWSCLI stations", len(nt.sts))
    cursor.close()
    conn.close()
    return nt


@click.command()
@common.init
def main(*args, **kwargs):
    """Go Main Go."""
    print("DARYL")
    df = task.deferLater(reactor, 0, load_stations)
    df.addCallback(ready)
    df.addErrback(common.email_error)
