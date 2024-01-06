"""Parse CLI text products

The CLI report has lots of good data that is hard to find in other products,
so we take what data we find in this product and overwrite the database
storage of what we got from the automated observations
"""

# 3rd Party
import click
from pyiem.network import Table as NetworkTable
from pyiem.nws.products import parser
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

DBPOOL = get_database("iem")
NT = NetworkTable("NWSCLI", only_online=False)


def processor(txn, text):
    """Protect the realprocessor"""
    prod = parser(text, nwsli_provider=NT.sts, utcnow=common.utcnow())
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


@click.command()
@common.init
def main(*args, **kwargs):
    """Go Main Go."""
    bridge(processor, dbpool=DBPOOL)
    reactor.run()
