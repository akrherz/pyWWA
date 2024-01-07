"""SPS product ingestor"""
# 3rd Party
import click
from pyiem.nws.products.sps import parser
from pyiem.nws.ugc import UGCProvider
from pyiem.util import LOG
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database, load_nwsli
from pywwa.ldm import bridge

POSTGIS = get_database("postgis")
UGC_DICT = UGCProvider()
NWSLI_DICT = {}


def real_process(txn, raw):
    """Really process!"""
    if raw.find("$$") == -1:
        LOG.info("$$ was missing from this product")
        raw += "\r\r\n$$\r\r\n"
    prod = parser(
        raw,
        utcnow=common.utcnow(),
        ugc_provider=UGC_DICT,
        nwsli_provider=NWSLI_DICT,
    )
    if common.dbwrite_enabled():
        prod.sql(txn)
    baseurl = common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    jmsgs = prod.get_jabbers(baseurl)
    for mess, htmlmess, xtra in jmsgs:
        common.send_message(mess, htmlmess, xtra)
    if prod.warnings:
        common.email_error("\n\n".join(prod.warnings), prod.text)


@click.command()
@common.init
def main(*args, **kwargs):
    """Go Main Go."""
    load_nwsli(NWSLI_DICT)
    bridge(real_process, dbpool=POSTGIS)
    reactor.run()
