"""SPS product ingestor"""
from functools import partial

# 3rd Party
import click
from pyiem.nws.products.sps import parser
from pyiem.nws.ugc import UGCProvider
from pyiem.util import LOG

# Local
from pywwa import common
from pywwa.database import get_database, get_dbconn, load_nwsli
from pywwa.ldm import bridge

NWSLI_DICT = {}


def real_process(ugc_dict, txn, raw):
    """Really process!"""
    if raw.find("$$") == -1:
        LOG.info("$$ was missing from this product")
        raw += "\r\r\n$$\r\r\n"
    prod = parser(
        raw,
        utcnow=common.utcnow(),
        ugc_provider=ugc_dict,
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
    ugc_dict = UGCProvider(pgconn=get_dbconn("postgis"))
    load_nwsli(NWSLI_DICT)
    func = partial(real_process, ugc_dict)
    bridge(func, dbpool=get_database("postgis"))
