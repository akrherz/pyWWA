"""FFG"""

# 3rd Party
import click
from pyiem.nws.products.ffg import parser

# Local
from pywwa import LOG, common
from pywwa.database import get_database
from pywwa.ldm import bridge


def real_parser(txn, buf):
    """callback func"""
    ffg = parser(buf)
    if ffg.afos == "FFGMPD":
        return
    if common.dbwrite_enabled():
        ffg.sql(txn)
    if ffg.warnings and ffg.warnings[0].find("termination") == -1:
        common.email_error("\n".join(ffg.warnings), buf)
    sz = 0 if ffg.data is None else len(ffg.data.index)
    LOG.info("FFG found %s entries for product %s", sz, ffg.get_product_id())


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Our main method"""
    bridge(real_parser, dbpool=get_database("postgis"))
