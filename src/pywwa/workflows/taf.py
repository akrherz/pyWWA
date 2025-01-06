"""TAF Ingestor"""

from typing import TYPE_CHECKING

import click
from pyiem.nws.products.taf import parser
from twisted.internet import reactor
from twisted.internet.interfaces import IReactorTime

from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

if TYPE_CHECKING:
    reactor: IReactorTime


def real_process(txn, raw):
    """Process the product, please"""
    prod = parser(raw, utcnow=common.utcnow())
    if common.dbwrite_enabled():
        prod.sql(txn)
    baseurl = common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    jmsgs = prod.get_jabbers(baseurl)
    # The downstream workflow needs to have these TAF messages available within
    # the AFOS database, so we add some delay here.
    for mess, htmlmess, xtra in jmsgs:
        reactor.callLater(15, common.send_message, mess, htmlmess, xtra)
    if prod.warnings:
        common.email_error("\n\n".join(prod.warnings), prod.text)


@click.command(help=__doc__)
@common.init
def main(*args, **kwargs):
    """Go Main Go"""
    bridge(real_process, dbpool=get_database("asos"))
