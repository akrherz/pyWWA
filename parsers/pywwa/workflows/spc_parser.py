"""SPC Geo Products Parser!"""

# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.spcpts import parser

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import get_database

WAITFOR = 20


def real_parser(txn, buf):
    """Actually process"""
    prod = parser(buf)
    # spc.draw_outlooks()
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)
    return prod


def do_jabber(prod):
    """Callback after database work is done."""
    jmsgs = prod.get_jabbers("")
    for (txt, html, xtra) in jmsgs:
        common.send_message(txt, html, xtra)
    LOG.info(
        "Sent %s messages for product %s", len(jmsgs), prod.get_product_id()
    )


def main():
    """Go Main Go."""
    common.main()
    bridge(real_parser, dbpool=get_database("postgis"), cb2=do_jabber)
    reactor.run()


if __name__ == "__main__":
    main()
