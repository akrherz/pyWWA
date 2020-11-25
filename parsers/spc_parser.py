"""SPC Geo Products Parser!"""

# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.spcpts import parser

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge

WAITFOR = 20
JABBER = make_jabber_client()


def real_parser(txn, buf):
    """Actually process"""
    spc = parser(buf)
    # spc.draw_outlooks()
    spc.compute_wfos(txn)
    if common.dbwrite_enabled():
        spc.sql(txn)
    jmsgs = spc.get_jabbers("")
    for (txt, html, xtra) in jmsgs:
        JABBER.send_message(txt, html, xtra)
    LOG.info(
        "Sent %s messages for product %s", len(jmsgs), spc.get_product_id()
    )


if __name__ == "__main__":
    bridge(real_parser, dbpool=common.get_database("postgis"))
    reactor.run()  # @UndefinedVariable
