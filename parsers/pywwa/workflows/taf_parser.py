""" TAF Ingestor """

# 3rd Party
from twisted.internet import reactor
from pyiem.nws.products.taf import parser

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import get_database
from pywwa.xmpp import make_jabber_client

JABBER = make_jabber_client()
PROD_URL = common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")


def real_process(txn, raw):
    """Process the product, please"""
    prod = parser(raw)
    if common.dbwrite_enabled():
        prod.sql(txn)
    jmsgs = prod.get_jabbers(PROD_URL)
    for (mess, htmlmess, xtra) in jmsgs:
        JABBER.send_message(mess, htmlmess, xtra)
    if prod.warnings:
        common.email_error("\n\n".join(prod.warnings), prod.text)


def main():
    """Go Main Go"""
    bridge(real_process, dbpool=get_database("asos"))
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
