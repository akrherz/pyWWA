""" SPC Watch Ingestor """

# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.saw import parser as sawparser

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge
from pywwa.database import get_database

IEM_URL = common.SETTINGS.get("pywwa_watch_url", "pywwa_watch_url")
JABBER = make_jabber_client()


def real_process(txn, raw):
    """Process the product, please"""
    prod = sawparser(raw)
    if prod.is_test():
        LOG.info("TEST watch found, skipping")
        return
    if common.dbwrite_enabled():
        prod.sql(txn)
    prod.compute_wfos(txn)
    for (txt, html, xtra) in prod.get_jabbers(IEM_URL):
        JABBER.send_message(txt, html, xtra)


def main():
    """Go Main Go"""
    bridge(real_process, dbpool=get_database("postgis"))
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
