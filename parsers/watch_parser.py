""" SPC Watch Ingestor """

# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.saw import parser as sawparser
from pyldm import ldmbridge

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client

DBPOOL = common.get_database("postgis")
IEM_URL = common.SETTINGS.get("pywwa_watch_url", "pywwa_watch_url")
JABBER = make_jabber_client()


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """STDIN is shut, so lets shutdown"""
        common.shutdown()

    def process_data(self, data):
        """Process the product!"""
        df = DBPOOL.runInteraction(real_process, data)
        df.addErrback(common.email_error, data)


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
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
