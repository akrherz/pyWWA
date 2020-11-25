""" FFG """

# 3rd Party
from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.util import LOG
from pyiem.nws.products.ffg import parser

# Local
from pywwa import common

DBPOOL = common.get_database("postgis")


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """Connection was lost"""
        common.shutdown()

    def process_data(self, data):
        """Process the product"""
        if not common.dbwrite_enabled():
            return
        df = DBPOOL.runInteraction(real_parser, data)
        df.addErrback(common.email_error, data)


def shutdown():
    """Go shutdown"""
    reactor.callWhenRunning(reactor.stop)  # @UndefinedVariable


def real_parser(txn, buf):
    """callback func"""
    ffg = parser(buf)
    if ffg.afos == "FFGMPD":
        return
    ffg.sql(txn)
    if ffg.warnings and ffg.warnings[0].find("termination") == -1:
        common.email_error("\n".join(ffg.warnings), buf)
    sz = 0 if ffg.data is None else len(ffg.data.index)
    LOG.info("FFG found %s entries for product %s", sz, ffg.get_product_id())


def main():
    """Our main method"""
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
