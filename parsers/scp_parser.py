""" NESDIS SCP Ingestor """

# 3rd Party
from twisted.internet import reactor
from pyiem.nws.products.scp import parser
from pyldm import ldmbridge

# Local
from pywwa import common

DBPOOL = common.get_database("asos", cp_max=1)


def shutdown():
    """Shut things down, please"""
    reactor.callWhenRunning(reactor.stop)  # @UndefinedVariable


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
    prod = parser(raw)
    if common.dbwrite_enabled():
        prod.sql(txn)


def main():
    """Go Main Go"""
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
