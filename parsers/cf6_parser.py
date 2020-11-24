"""Parse CF6 text products."""

from twisted.internet import reactor
from twisted.python import log
from pyldm import ldmbridge
from pyiem.nws.products.cf6 import parser
import common

DBPOOL = common.get_database("iem", cp_max=1)


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ Connection was lost! """
        log.err(reason)
        reactor.callLater(7, reactor.callWhenRunning, reactor.stop)

    def process_data(self, data):
        """ Process the product """
        deffer = DBPOOL.runInteraction(processor, data)
        deffer.addErrback(common.email_error, data)


def processor(txn, text):
    """ Protect the realprocessor """
    prod = parser(text)
    if not common.CTX.disable_dbwrite:
        prod.sql(txn)


if __name__ == "__main__":
    # Do Stuff
    ldmbridge.LDMProductFactory(MyProductIngestor())

    reactor.run()
