"""Parse CF6 text products."""

from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.nws.products.cf6 import parser
import common

DBPOOL = common.get_database("iem", cp_max=1)


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ Connection was lost! """
        common.shutdown()

    def process_data(self, data):
        """ Process the product """
        deffer = DBPOOL.runInteraction(processor, data)
        deffer.addErrback(common.email_error, data)


def processor(txn, text):
    """ Protect the realprocessor """
    prod = parser(text)
    if common.dbwrite_enabled():
        prod.sql(txn)


if __name__ == "__main__":
    # Do Stuff
    ldmbridge.LDMProductFactory(MyProductIngestor())

    reactor.run()
