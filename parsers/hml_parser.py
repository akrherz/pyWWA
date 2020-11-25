""" HML parser! """

from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.util import LOG
from pyiem.nws.products.hml import parser as hmlparser

import common  # @UnresolvedImport

DBPOOL = common.get_database("hml")


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """Connection was lost"""
        common.shutdown()

    def process_data(self, data):
        """ Process the product """
        defer = DBPOOL.runInteraction(real_parser, data)
        defer.addErrback(common.email_error, data)
        defer.addErrback(LOG.error)


def real_parser(txn, buf):
    """I'm gonna do the heavy lifting here"""
    prod = hmlparser(buf)
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)


if __name__ == "__main__":
    ldmbridge.LDMProductFactory(MyProductIngestor())

    reactor.run()  # @UndefinedVariable
